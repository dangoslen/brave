/*
 * Copyright 2013-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.internal.baggage;

import brave.internal.InternalPropagation;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Factory that manages a single mutable element of {@link TraceContext#extra()} per {@linkplain
 * TraceContext context} or {@link TraceContextOrSamplingFlags extraction result}: {@link #<E>}.
 *
 * <p>If your data is not mutable, and is constant through the trace, do not use this factory.
 * Instead, add your data during {@link TraceContext.Extractor#extract} as default behavior of the
 * tracer will copy it down to children.
 *
 * <p>{@link #<E>} is copy-on-write internally, but it is still mutable {@link #<E>} is mutable.
 * This factory handles state forking needed to ensure that updates to child spans are invisible to
 * their parents or siblings.
 *
 * <p>The state forking is managed by {@link #decorate(TraceContext)} and must be integrated to
 * ensure data is managed properly, ex via {@link Propagation.Factory#decorate(TraceContext)}.
 *
 * @param <E> They type of {@link Extra} managed by this factory. Must be a final class. The type is
 * typically package private to avoid accidental interference.
 * @param <F> An instance of this factory. {@link #<E>} should be associated with only one factory.
 */
public abstract class ExtraFactory<E extends Extra<E, F>, F extends ExtraFactory<E, F>> {
  final Object initialState;

  /**
   * @param initialState shared with all new instances of {@link #<E>} until there is a state
   * change.
   */
  protected ExtraFactory(Object initialState) {
    if (initialState == null) throw new NullPointerException("initialState == null");
    this.initialState = initialState;
  }

  /**
   * Creates a new instance of {@link #<E>}, with this factory's initial state.
   *
   * <p>Propagation extensions call this to setup any state from the request during {@link
   * TraceContext.Extractor#extract}. The result must be added to {@link
   * TraceContextOrSamplingFlags#extra()} or {@link TraceContext#extra()}.
   *
   * <p><em>Note</em>: It is a programming error to add more than one instance generated here to
   * {@link TraceContextOrSamplingFlags#extra()} or {@link TraceContext#extra()}.
   *
   * @return a new instance later managed by {@link #decorate(TraceContext)}
   */
  public abstract E create();

  /**
   * This an update via copy-on-write. Only invoke this after comparing values aren't already
   * effectively the same.
   *
   * @param extra the subject of update.
   * @param expectedState the prior {@linkplain Extra#state() state}.
   * @param newState the new {@linkplain Extra#state() state}.
   * @return {@code false} implies a lost race: review state and call again if necessary.
   */
  protected final boolean compareAndSetState(E extra, Object expectedState, Object newState) {
    if (extra == null) throw new NullPointerException("extra == null");
    if (expectedState == null) throw new NullPointerException("expectedState == null");
    if (newState == null) throw new NullPointerException("newState == null");
    return extra.internal.compareAndSet(expectedState, newState);
  }

  /**
   * This shouldn't be called directly, rather integrated into a hook that operates on every new
   * trace context.
   *
   * @see {@link Propagation.Factory#decorate(TraceContext)}
   */
  public final TraceContext decorate(TraceContext context) {
    long traceId = context.traceId(), spanId = context.spanId();

    E claimed = null;
    int existingIndex = -1;
    for (int i = 0, length = context.extra().size(); i < length; i++) {
      Object next = context.extra().get(i);
      if (next instanceof Extra) {
        Extra nextExtra = (Extra) next;
        // Don't interfere with other instances or subtypes
        if (nextExtra.factory != this) continue;

        if (claimed == null && nextExtra.tryToClaim(traceId, spanId)) {
          claimed = (E) nextExtra;
          continue;
        }

        if (existingIndex != -1) {
          // Throwing here as this is internal code and handling bugs is complex state management.
          // This factory should never be exposed to end users. Plugins using this factory should
          // only add one extra element per type.
          //
          // Ex.  builder.extra(factory.create());
          // Not: builder.extra(factory.create()).extra(factory.create())
          throw new RuntimeException(
              "BUG: something added the result of create() multiple times to context.extra()!");
        }
        existingIndex = i;
      }
    }

    // Easiest when there is neither existing state to assign, nor need to change context.extra()
    if (claimed != null && existingIndex == -1) {
      return context;
    }

    ArrayList<Object> mutableExtraList = new ArrayList<>(context.extra());

    // If context.extra() didn't have an unclaimed extra instance, create one for this context.
    if (claimed == null) {
      claimed = create();
      claimed.tryToClaim(traceId, spanId);
      mutableExtraList.add(claimed);
    }

    if (existingIndex != -1) {
      E existing = (E) mutableExtraList.remove(existingIndex);

      // If the claimed extra instance was new or had no changes, simply assign existing to it 
      if (claimed.state() == initialState) {
        claimed.internal.set(existing.state());
      } else if (existing.state() != initialState) {
        Object update = claimed.mergeStateKeepingOursOnConflict(existing);
        if (update == null) {
          throw new NullPointerException("BUG: mergeStateKeepingOursOnConflict returned null");
        }
        claimed.internal.set(update);
      }
    }

    return contextWithExtra(context, Collections.unmodifiableList(mutableExtraList));
  }

  // TODO: this is internal. If this ever expose it otherwise, this should use Lists.ensureImmutable
  TraceContext contextWithExtra(TraceContext context, List<Object> extra) {
    return InternalPropagation.instance.withExtra(context, extra);
  }
}
