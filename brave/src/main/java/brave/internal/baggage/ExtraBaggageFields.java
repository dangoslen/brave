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

import brave.baggage.BaggageField;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static brave.internal.baggage.ExtraBaggageFieldsFactory.MAX_DYNAMIC_FIELDS;
import static brave.internal.baggage.LongBitSet.isSet;
import static brave.internal.baggage.LongBitSet.setBit;
import static brave.internal.baggage.LongBitSet.unsetBit;

/**
 * Holds one or more baggage fields in {@link TraceContext#extra()} or {@link
 * TraceContextOrSamplingFlags#extra()}.
 */
public final class ExtraBaggageFields extends Extra<ExtraBaggageFields, ExtraBaggageFieldsFactory> {
  ExtraBaggageFields(ExtraBaggageFieldsFactory factory) {
    super(factory);
  }

  Object[] array() {
    return (Object[]) state();
  }

  /** When true, calls to {@link #getAllFields()} cannot be cached. */
  public boolean isDynamic() {
    return factory.isDynamic;
  }

  /** The list of fields present, regardless of value. */
  public List<BaggageField> getAllFields() {
    if (!factory.isDynamic) return factory.initialFieldList;
    Object[] array = array();
    List<BaggageField> result = new ArrayList<>(array.length / 2);
    for (int i = 0; i < array.length; i += 2) {
      result.add((BaggageField) array[i]);
    }
    return Collections.unmodifiableList(result);
  }

  /** Returns a read-only view of the non-null baggage field values */
  public Map<BaggageField, String> toMapFilteringFields(BaggageField... filtered) {
    return UnsafeArrayMap.<BaggageField, String>create(array()).filterKeys(filtered);
  }

  /**
   * Returns the value of the field with the specified name or {@code null} if not available.
   *
   * @see BaggageField#getValue(TraceContext)
   * @see BaggageField#getValue(TraceContextOrSamplingFlags)
   */
  @Nullable public String getValue(BaggageField field) {
    if (field == null) return null;
    Object[] state = array();
    int i = indexOfField(state, field);
    return i != -1 ? (String) state[i + 1] : null;
  }

  int indexOfField(Object[] state, BaggageField field) {
    Integer index = factory.initialFieldIndices.get(field);
    if (index != null) return index;
    for (int i = factory.initialArrayLength; i < state.length; i += 2) {
      if (state[i] == null) break; // end of keys
      if (field.equals(state[i])) return i;
    }
    return -1;
  }

  /**
   * Updates a state object to include a value change.
   *
   * @param field the field that was updated
   * @param value {@code null} means remove the mapping to this field.
   * @return true implies a a change in the underlying state
   * @see BaggageField#updateValue(TraceContext, String)
   * @see BaggageField#updateValue(TraceContextOrSamplingFlags, String)
   */
  public boolean updateValue(BaggageField field, @Nullable String value) {
    if (field == null) return false;
    int updateAttempts = 3; // prevent spinning or bugs from killing things
    while (updateAttempts > 0) {
      Object[] state = array();
      int i = indexOfField(state, field);
      if (i != -1) {
        if (equal(value, state[i + 1])) return false;
        // We have the same field, just a different value
        if (tryUpdateValue(state, i, value)) return true;
        updateAttempts--;
        continue;
      }

      // When we reach here, there's a new field, but we may not have a policy
      // grow, or we may have reached the maximum allowed field count.
      if (!isDynamic()) return false; // this policy does not allow new fields.
      if (tryAddNewField(state, field, value)) return true;

      updateAttempts--;
    }

    Platform.get().log("Failed to update %s", field, null);
    return false;
  }

  @Override protected Object[] mergeStateKeepingOursOnConflict(ExtraBaggageFields theirFields) {
    Object[] ourArray = array(), theirArray = theirFields.array();

    // scan first to see if we need to change our values, grow our array, or neither
    long changeInOurs = 0, newToOurs = 0;
    for (int i = 0; i < theirArray.length; i += 2) {
      if (theirArray[i] == null) break; // end of keys
      int ourIndex = indexOfField(ourArray, (BaggageField) theirArray[i]);
      int bitsetIndex = i / 2;
      if (ourIndex == -1) {
        newToOurs = setBit(newToOurs, bitsetIndex);
      } else {
        Object ourValue = ourArray[ourIndex + 1];
        if (ourValue != null) continue; // our ourArray wins
        if (!equal(ourArray[ourIndex + 1], theirArray[i + 1])) {
          changeInOurs = setBit(changeInOurs, bitsetIndex);
        }
      }
    }

    if (changeInOurs == 0 && newToOurs == 0) return ourArray;

    // To implement copy-on-write, we provision a new array large enough for all changes.
    int newArrayLength = ourArray.length + LongBitSet.size(newToOurs) * 2;
    if (newArrayLength / 2 > MAX_DYNAMIC_FIELDS) {
      Platform.get().log("Ignoring request to add > %s dynamic fields", MAX_DYNAMIC_FIELDS, null);
      newToOurs = 0;
    }
    Object[] newState = Arrays.copyOf(ourArray, newArrayLength);

    // Now, we iterate through all changes and apply them
    int endOfOurs = ourArray.length;
    for (int i = 0; i < theirArray.length; i += 2) {
      if (theirArray[i] == null) break; // end of keys
      int bitsetIndex = i / 2;
      if (isSet(changeInOurs, bitsetIndex)) {
        changeInOurs = unsetBit(changeInOurs, bitsetIndex);
        int ourIndex = indexOfField(newState, (BaggageField) theirArray[i]);
        newState[ourIndex + 1] = theirArray[i + 1];
      } else if (isSet(newToOurs, bitsetIndex)) {
        newToOurs = unsetBit(newToOurs, bitsetIndex);
        newState[endOfOurs] = theirArray[i];
        newState[endOfOurs + 1] = theirArray[i + 1];
        endOfOurs += 2;
      }
      if (changeInOurs == 0 && newToOurs == 0) break;
    }
    return newState;
  }

  /**
   * It is important to note that fields are append-only. Knowing this, any lost race attempting to
   * update an existing value can easily retried by comparing against the current index.
   */
  boolean tryUpdateValue(Object[] state, int i, @Nullable String value) {
    Object[] newState = Arrays.copyOf(state, state.length); // copy-on-write
    newState[i + 1] = value;
    return factory.compareAndSetState(this, state, newState);
  }

  /** Grows the array to append a  new field/value pair. */
  boolean tryAddNewField(Object[] state, BaggageField field, @Nullable String value) {
    int newIndex = state.length;
    int newArrayLength = newIndex + 2;
    if (newArrayLength / 2 > MAX_DYNAMIC_FIELDS) {
      Platform.get().log("Ignoring request to add > %s dynamic fields", MAX_DYNAMIC_FIELDS, null);
      return false;
    }
    Object[] newState = Arrays.copyOf(state, newArrayLength); // copy-on-write
    newState[newIndex] = field;
    newState[newIndex + 1] = value;
    return factory.compareAndSetState(this, state, newState);
  }

  @Override protected boolean stateEquals(Object thatState) {
    return Arrays.equals(array(), (Object[]) thatState);
  }

  @Override protected int stateHashCode() {
    return Arrays.hashCode(array());
  }

  @Override protected String stateString() {
    return Arrays.toString(array());
  }
}
