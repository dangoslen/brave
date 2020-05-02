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

import brave.internal.Nullable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static brave.internal.baggage.LongBitSet.isSet;
import static brave.internal.baggage.LongBitSet.setBit;

/**
 * A potentially read-only map which is a view over an array of {@code key, value} pairs. No key can
 * be {@code null}. However, values can be {@code null}.
 *
 * <p>The array is shared with the caller to {@link #create}, hence being called "unsafe".
 * This type supports cheap views over data using thread-local or copy-on-write arrays.
 *
 * <p>An input with no keys coerces to {@link Collections#emptyMap()}.
 *
 * <p>As this is an immutable view, operations like {@link #keySet()}, {@link #values()} and {@link
 * #entrySet()} might return constants. As expected, stateful objects such as {@link Iterator} will
 * never be shared.
 */
class UnsafeArrayMap<K, V> implements Map<K, V> {
  static final int MAX_FILTERED_KEYS = LongBitSet.MAX_SIZE;

  static <K, V> Map<K, V> create(Object[] array) {
    if (array == null) throw new NullPointerException("array == null");
    int i = 0;
    for (; i < array.length; i += 2) {
      Object key = array[i];
      if (key == null) break; // we ignore anything starting at first null key
    }
    if (i == 0) return Collections.emptyMap();
    return new UnsafeArrayMap<>(array, i, 0);
  }

  /** Resets redaction based on the input. */
  static <K, V> Map<K, V> filterKeys(Object[] array, K... filteredKeys) {
    if (array == null) throw new NullPointerException("array == null");
    if (filteredKeys == null || filteredKeys.length == 0) return create(array);

    if (filteredKeys.length > MAX_FILTERED_KEYS) {
      throw new IllegalArgumentException("cannot redact more than " + MAX_FILTERED_KEYS + " keys");
    }

    long filteredBitSet = 0;
    int i = 0, numFiltered = 0;
    for (; i < array.length; i += 2) {
      Object key = array[i];
      if (key == null) break; // we ignore anything starting at first null key
      for (K filteredKey : filteredKeys) {
        if (filteredKey.equals(key)) {
          filteredBitSet = setBit(filteredBitSet, i / 2);
          numFiltered++;
          break;
        }
      }
    }
    if (numFiltered == i / 2) return Collections.emptyMap();
    return new UnsafeArrayMap<>(array, i, filteredBitSet);
  }

  final Object[] array;
  final int toIndex, size;
  final long filteredKeys;

  UnsafeArrayMap(Object[] array, int toIndex, long filteredKeys) {
    this.array = array;
    this.toIndex = toIndex;
    this.filteredKeys = filteredKeys;
    this.size = toIndex / 2 - LongBitSet.size(filteredKeys);
  }

  @Override public int size() {
    return size;
  }

  @Override public boolean containsKey(Object o) {
    if (o == null) return false; // null keys are not allowed
    return arrayIndexOfKey(o) != -1;
  }

  @Override public boolean containsValue(Object o) {
    for (int i = 0; i < toIndex; i += 2) {
      if (equal(o, array[i + 1]) && !isSet(filteredKeys, i / 2)) return true;
    }
    return false;
  }

  @Override public V get(Object o) {
    if (o == null) return null; // null keys are not allowed
    int i = arrayIndexOfKey(o);
    return i != -1 ? (V) array[i + 1] : null;
  }

  int arrayIndexOfKey(Object o) {
    int result = -1;
    for (int i = 0; i < toIndex; i += 2) {
      if (o.equals(array[i]) && !isSet(filteredKeys, i / 2)) {
        return i;
      }
    }
    return result;
  }

  @Override public Set<K> keySet() {
    return new KeySetView();
  }

  @Override public Collection<V> values() {
    return new ValuesView();
  }

  @Override public Set<Map.Entry<K, V>> entrySet() {
    return new EntrySetView();
  }

  @Override public boolean isEmpty() {
    return false;
  }

  @Override public V put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override public void clear() {
    throw new UnsupportedOperationException();
  }

  final class KeySetView extends SetView<K> {
    @Override K elementAtArrayIndex(int i) {
      return (K) array[i];
    }

    @Override public boolean contains(Object o) {
      return containsKey(o);
    }
  }

  final class ValuesView extends SetView<V> {
    @Override V elementAtArrayIndex(int i) {
      return (V) array[i + 1];
    }

    @Override public boolean contains(Object o) {
      return containsValue(o);
    }
  }

  final class EntrySetView extends SetView<Map.Entry<K, V>> {
    @Override Map.Entry<K, V> elementAtArrayIndex(int i) {
      return new Entry<>((K) array[i], (V) array[i + 1]);
    }

    @Override public boolean contains(Object o) {
      if (!(o instanceof Map.Entry) || ((Map.Entry) o).getKey() == null) return false;
      Map.Entry that = (Map.Entry) o;
      int i = arrayIndexOfKey(that.getKey());
      if (i == -1) return false;
      return equal(that.getValue(), array[i + 1]);
    }
  }

  abstract class SetView<E> implements Set<E> {
    int advancePastFiltered(int i) {
      while (i < toIndex && isFilteredKey(i)) i += 2;
      return i;
    }

    @Override public int size() {
      return size;
    }

    /**
     * By abstracting this, {@link #keySet()} {@link #values()} and {@link #entrySet()} only
     * implement need implement two methods based on {@link #<E>}: this method and and {@link
     * #contains(Object)}.
     */
    abstract E elementAtArrayIndex(int i);

    @Override public Iterator<E> iterator() {
      return new ReadOnlyIterator();
    }

    @Override public Object[] toArray() {
      return copyTo(new Object[size]);
    }

    @Override public <T> T[] toArray(T[] a) {
      T[] result = a.length >= size ? a
          : (T[]) Array.newInstance(a.getClass().getComponentType(), size());
      return copyTo(result);
    }

    <T> T[] copyTo(T[] dest) {
      for (int i = 0, d = 0; i < toIndex; i += 2) {
        if (isFilteredKey(i)) continue;
        dest[d++] = (T) elementAtArrayIndex(i);
      }
      return dest;
    }

    final class ReadOnlyIterator implements Iterator<E> {
      int i = advancePastFiltered(0);

      @Override public boolean hasNext() {
        i = advancePastFiltered(i);
        return i < toIndex;
      }

      @Override public E next() {
        if (!hasNext()) throw new NoSuchElementException();
        E result = elementAtArrayIndex(i);
        i += 2;
        return result;
      }

      @Override public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    @Override public boolean containsAll(Collection<?> c) {
      if (c == null) return false;
      if (c.isEmpty()) return true;

      for (Object element : c) {
        if (!contains(element)) return false;
      }
      return true;
    }

    @Override public boolean isEmpty() {
      return false;
    }

    @Override public boolean add(E e) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  static final class Entry<K, V> implements Map.Entry<K, V> {
    final K key;
    @Nullable final V value;

    Entry(K key, V value) {
      if (key == null) throw new NullPointerException("key == null");
      this.key = key;
      this.value = value;
    }

    @Override public K getKey() {
      return key;
    }

    @Override public V getValue() {
      return value;
    }

    @Override public V setValue(V value) {
      throw new UnsupportedOperationException();
    }

    @Override public int hashCode() {
      int h = 1000003;
      h ^= key.hashCode();
      h *= 1000003;
      h ^= value == null ? 0 : value.hashCode();
      return h;
    }

    @Override public boolean equals(Object o) {
      if (!(o instanceof Map.Entry)) return false;
      Map.Entry that = (Map.Entry) o;
      return key.equals(that.getKey()) && equal(value, that.getValue());
    }

    @Override public String toString() {
      return "Entry{" + key + "=" + value + "}";
    }
  }

  @Override public String toString() {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < toIndex; i += 2) {
      if (isFilteredKey(i)) continue;
      if (result.length() > 0) result.append(',');
      result.append(array[i]).append('=').append(array[i + 1]);
    }
    return result.insert(0, "UnsafeArrayMap{").append("}").toString();
  }

  int calculateSize(int toIndex) {
    int size = 0;
    for (int i = 0; i < toIndex; i += 2) {
      if (array[i] != null && !isFilteredKey(i)) size++;
    }
    return size;
  }

  boolean isFilteredKey(int i) {
    return isSet(filteredKeys, i / 2);
  }

  static boolean equal(@Nullable Object a, @Nullable Object b) {
    return a == null ? b == null : a.equals(b); // Java 6 can't use Objects.equals()
  }
}
