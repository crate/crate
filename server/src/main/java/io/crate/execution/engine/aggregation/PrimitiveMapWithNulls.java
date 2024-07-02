/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.aggregation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.crate.common.collections.Lists;

/**
 * Map that extends primitive maps like {@link io.netty.util.collection.IntObjectMap} with support for null keys (but no null values)
 */
public class PrimitiveMapWithNulls<K, V> implements Map<K, V> {

    private final Map<K, V> delegate;
    private V nullKeyValue = null;

    public PrimitiveMapWithNulls(Map<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size() + (nullKeyValue == null ? 0 : 1);
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty() && nullKeyValue == null;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            return nullKeyValue != null;
        }
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value) || value.equals(nullKeyValue);
    }

    @Override
    public V get(Object key) {
        if (key == null) {
            return nullKeyValue;
        }
        return delegate.get(key);
    }

    @Override
    public V put(K key, V value) {
        if (key == null) {
            V v = this.nullKeyValue;
            this.nullKeyValue = value;
            return v;
        } else {
            return delegate.put(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        if (key == null) {
            V v = this.nullKeyValue;
            this.nullKeyValue = null;
            return v;
        } else {
            return delegate.remove(key);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("putAll is not supported on PrimitiveMapWithNulls");
    }

    @Override
    public void clear() {
        nullKeyValue = null;
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        if (nullKeyValue == null) {
            return delegate.keySet();
        } else {
            HashSet<K> ks = new HashSet<>(delegate.keySet());
            ks.add(null);
            return ks;
        }
    }

    @Override
    public Collection<V> values() {
        if (nullKeyValue == null) {
            return delegate.values();
        } else {
            return Lists.concat(delegate.values(), nullKeyValue);
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (nullKeyValue == null) {
            return delegate.entrySet();
        } else {
            Set<Entry<K, V>> entries = delegate.entrySet();
            return new Set<>() {
                @Override
                public int size() {
                    return entries.size() + 1;
                }

                @Override
                public boolean isEmpty() {
                    return false;
                }

                @Override
                public boolean contains(Object o) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public Iterator<Entry<K, V>> iterator() {
                    Iterator<Entry<K, V>> it = entries.iterator();
                    return new Iterator<>() {

                        boolean visitedNullKey = false;

                        @Override
                        public boolean hasNext() {
                            if (visitedNullKey == false) {
                                return true;
                            }
                            return it.hasNext();
                        }

                        @Override
                        public Entry<K, V> next() {
                            if (visitedNullKey == false) {
                                visitedNullKey = true;
                                return new Entry<>() {
                                    @Override
                                    public K getKey() {
                                        return null;
                                    }

                                    @Override
                                    public V getValue() {
                                        return nullKeyValue;
                                    }

                                    @Override
                                    public V setValue(V value) {
                                        V v = PrimitiveMapWithNulls.this.nullKeyValue;
                                        PrimitiveMapWithNulls.this.nullKeyValue = value;
                                        return v;
                                    }
                                };
                            }
                            return it.next();
                        }
                    };
                }

                @Override
                public Object[] toArray() {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public <T> T[] toArray(T[] a) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean add(Entry<K, V> kvEntry) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean remove(Object o) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean containsAll(Collection<?> c) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean addAll(Collection<? extends Entry<K, V>> c) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean retainAll(Collection<?> c) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public boolean removeAll(Collection<?> c) {
                    throw new UnsupportedOperationException("");
                }

                @Override
                public void clear() {
                    throw new UnsupportedOperationException("");
                }
            };
        }
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        if (key == null) {
            return nullKeyValue == null ? defaultValue : nullKeyValue;
        }
        return delegate.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        action.accept(null, nullKeyValue);
        delegate.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        nullKeyValue = function.apply(null, nullKeyValue);
        delegate.replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException("putIfAbsent not implemented");
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException("remove not implemented");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException("replace not implemented");
    }

    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException("replace not implemented");
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        throw new UnsupportedOperationException("computeIfAbsent not implemented");
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("computeIfPresent not implemented");
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("compute not implemented");
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throw new UnsupportedOperationException("merge not implemented");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimitiveMapWithNulls<?, ?> that = (PrimitiveMapWithNulls<?, ?>) o;

        if (!delegate.equals(that.delegate)) {
            return false;
        }
        return Objects.equals(nullKeyValue, that.nullKeyValue);
    }

    @Override
    public int hashCode() {
        int result = delegate.hashCode();
        result = 31 * result + (nullKeyValue != null ? nullKeyValue.hashCode() : 0);
        return result;
    }
}
