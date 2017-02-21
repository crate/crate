/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.data;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This interface exposes a list of {@link Input} objects to be used as accessors to the underlying data for each column
 * of a row of data.
 */
public interface Columns extends Iterable<Input<?>> {

    /**
     * Returns an input object to be used as an accessor to the data of the column identified by its position.
     * <p>
     * Note that objects implementing this interface are required to always return the same input instance in such that
     * get(1) == get(1) is always true.
     *
     * @param index zero based position of the input
     * @return the input at the specified position
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    Input<?> get(int index);

    /**
     * Returns the number of columns.
     */
    int size();

    Columns EMPTY = new Columns() {
        @Override
        public Input<?> get(int index) {
            throw new IndexOutOfBoundsException("No column found at index: " + index);
        }

        @Override
        public int size() {
            return 0;
        }
    };

    @Override
    default Iterator<Input<?>> iterator() {
        return new Iterator<Input<?>>() {
            int i = -1;

            @Override
            public boolean hasNext() {
                return i + 1 < size();
            }

            @Override
            public Input<?> next() {
                if (size() > i) {
                    return get(++i);
                }
                throw new NoSuchElementException("Iterator exhausted");
            }
        };
    }

    /**
     * Creates a new columns object with a single column using the given input as data source.
     *
     * @param input the input providing the value
     * @param <T>      the type of object returned by the input
     * @return a new columns object
     */
    static <T> Columns singleCol(Input<T> input) {
        return new SingleColumns<>(input);
    }

    /**
     * Creates a new columns object which wraps the given list of inputs. Modifications to the passed in list must be
     * prevented, since this would break the immutability contract of this interface.
     *
     * @param inputs the list of inputs to be wrapped
     * @return a new columns object
     */
    static Columns wrap(List<? extends Input<?>> inputs) {
        return new ListBackedColumns(inputs);
    }
}
