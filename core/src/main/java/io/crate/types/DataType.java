/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

public abstract class DataType<T> implements Comparable, Streamable {

    public abstract int id();

    public abstract String getName();

    public abstract Streamer<?> streamer();

    public abstract T value(Object value) throws IllegalArgumentException, ClassCastException;

    public abstract int compareValueTo(T val1, T val2);

    /**
     * check whether a value of this type is convertible to <code>other</code>
     *
     * @param other the DataType to check conversion to
     * @return true or false
     */
    public boolean isConvertableTo(DataType other) {
        if (this.equals(other)) {
            return true;
        }
        Set<DataType> possibleConversions = DataTypes.ALLOWED_CONVERSIONS.get(id());
        //noinspection SimplifiableIfStatement
        if (possibleConversions == null) {
            return false;
        }
        return possibleConversions.contains(other);
    }

    public boolean isNumeric() {
        return DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(this);
    }

    static <T> int nullSafeCompareValueTo(T val1, T val2, Comparator<T> cmp) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        }
        if (val2 == null) {
            return 1;
        }
        return Objects.compare(val1, val2, cmp);
    }

    public int hashCode() {
        return id();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataType)) return false;

        DataType that = (DataType) o;
        return (id() == that.id());
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof DataType)) return -1;
        return Integer.compare(id(), ((DataType) o).id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String toString() {
        return getName();
    }
}
