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

package io.crate.types;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import io.crate.Streamer;

import java.util.List;

/**
 * Composite type which represents a row which consists of one or more types.
 *
 * <pre>
 *     (x, y) -> RowType(integer, long)
 * </pre>
 */
public class RowType extends DataType<Object> {

    private final static Joiner commaJoiner = Joiner.on(", ");

    public final static int ID = 15;
    private final List<DataType> types;
    private final String name;

    public RowType(List<DataType> types) {
        this.types = types;
        this.name = "row(" + commaJoiner.join(Iterables.transform(types, DataType.TO_NAME)) + ")";
    }

    public List<DataType> types() {
        return types;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Streamer<?> streamer() {
        throw new UnsupportedOperationException("Cannot stream values of type Row");
    }

    @Override
    public Object value(Object value) throws IllegalArgumentException, ClassCastException {
        return value;
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        return 0;
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        if (types.size() > 1) {
            return this.equals(other);
        }
        /*
         * type must be an exact match because our cast functions cannot handle casting row types to something else
         * E.g.
         *      str_c = to_string(select int_c)     -> doesn't work
         *      int_c = (select int_c)              -> does work because cast function can be omitted
         *
         * Users can still add explicit casts:
         *
         *      str_c = (select cast(int_c as string))
         */
        return types.get(0).equals(other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RowType rowType = (RowType) o;

        return types.equals(rowType.types);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + types.hashCode();
        return result;
    }
}
