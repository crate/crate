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

public class Row1 implements Row {

    public static final long ERROR = -2L;
    public static final Row1 ROW_COUNT_UNKNOWN = new Row1(-1L);


    private final Object value;

    public Row1(Object value) {
        this.value = value;
    }

    @Override
    public int numColumns() {
        return 1;
    }

    @Override
    public Object get(int index) {
        if (index != 0)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: 1");
        return value;
    }

    @Override
    public Object[] materialize() {
        return new Object[]{value};
    }

    @Override
    public String toString() {
        return "Row1{" + value + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Row1 row1 = (Row1) o;
        return value != null ? value.equals(row1.value) : row1.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
