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


import java.util.function.Function;

public class Buckets {

    public static Function<Object[], Row> arrayToSharedRow() {
        return new Function<Object[], Row>() {
            final ArrayRow row = new ArrayRow();

            @Override
            public Row apply(Object[] input) {
                row.cells(input);
                return row;
            }
        };
    }

    public static Function<Object[], Row> arrayToSharedRow(final int numColumns) {
        return new Function<Object[], Row>() {
            final RowN row = new RowN(numColumns);

            @Override
            public Row apply(Object[] input) {
                row.cells(input);
                return row;
            }
        };
    }

    public static Object[][] materialize(Bucket bucket) {
        Object[][] res = new Object[bucket.size()][];
        int i = 0;
        for (Row row : bucket) {
            res[i++] = row.materialize();
        }
        return res;
    }

    public static Object[] materialize(Row row) {
        Object[] res = new Object[row.numColumns()];
        for (int i = 0; i < row.numColumns(); i++) {
            res[i] = row.get(i);
        }
        return res;
    }
}
