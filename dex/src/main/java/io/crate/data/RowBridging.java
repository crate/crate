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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Helper functions to migrate between {@link Columns} based and {@link Row} based components.
 */
public class RowBridging {

    public static Columns toInputs(final Supplier<Row> rowSupplier, int numCols){
        assert rowSupplier != null: "rowSupplier must not be null";
        List<Input<?>> result = new ArrayList<>(numCols);
        for (int i = 0; i < numCols; i++) {
            int finalI = i;
            result.add(() -> rowSupplier.get().get(finalI));
        }
        return Columns.wrap(result);
    }

    public static Object[] materialize(Columns inputs){
        assert inputs != null: "inputs must not be null";
        Object[] res = new Object[inputs.size()];
        for (int i = 0; i < res.length; i++) {
            res[i] = inputs.get(i).value();
        }
        return res;
    }

    public static Row toRow(Columns inputs){
        assert inputs != null: "inputs must not be null";
        return new Row() {
            @Override
            public int numColumns() {
                return inputs.size();
            }

            @Override
            public Object get(int index) {
                return inputs.get(index).value();
            }

            @Override
            public Object[] materialize() {
                return Buckets.materialize(this);
            }
        };
    }
}
