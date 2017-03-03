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
import java.util.function.Function;
import java.util.function.Supplier;

public class ListBackedColumns implements Columns {

    private final List<? extends Input<?>> inputs;

    public static Columns forArray(Object[] cells) {
        List<Input<?>> inputs = new ArrayList<>(cells.length);
        for (int i = 0; i < cells.length; i++) {
            inputs.add(new InputAtIndex<Object>(i) {
                @Override
                public Object value() {
                    return cells[idx];
                }
            });
        }
        return new ListBackedColumns(inputs);
    }

    /**
     * Returns a function which returns a columns implementation wrapping the given supplier of an object array.
     *
     * @param numCols the number of columns
     * @return a function object creating a Columns object
     */
    public static Function<Supplier<Object[]>, Columns> forArraySupplier(final int numCols) {
        return (data) -> {
            List<Input<?>> inputs = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                inputs.add(new InputAtIndex<Object>(i) {
                    @Override
                    public Object value() {
                        return data.get()[idx];
                    }
                });
            }
            return new ListBackedColumns(inputs);
        };
    }

    ListBackedColumns(List<? extends Input<?>> inputs) {
        this.inputs = inputs;
    }

    @Override
    public Input<?> get(int index) {
        return inputs.get(index);
    }

    @Override
    public int size() {
        return inputs.size();
    }
}
