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

package io.crate.operation;

import io.crate.data.Buckets;
import io.crate.data.Input;
import io.crate.data.Row;

import java.util.List;

public class InputRow implements Row {

    private final List<? extends Input<?>> inputs;

    public InputRow(List<? extends Input<?>> inputs) {
        assert inputs != null: "inputs must not be null";
        this.inputs = inputs;
    }

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

    @Override
    public String toString() {
        return "InputRow{" +
               "inputs=" + inputs +
               '}';
    }
}
