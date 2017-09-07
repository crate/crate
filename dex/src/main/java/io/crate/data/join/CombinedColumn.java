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

package io.crate.data.join;

import io.crate.data.Columns;
import io.crate.data.Input;
import io.crate.data.ProxyInput;

class CombinedColumn implements Columns {

    private static final Input<Object> NULL_INPUT = () -> null;

    private final ProxyInput[] inputs;
    private final Columns left;
    private final Columns right;

    CombinedColumn(Columns left, Columns right) {
        this.left = left;
        this.right = right;
        inputs = new ProxyInput[left.size() + right.size()];
        for (int i = 0; i < left.size(); i++) {
            inputs[i] = new ProxyInput();
            inputs[i].input = left.get(i);
        }
        for (int i = left.size(); i < inputs.length; i++) {
            inputs[i] = new ProxyInput();
            inputs[i].input = right.get(i - left.size());
        }
    }

    @Override
    public Input<?> get(int index) {
        return inputs[index];
    }

    @Override
    public int size() {
        return inputs.length;
    }

    void nullRight() {
        for (int i = left.size(); i < inputs.length; i++) {
            inputs[i].input = NULL_INPUT;
        }
    }

    void resetRight() {
        for (int i = left.size(); i < inputs.length; i++) {
            inputs[i].input = right.get(i - left.size());
        }
    }

    void nullLeft() {
        for (int i = 0; i < left.size(); i++) {
            inputs[i].input = NULL_INPUT;
        }
    }

    void resetLeft() {
        for (int i = 0; i < left.size(); i++) {
            inputs[i].input = left.get(i);
        }
    }
}
