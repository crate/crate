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

public class CompositeColumns implements Columns {

    private final int numColumns;
    private final BatchIterator[] iterators;

    private ProxyInput[] inputs;

    public CompositeColumns(BatchIterator[] iterators) {
        this.iterators = iterators;
        numColumns = iterators[0].rowData().size();
        inputs = new ProxyInput[numColumns];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new ProxyInput();
        }
        updateInputs(0);
    }

    void updateInputs(int idx) {
        if (idx >= iterators.length) {
            return;
        }
        Columns columns = iterators[idx].rowData();
        for (int i = 0; i < numColumns; i++) {
            inputs[i].input = columns.get(i);
        }
    }

    @Override
    public Input<?> get(int index) {
        return inputs[index];
    }

    @Override
    public int size() {
        return numColumns;
    }
}
