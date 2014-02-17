/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.collector;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * collect all rows it receives
 */
public class SimpleCollector implements RowCollector<Object[][]> {

    private final Input<Object[]> rowInput;
    private final List<Object[]> rows;

    public SimpleCollector(Input<Object[]> rowInput) {
        this.rowInput = rowInput;
        rows = new ArrayList<>();
    }

    public SimpleCollector(Input<Object[]> rowInput, int capacity) {
        this.rowInput = rowInput;
        rows = new ArrayList<>(capacity);
    }

    @Override
    public boolean startCollect() {
        return true;
    }

    @Override
    public boolean processRow() {
        rows.add(rowInput.value());
        return true;
    }

    @Override
    public Object[][] finishCollect() {
        return rows.toArray(new Object[rows.size()][]);
    }
}
