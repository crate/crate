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

package io.crate.operation.fetch;

import io.crate.core.collections.Row;

import java.util.List;

public class OrderedRowDelegate implements Row {
    private final List<?> orderBy;
    private Object[] cells;

    public OrderedRowDelegate(Row delegate, List<?> orderBy) {
        this.cells = delegate.materialize();
        this.orderBy = orderBy;
    }

    @Override
    public int size() {
        return cells.length;
    }

    @Override
    public Object get(int index) {
        if (index >= cells.length && index < cells.length + orderBy.size()) {
            return orderBy.get(index - cells.length);
        } else if (index < cells.length) {
            return cells[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    @Override
    public Object[] materialize() {
        return cells;
    }
}
