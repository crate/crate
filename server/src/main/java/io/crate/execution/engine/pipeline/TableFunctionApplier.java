/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.pipeline;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;

public final class TableFunctionApplier implements Function<Row, Iterator<Row>> {

    private final RowN outgoingRow;
    private final Object[] outgoingCells;
    private final List<Input<Iterable<Row>>> tableFunctions;
    private final List<Input<?>> standalone;
    private final List<CollectExpression<Row, ?>> expressions;

    public TableFunctionApplier(List<Input<Iterable<Row>>> tableFunctions,
                                List<Input<?>> standalone,
                                List<CollectExpression<Row, ?>> expressions) {
        this.tableFunctions = tableFunctions;
        this.standalone = standalone;
        this.expressions = expressions;
        this.outgoingCells = new Object[tableFunctions.size() + standalone.size()];
        this.outgoingRow = new RowN(outgoingCells);
    }

    @Override
    public Iterator<Row> apply(Row row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        mapIncomingValuesToOutgoingCells();
        List<Iterator<Row>> iterators = Lists.map(tableFunctions, x -> x.value().iterator());
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                for (int i = 0; i < iterators.size(); i++) {
                    if (iterators.get(i).hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator doesn't have any more elements");
                }
                for (int i = 0; i < iterators.size(); i++) {
                    Iterator<Row> it = iterators.get(i);
                    if (it.hasNext()) {
                        var row = it.next();
                        // See description of TableFunctionImplementation for an explanation why single column treatment is different.
                        if (row.numColumns() == 1) {
                            outgoingCells[i] = row.get(0);
                        } else {
                            outgoingCells[i] = row;
                        }
                    } else {
                        outgoingCells[i] = null;
                    }
                }
                return outgoingRow;
            }
        };
    }

    private void mapIncomingValuesToOutgoingCells() {
        for (int i = 0; i < standalone.size(); i++) {
            outgoingCells[tableFunctions.size() + i] = standalone.get(i).value();
        }
    }
}
