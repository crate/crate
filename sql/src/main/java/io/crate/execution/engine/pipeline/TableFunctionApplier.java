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

package io.crate.execution.engine.pipeline;

import io.crate.common.collections.Lists2;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public final class TableFunctionApplier implements Function<Row, Iterator<Row>> {

    private final RowN outgoingRow;
    private final Object[] outgoingCells;
    private final List<Func> tableFunctions;
    private final List<Input<?>> standalone;
    private final List<CollectExpression<Row, ?>> expressions;

    private static class Func {
        private final Input<Bucket> input;
        Iterator<Row> iterator;

        private Func(Input<Bucket> input) {
            this.input = input;
        }
    }


    public TableFunctionApplier(List<Input<Bucket>> tableFunctions,
                                List<Input<?>> standalone,
                                List<CollectExpression<Row, ?>> expressions) {
        this.tableFunctions = Lists2.map(tableFunctions, Func::new);
        this.standalone = standalone;
        this.expressions = expressions;
        this.outgoingCells = new Object[tableFunctions.size() + standalone.size()];
        this.outgoingRow = new RowN(outgoingCells);
    }

    @Override
    public Iterator<Row> apply(Row row) {
        int maxRows = 0;
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        for (Func tableFunction : tableFunctions) {
            Bucket bucket = tableFunction.input.value();
            maxRows = Math.max(bucket.size(), maxRows);
            tableFunction.iterator = bucket.iterator();
        }
        mapIncomingValuesToOutgoingCells();
        final int numRows = maxRows;
        return new Iterator<Row>() {

            int currentRow = 0;

            @Override
            public boolean hasNext() {
                return currentRow < numRows;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator doesn't have any more elements");
                }
                fillOutgoingCellsFromBuckets();
                currentRow++;
                return outgoingRow;
            }
        };
    }

    private void fillOutgoingCellsFromBuckets() {
        for (int i = 0; i < tableFunctions.size(); i++) {
            Func func = tableFunctions.get(i);
            outgoingCells[i] = func.iterator.hasNext() ? func.iterator.next().get(0) : null;
        }
    }

    private void mapIncomingValuesToOutgoingCells() {
        for (int i = 0; i < standalone.size(); i++) {
            outgoingCells[tableFunctions.size() + i] = standalone.get(i).value();
        }
    }
}
