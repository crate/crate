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

import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public final class TableFunctionApplier implements Function<Row, Iterator<Row>> {

    private final RowN outgoingRow;
    private final Object[] outgoingCells;
    private final List<Func> tableFunctions;
    private final int[] incomingValueMapping;

    public static class Func {
        final TableFunctionImplementation implementation;
        final List<Input> args;
        final int posInOutput;

        private Iterator<Row> bucket;

        public Func(TableFunctionImplementation implementation, List<Input> args, int posInOutput) {
            this.implementation = implementation;
            this.args = args;
            this.posInOutput = posInOutput;
        }
    }

    public TableFunctionApplier(List<Func> tableFunctions, int[] incomingValueMapping) {
        this.tableFunctions = tableFunctions;
        this.incomingValueMapping = incomingValueMapping;
        this.outgoingCells = new Object[tableFunctions.size() + incomingValueMapping.length];
        this.outgoingRow = new RowN(outgoingCells);
    }

    @Override
    public Iterator<Row> apply(Row row) {
        int maxRows = 0;
        for (Func tableFunction : tableFunctions) {
            Bucket bucket = tableFunction.implementation.execute(tableFunction.args);
            maxRows = Math.max(bucket.size(), maxRows);
            tableFunction.bucket = bucket.iterator();
        }
        mapIncomingValuesToOutgoingCells(row);
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
            outgoingCells[func.posInOutput] = func.bucket.hasNext() ? func.bucket.next().get(0) : null;
        }
    }

    private void mapIncomingValuesToOutgoingCells(Row incomingRow) {
        for (int inCellPos = 0; inCellPos < incomingValueMapping.length; inCellPos++) {
            int outCellPos = incomingValueMapping[inCellPos];
            outgoingCells[outCellPos] = incomingRow.get(inCellPos);
        }
    }
}
