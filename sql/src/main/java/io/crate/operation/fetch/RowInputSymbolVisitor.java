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
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.AbstractImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class RowInputSymbolVisitor extends AbstractImplementationSymbolVisitor<RowInputSymbolVisitor.Context> {

    public static class Context extends AbstractImplementationSymbolVisitor.Context {

        private int inputIndex = 0;
        private Row row;
        private List<Reference> references = new ArrayList<>();

        private int inputIndexPartitionedBy = 0;
        private Row partitionByRow;
        private List<ReferenceInfo> partitionedBy;

        public void row(Row row) {
            this.row = row;
        }

        public void partitionedBy(List<ReferenceInfo> partitionedBy) {
            this.partitionedBy = partitionedBy;
        }

        public void partitionByRow(Row row) {
            this.partitionByRow = row;
        }

        public List<Reference> references() {
            return references;
        }

        public Input<?> allocateInput(Reference reference) {
            if (reference.info().granularity() == RowGranularity.PARTITION) {
                return allocatePartitionedInput(reference.info());
            }
            int idx = references.indexOf(reference);
            if (idx > -1) {
                return new RowInput(row, idx);
            } else {
                references.add(reference);
                return new RowInput(row, inputIndex++);
            }
        }

        public Input<?> allocatePartitionedInput(ReferenceInfo referenceInfo) {
            assert partitionedBy != null : "partitionedBy must be set first";
            int idx = partitionedBy.indexOf(referenceInfo);
            if (idx > -1) {
                return new RowInput(partitionByRow, idx);
            }
            throw new AssertionError(String.format(Locale.ENGLISH,
                    "Partition reference info {} not known", referenceInfo));
        }

    }

    static class RowInput implements Input<Object> {

        private final Row row;
        private final int index;

        public RowInput(Row row, int index) {
            this.row = row;
            this.index = index;
        }

        @Override
        public Object value() {
            return row.get(index);
        }
    }

    public RowInputSymbolVisitor(Functions functions) {
        super(functions);
    }

    @Override
    protected Context newContext() {
        return new Context();
    }

    @Override
    public Input<?> visitReference(Reference symbol, Context context) {
        return context.allocateInput(symbol);
    }
}
