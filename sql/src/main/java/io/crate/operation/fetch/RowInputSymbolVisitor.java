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
import io.crate.operation.BaseImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;

import java.util.*;

public class RowInputSymbolVisitor extends BaseImplementationSymbolVisitor<RowInputSymbolVisitor.Context> {

    public static class Context {

        private boolean needInputRow = false;
        private final Row inputRow;
        private final Row partitionByRow;

        private final Row fetchRow;

        private Map<Reference, RowInput> references = new LinkedHashMap<>();
        private List<ReferenceInfo> partitionedBy;

        public Context(Row inputRow, Row fetchRow, Row partitionByRow) {
            this.inputRow = inputRow;
            this.fetchRow = fetchRow;
            this.partitionByRow = partitionByRow;
        }

        public boolean needInputRow() {
            return needInputRow;
        }

        public void partitionedBy(List<ReferenceInfo> partitionedBy) {
            this.partitionedBy = partitionedBy;
        }

        public Collection<Reference> references() {
            return references.keySet();
        }

        public Input<?> allocateInput(InputColumn col) {
            return new RowInput(inputRow, col.index());
        }

        public Input<?> allocateInput(Reference reference) {
            if (reference.info().granularity() == RowGranularity.PARTITION) {
                return allocatePartitionedInput(reference.info());
            }
            RowInput input = references.get(reference);
            if (input != null) {
                return input;
            }
            input = new RowInput(fetchRow, references.size());
            references.put(reference, input);
            return input;
        }

        public Input<?> allocatePartitionedInput(ReferenceInfo referenceInfo) {
            assert partitionedBy != null : "partitionedBy must be set first";
            int idx = partitionedBy.indexOf(referenceInfo);
            if (idx > -1) {
                return new RowInput(partitionByRow, idx);
            }
            throw new AssertionError(String.format(Locale.ENGLISH,
                    "Partition reference info %s not known", referenceInfo));
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
    public Input<?> visitReference(Reference symbol, Context context) {
        return context.allocateInput(symbol);
    }

    @Override
    public Input<?> visitInputColumn(InputColumn inputColumn, Context context) {
        context.needInputRow = true;
        return context.allocateInput(inputColumn);
    }
}
