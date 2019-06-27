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

package io.crate.execution.dml.upsert;

import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.data.ArrayRow;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public final class InsertSourceFromCells implements InsertSourceGen {

    private final List<Reference> targets;
    private final ArrayRow row = new ArrayRow();
    private final CheckConstraints<Row, CollectExpression<Row, ?>> checks;
    private final GeneratedColumns<Row> generatedColumns;

    InsertSourceFromCells(TransactionContext txnCtx,
                          Functions functions,
                          DocTableInfo table,
                          String indexName,
                          GeneratedColumns.Validation validation,
                          List<Reference> targets) {
        this.targets = targets;
        ReferencesFromInputRow referenceResolver = new ReferencesFromInputRow(targets, table.partitionedByColumns(), indexName);
        InputFactory inputFactory = new InputFactory(functions);
        if (table.generatedColumns().isEmpty() && table.defaultExpressionColumns().isEmpty()) {
            generatedColumns = GeneratedColumns.empty();
        } else {
            generatedColumns = new GeneratedColumns<>(
                inputFactory,
                txnCtx,
                validation,
                referenceResolver,
                targets,
                table.generatedColumns(),
                table.defaultExpressionColumns()
            );
        }
        checks = new CheckConstraints<>(txnCtx, inputFactory, referenceResolver, table);
    }

    public void checkConstraints(Object[] values) {
        row.cells(values);
        checks.validate(row);
    }

    public BytesReference generateSource(Object[] values) throws IOException {
        HashMap<String, Object> source = new HashMap<>();

        row.cells(values);
        generatedColumns.setNextRow(row);
        for (int i = 0; i < targets.size(); i++) {
            Reference target = targets.get(i);
            Object value = values[i];

            // partitioned columns must not be included in the source
            if (target.granularity() == RowGranularity.DOC) {
                ColumnIdent column = target.column();
                Maps.mergeInto(source, column.name(), column.path(), value);
            }
        }
        generatedColumns.validateValues(source);
        for (var entry : generatedColumns.generatedToInject()) {
            ColumnIdent column = entry.getKey().column();
            Maps.mergeInto(source, column.name(), column.path(), entry.getValue().value());
        }
        for (var entry : generatedColumns.defaultsToInject()) {
            ColumnIdent column = entry.getKey().column();
            if (column.isTopLevel() || ValueExtractors.fromMap(source, column) == null) {
                Maps.mergeInto(source, column.name(), column.path(), entry.getValue().value());
            }
        }

        return BytesReference.bytes(XContentFactory.jsonBuilder().map(source));
    }

    private static class ReferencesFromInputRow implements ReferenceResolver<CollectExpression<Row, ?>> {
        private final List<Reference> targets;
        private final List<Reference> partitionedBy;
        private final List<ColumnIdent> columns;
        @Nullable
        private final PartitionName partitionName;

        ReferencesFromInputRow(List<Reference> targets, List<Reference> partitionedBy, String indexName) {
            this.columns = Lists2.map(targets, Reference::column);
            this.targets = targets;
            this.partitionedBy = partitionedBy;
            this.partitionName = partitionedBy.isEmpty() ? null : PartitionName.fromIndexOrTemplate(indexName);
        }

        @Override
        public CollectExpression<Row, ?> getImplementation(Reference ref) {
            int idx = targets.indexOf(ref);
            if (idx >= 0) {
                return new InputCollectExpression(idx);
            } else {
                int rootIdx = columns.indexOf(ref.column().getRoot());
                if (rootIdx < 0) {
                    int partitionPos = partitionedBy.indexOf(ref);
                    if (partitionPos < 0) {
                        return NestableCollectExpression.constant(null);
                    } else {
                        assert partitionName != null
                            : "If there was a match in `partitionedBy`, then partitionName must not be null";
                        return NestableCollectExpression.constant(partitionName.values().get(partitionPos));
                    }
                } else {
                    return NestableCollectExpression.forFunction(
                        ValueExtractors.fromRow(rootIdx, ref.column().path())
                    );
                }
            }
        }
    }
}
