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

package io.crate.execution.dml.upsert;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.data.BiArrayRow;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.common.collections.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InsertSourceFromCells implements InsertSourceGen {

    private final List<Reference> targets;
    private final BiArrayRow row = new BiArrayRow();
    private final CheckConstraints<Map<String, Object>, CollectExpression<Map<String, Object>, ?>> checks;
    private final GeneratedColumns<Row> generatedColumns;
    private final Object[] defaultValues;
    private final List<Reference> partitionedByColumns;

    public InsertSourceFromCells(TransactionContext txnCtx,
                                 NodeContext nodeCtx,
                                 DocTableInfo table,
                                 String indexName,
                                 GeneratedColumns.Validation validation,
                                 List<Reference> targets) {
        Tuple<List<Reference>, Object[]> allTargetColumnsAndDefaults = addDefaults(targets, table, txnCtx, nodeCtx);
        this.targets = allTargetColumnsAndDefaults.v1();
        this.defaultValues = allTargetColumnsAndDefaults.v2();
        this.partitionedByColumns = table.partitionedByColumns();

        ReferencesFromInputRow referenceResolver = new ReferencesFromInputRow(
            this.targets,
            table.partitionedByColumns(),
            indexName
        );
        InputFactory inputFactory = new InputFactory(nodeCtx);
        if (table.generatedColumns().isEmpty()) {
            generatedColumns = GeneratedColumns.empty();
        } else {
            generatedColumns = new GeneratedColumns<>(
                inputFactory,
                txnCtx,
                validation,
                referenceResolver,
                this.targets,
                table.generatedColumns()
            );
        }
        checks = new CheckConstraints<>(
            txnCtx,
            inputFactory,
            new FromSourceRefResolver(table.partitionedByColumns(), indexName),
            table
        );
    }

    @Override
    public Map<String, Object> generateSourceAndCheckConstraints(Object[] values) {
        row.firstCells(values);
        row.secondCells(defaultValues);

        HashMap<String, Object> source = new HashMap<>();
        for (int i = 0; i < targets.size(); i++) {
            Reference target = targets.get(i);
            Object valueForInsert = target
                .valueType()
                .valueForInsert(row.get(i));
            var column = target.column();
            if (valueForInsert != null) {
                Maps.mergeInto(source, column.name(), column.path(), valueForInsert, Map::putIfAbsent);
            }
        }
        for (int i = 0; i < partitionedByColumns.size(); i++) {
            var pCol = partitionedByColumns.get(i);
            var column = pCol.column();
            ArrayList<String> fullPath = new ArrayList<>(1 + column.path().size());
            fullPath.add(column.name());
            fullPath.addAll(column.path());
            Maps.removeByPath(source, fullPath);
        }

        generatedColumns.setNextRow(row);
        generatedColumns.validateValues(source);
        for (var entry : generatedColumns.generatedToInject()) {
            var reference = entry.getKey();
            var value = entry.getValue().value();
            var valueForInsert = reference
                .valueType()
                .valueForInsert(value);
            var column = reference.column();
            Maps.mergeInto(source, column.name(), column.path(), valueForInsert);
        }
        checks.validate(source);
        return source;
    }

    private static Tuple<List<Reference>, Object[]> addDefaults(List<Reference> targets,
                                                                DocTableInfo table,
                                                                TransactionContext txnCtx,
                                                                NodeContext nodeCtx) {
        ArrayList<Reference> defaultColumns = new ArrayList<>(table.defaultExpressionColumns().size());
        ArrayList<Object> defaultValues = new ArrayList<>();
        for (Reference ref : table.defaultExpressionColumns()) {
            if (targets.contains(ref) == false) {
                defaultColumns.add(ref);
                Object val = SymbolEvaluator.evaluateWithoutParams(txnCtx, nodeCtx, ref.defaultExpression());
                defaultValues.add(val);
            }
        }
        List<Reference> allColumns;
        if (defaultColumns.isEmpty()) {
            allColumns = targets;
        } else {
            allColumns = Lists2.concat(targets, defaultColumns);
        }
        return new Tuple<>(allColumns, defaultValues.toArray(new Object[0]));
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
