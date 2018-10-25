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

import io.crate.core.collections.Maps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to apply update expressions to create a updated source
 *
 * <pre>
 * {@code
 * For updates:
 *
 *  UPDATE t SET x = x + 10
 *
 *      getResult:  {x: 20, y: 30}
 *      updateAssignments: x + 10
 *          (x = Reference)
 *          (10 = Literal)
 *      insertValues: null
 *
 *      resultSource: {x: 30, y: 30}
 *
 *
 * For ON CONFLICT DO UPDATE:
 *
 *  INSERT INTO t VALUES (10) ON CONFLICT .. DO UPDATE SET x = x + excluded.x
 *      getResult: {x: 20, y: 30}
 *      updateAssignments: x + excluded.x
 *          (x = Reference)
 *          (excluded.x = Reference)
 *      insertValues: [10]
 *      
 *      resultSource: {x: 30, y: 30}
 * </pre>
 */
final class UpdateSourceGen {

    private final Evaluator eval;
    private final GeneratedColumns<Doc> generatedColumns;
    private final ArrayList<Reference> updateColumns;
    private final CheckConstraints<Doc, CollectExpression<Doc, ?>> checks;

    UpdateSourceGen(Functions functions, DocTableInfo table, String[] updateColumns) {
        DocRefResolver refResolver = new DocRefResolver(table.partitionedBy());
        this.eval = new Evaluator(functions, refResolver);
        InputFactory inputFactory = new InputFactory(functions);
        this.checks = new CheckConstraints<>(inputFactory, refResolver, table);
        this.updateColumns = new ArrayList<>(updateColumns.length);
        for (String updateColumn : updateColumns) {
            ColumnIdent column = ColumnIdent.fromPath(updateColumn);
            Reference ref = table.getReference(column);
            this.updateColumns.add(ref == null ? table.getDynamic(column, true) : ref);
        }
        if (table.generatedColumns().isEmpty()) {
            generatedColumns = GeneratedColumns.empty();
        } else {
            generatedColumns = new GeneratedColumns<>(
                inputFactory,
                GeneratedColumns.Validation.VALUE_MATCH,
                refResolver,
                this.updateColumns,
                table.generatedColumns()
            );
        }
    }

    BytesReference generateSource(Doc result, Symbol[] updateAssignments, Object[] insertValues) throws IOException {
        /* We require a new HashMap because all evaluations of the updateAssignments need to be based on the
         * values *before* the update. For example:
         *
         * source: x=5
         * SET x = 10, y = x + 5
         *
         * Must result in y = 10, not 15
         */
        Values values = new Values(result, insertValues);
        HashMap<String, Object> updatedSource = new HashMap<>(result.getSource());
        Doc updatedDoc = result.withUpdatedSource(updatedSource);
        for (int i = 0; i < updateColumns.size(); i++) {
            Reference ref = updateColumns.get(i);
            Object value = eval.process(updateAssignments[i], values).value();
            ColumnIdent column = ref.column();
            Maps.mergeInto(updatedSource, column.name(), column.path(), value);
            generatedColumns.setNextRow(updatedDoc);
            generatedColumns.validateValue(ref, value);
        }
        injectGeneratedColumns(updatedSource);
        checks.validate(updatedDoc);
        return BytesReference.bytes(XContentFactory.jsonBuilder().map(updatedSource));
    }

    private void injectGeneratedColumns(HashMap<String, Object> updatedSource) {
        for (Map.Entry<Reference, Input<?>> entry : generatedColumns.toInject()) {
            ColumnIdent column = entry.getKey().column();
            Object value = entry.getValue().value();
            Maps.mergeInto(updatedSource, column.name(), column.path(), value);
        }
    }

    private static class Values {

        private final Doc getResult;
        private final Object[] insertValues;

        Values(Doc getResult, Object[] insertValues) {
            this.getResult = getResult;
            this.insertValues = insertValues;
        }
    }

    private static class Evaluator extends BaseImplementationSymbolVisitor<Values> {

        private final ReferenceResolver<CollectExpression<Doc, ?>> refResolver;

        private Evaluator(Functions functions,
                          ReferenceResolver<CollectExpression<Doc, ?>> refResolver) {
            super(functions);
            this.refResolver = refResolver;
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, Values context) {
            return Literal.of(inputColumn.valueType(), context.insertValues[inputColumn.index()]);
        }

        @Override
        public Input<?> visitReference(Reference symbol, Values values) {
            CollectExpression<Doc, ?> expr = refResolver.getImplementation(symbol);
            expr.setNextRow(values.getResult);
            return expr;
        }
    }
}
