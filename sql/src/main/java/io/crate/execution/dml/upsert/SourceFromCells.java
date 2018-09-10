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

import io.crate.collections.Lists2;
import io.crate.data.ArrayRow;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SourceFromCells implements SourceGen {

    private final List<Reference> targets;
    private final Map<Reference, Input<?>> generatedColumnsToInject;
    private final Map<Reference, Input<?>> generatedColumnsToValidate;
    private final ArrayRow row = new ArrayRow();
    private final List<CollectExpression<Row, ?>> expressions;
    private final Validation validation;
    private final CheckConstraints<Row, CollectExpression<Row, ?>> checks;

    SourceFromCells(Functions functions,
                    DocTableInfo table,
                    Validation validation,
                    List<Reference> targets) {
        assert targets.stream().allMatch(ref -> ref.column().isTopLevel()) : "Can only insert into top-level columns";

        this.targets = targets;
        this.validation = validation;
        ReferencesFromInputRow referenceResolver = new ReferencesFromInputRow(targets);
        InputFactory inputFactory = new InputFactory(functions);
        if (table.generatedColumns().isEmpty()) {
            generatedColumnsToInject = Collections.emptyMap();
            generatedColumnsToValidate = Collections.emptyMap();
            expressions = Collections.emptyList();
        } else {
            generatedColumnsToInject = new HashMap<>();
            generatedColumnsToValidate = new HashMap<>();
            InputFactory.Context<CollectExpression<Row, ?>> context =
                inputFactory.ctxForRefs(referenceResolver);

            for (GeneratedReference generatedColumn : table.generatedColumns()) {
                Input<?> input = context.add(generatedColumn.generatedExpression());
                if (targets.contains(generatedColumn)) {
                    generatedColumnsToValidate.put(generatedColumn, input);
                } else {
                    generatedColumnsToInject.put(generatedColumn, input);
                }
            }
            expressions = context.expressions();
        }
        checks = new CheckConstraints<>(inputFactory, referenceResolver, table);
    }

    public void checkConstraints(Object[] values) {
        row.cells(values);
        checks.validate(row);
    }

    public BytesReference generateSource(Object[] values) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject();

        row.cells(values);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        for (int i = 0; i < targets.size(); i++) {
            Reference target = targets.get(i);
            Object value = values[i];

            // partitioned columns must not be included in the source
            if (target.granularity() == RowGranularity.DOC) {
                builder.field(target.column().fqn(), value);
                if (validation == Validation.GENERATED_VALUE_MATCH) {
                    assertValueMatchesWithGeneratedColumnCalculation(target, value);
                }
            }
        }
        for (Map.Entry<Reference, Input<?>> entry : generatedColumnsToInject.entrySet()) {
            builder.field(entry.getKey().column().fqn(), entry.getValue().value());
        }

        return builder
            .endObject()
            .bytes();
    }

    private void assertValueMatchesWithGeneratedColumnCalculation(Reference target, Object value) {
        Input<?> input = generatedColumnsToValidate.get(target);
        if (input != null) {
            Object generatedValue = input.value();
            //noinspection unchecked
            if (target.valueType().compareValueTo(generatedValue, value) != 0) {
                throw new IllegalArgumentException(
                    "Given value " + value +
                    " for generated column " + target.column() +
                    " does not match calculation " + ((GeneratedReference) target).formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    private static class ReferencesFromInputRow implements ReferenceResolver<CollectExpression<Row, ?>> {
        private final List<Reference> targets;
        private final List<ColumnIdent> columns;

        ReferencesFromInputRow(List<Reference> targets) {
            this.columns = Lists2.copyAndReplace(targets, Reference::column);
            this.targets = targets;
        }

        @Override
        public CollectExpression<Row, ?> getImplementation(Reference ref) {
            int idx = targets.indexOf(ref);
            if (idx >= 0) {
                return new InputCollectExpression(idx);
            } else {
                int rootIdx = columns.indexOf(ref.column().getRoot());
                if (rootIdx < 0) {
                    return NestableCollectExpression.constant(null);
                } else {
                    return NestableCollectExpression.forFunction(
                        ValueExtractors.fromRow(rootIdx, ref.column().path())
                    );
                }
            }
        }
    }
}
