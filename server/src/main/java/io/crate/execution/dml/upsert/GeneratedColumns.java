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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;

public final class GeneratedColumns<T> {

    private static final GeneratedColumns EMPTY = new GeneratedColumns<>();

    @SuppressWarnings("unchecked")
    public static <T> GeneratedColumns<T> empty() {
        return (GeneratedColumns<T>) EMPTY;
    }

    private final Map<GeneratedReference, Input<?>> toValidate;
    private final Map<GeneratedReference, Input<?>> generatedToInject;
    private final List<CollectExpression<T, ?>> expressions;

    private GeneratedColumns() {
        toValidate = Collections.emptyMap();
        generatedToInject = Collections.emptyMap();
        expressions = Collections.emptyList();
    }

    GeneratedColumns(InputFactory inputFactory,
                     TransactionContext txnCtx,
                     boolean validation,
                     ReferenceResolver<CollectExpression<T, ?>> refResolver,
                     Collection<Reference> presentColumns,
                     List<GeneratedReference> allGeneratedColumns) {
        InputFactory.Context<CollectExpression<T, ?>> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        generatedToInject = new HashMap<>();
        for (GeneratedReference generatedCol : allGeneratedColumns) {
            if (!presentColumns.contains(generatedCol)) {
                generatedToInject.put(generatedCol, ctx.add(generatedCol.generatedExpression()));
            }
        }
        if (validation) {
            toValidate = new HashMap<>();
            for (var generatedCol : allGeneratedColumns) {
                // We skip validation of top level columns that are injected (since the user doesn't provide them)
                // For nested columns we can't be sure if the user provided them or not
                // ((INSERT INTO t (obj) VALUES ({a=1}), ({a=2, b=2}))
                // So we mark them as toValidate (and only validate them if actually provided)
                if (generatedCol.column().isRoot() && generatedToInject.containsKey(generatedCol)) {
                    continue;
                }
                toValidate.put(generatedCol, ctx.add(generatedCol.generatedExpression()));
            }
        } else {
            toValidate = Map.of();
        }
        expressions = ctx.expressions();
    }

    public void setNextRow(T row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
    }

    void validateValues(Map<String, Object> source) {
        for (var entry : toValidate.entrySet()) {
            GeneratedReference ref = entry.getKey();
            Object providedValue = ValueExtractors.fromMap(source, ref.column());
            if (providedValue == null && !ref.column().isRoot()) {
                // Nested columns will be present in `toValidate` even if they are *not* provided by the user but injected
                // That's because we can't be certain if they will be present or not.
                // (INSERT INTO (obj) VALUES ({a=1}), ({a=1, b=2}) -> obj is always there as `target` but child contents are dynamic.
                // If they're null here we can assume that they'll be generated after the validation
                continue;
            }
            Object generatedValue = entry.getValue().value();

            //noinspection unchecked
            DataType<Object> dataType = (DataType<Object>) ref.valueType();
            if (Comparator
                    .nullsFirst(dataType)
                    .compare(
                        dataType.sanitizeValue(generatedValue),
                        dataType.sanitizeValue(providedValue)
                    ) != 0) {
                throw new IllegalArgumentException(
                    "Given value " + providedValue +
                    " for generated column " + ref.column() +
                    " does not match calculation " + ref.formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    Iterable<? extends Map.Entry<? extends Reference, Input<?>>> generatedToInject() {
        return generatedToInject.entrySet();
    }
}
