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

import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class GeneratedColumns<T> {

    public enum Validation {
        NONE,
        VALUE_MATCH
    }

    private static final GeneratedColumns EMPTY = new GeneratedColumns();

    public static <T> GeneratedColumns<T> empty() {
        //noinspection unchecked
        return EMPTY;
    }

    private final Map<Reference, Input<?>> toValidate;
    private final Map<Reference, Input<?>> generatedToInject;
    private final Map<Reference, Input<?>> defaultsToInject;
    private final List<CollectExpression<T, ?>> expressions;

    private GeneratedColumns() {
        toValidate = Collections.emptyMap();
        generatedToInject = Collections.emptyMap();
        defaultsToInject = Collections.emptyMap();
        expressions = Collections.emptyList();
    }

    public GeneratedColumns(InputFactory inputFactory,
                            TransactionContext txnCtx,
                            Validation validation,
                            ReferenceResolver<CollectExpression<T, ?>> refResolver,
                            Collection<Reference> presentColumns,
                            List<GeneratedReference> allGeneratedColumns,
                            List<Reference> allDefaultExpressionColumns) {
        InputFactory.Context<CollectExpression<T, ?>> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        defaultsToInject = new HashMap<>();
        for (Reference colWithDefault : allDefaultExpressionColumns) {
            if (!presentColumns.contains(colWithDefault)) {
                defaultsToInject.put(colWithDefault, ctx.add(colWithDefault.defaultExpression()));
            }
        }
        generatedToInject = new HashMap<>();
        for (GeneratedReference generatedCol : allGeneratedColumns) {
            if (!presentColumns.contains(generatedCol)) {
                generatedToInject.put(generatedCol, ctx.add(generatedCol.generatedExpression()));
            }
        }
        if (validation == Validation.VALUE_MATCH) {
            toValidate = new HashMap<>();
            for (var generatedCol : allGeneratedColumns) {
                // We skip validation of top level columns that are injected (since the user doesn't provide them)
                // For nested columns we can't be sure if the user provided them or not
                // ((INSERT INTO t (obj) VALUES ({a=1}), ({a=2, b=2}))
                // So we mark them as toValidate (and only validate them if actually provided)
                if (generatedCol.column().isTopLevel() && generatedToInject.containsKey(generatedCol)) {
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

    public void validateValues(HashMap<String, Object> source) {
        for (var entry : toValidate.entrySet()) {
            Reference ref = entry.getKey();
            Object providedValue = ValueExtractors.fromMap(source, ref.column());
            if (providedValue == null && !ref.column().isTopLevel()) {
                // Nested columns will be present in `toValidate` even if they are *not* provided by the user but injected
                // That's because we can't be certain if they will be present or not.
                // (INSERT INTO (obj) VALUES ({a=1}), ({a=1, b=2}) -> obj is always there as `target` but child contents are dynamic.
                // If they're null here we can assume that they'll be generated after the validation
                continue;
            }
            Object generatedValue = entry.getValue().value();

            //noinspection unchecked
            if (ref.valueType().compareValueTo(generatedValue, providedValue) != 0) {
                throw new IllegalArgumentException(
                    "Given value " + providedValue +
                    " for generated column " + ref.column() +
                    " does not match calculation " + ((GeneratedReference) ref).formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    public Iterable<? extends Map.Entry<Reference, Input<?>>> generatedToInject() {
        return generatedToInject.entrySet();
    }

    public Iterable<? extends Map.Entry<Reference, Input<?>>> defaultsToInject() {
        return defaultsToInject.entrySet();
    }
}
