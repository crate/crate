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
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;

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
    private final Map<Reference, Input<?>> toInject;
    private final List<CollectExpression<T, ?>> expressions;

    private GeneratedColumns() {
        toValidate = Collections.emptyMap();
        toInject = Collections.emptyMap();
        expressions = Collections.emptyList();
    }

    public GeneratedColumns(InputFactory inputFactory,
                            Validation validation,
                            ReferenceResolver<CollectExpression<T, ?>> refResolver,
                            Collection<Reference> presentColumns,
                            List<GeneratedReference> allGeneratedColumns) {
        toValidate = validation == Validation.NONE ? Collections.emptyMap() : new HashMap<>();
        InputFactory.Context<CollectExpression<T, ?>> ctx = inputFactory.ctxForRefs(refResolver);
        toInject = new HashMap<>();
        for (GeneratedReference generatedCol : allGeneratedColumns) {
            if (presentColumns.contains(generatedCol)) {
                if (validation == Validation.VALUE_MATCH) {
                    toValidate.put(generatedCol, ctx.add(generatedCol.generatedExpression()));
                }
            } else {
                toInject.put(generatedCol, ctx.add(generatedCol.generatedExpression()));
            }
        }
        expressions = ctx.expressions();
    }

    public void setNextRow(T row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
    }

    public void validateValue(Reference target, Object providedValue) {
        Input<?> input = toValidate.get(target);
        if (input != null) {
            Object generatedValue = input.value();
            //noinspection unchecked
            if (target.valueType().compareValueTo(generatedValue, providedValue) != 0) {
                throw new IllegalArgumentException(
                    "Given value " + providedValue +
                    " for generated column " + target.column() +
                    " does not match calculation " + ((GeneratedReference) target).formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    public Iterable<? extends Map.Entry<Reference, Input<?>>> toInject() {
        return toInject.entrySet();
    }

}
