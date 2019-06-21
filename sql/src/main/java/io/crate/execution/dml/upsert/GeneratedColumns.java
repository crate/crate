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

import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.types.ObjectType;

import java.util.ArrayList;
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

    private final Validation validation;
    private final Map<String, List<Reference>> nestedReferenceByTopLevelColumnName;
    private final Map<Reference, Input<?>> toInject;
    private final List<CollectExpression<T, ?>> expressions;

    private GeneratedColumns() {
        validation = Validation.NONE;
        nestedReferenceByTopLevelColumnName = Collections.emptyMap();
        toInject = Collections.emptyMap();
        expressions = Collections.emptyList();
    }

    public GeneratedColumns(InputFactory inputFactory,
                            TransactionContext txnCtx,
                            Validation validation,
                            ReferenceResolver<CollectExpression<T, ?>> refResolver,
                            Collection<Reference> presentColumns,
                            List<GeneratedReference> allGeneratedColumns,
                            List<Reference> allDefaultExpressionColumns) {
        this.validation = validation;
        InputFactory.Context<CollectExpression<T, ?>> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        toInject = new HashMap<>();
        nestedReferenceByTopLevelColumnName = new HashMap<>();

        for (Reference colWithDefault : allDefaultExpressionColumns) {
            //processGeneratedExpressionColumn(colWithDefault, ctx.add(colWithDefault.defaultExpression()));
            toInject.put(colWithDefault, ctx.add(colWithDefault.defaultExpression()));
        }

        for (GeneratedReference generatedCol : allGeneratedColumns) {
            processGeneratedExpressionColumn(generatedCol, ctx.add(generatedCol.generatedExpression()));
        }
        expressions = ctx.expressions();
    }

    private void processGeneratedExpressionColumn(Reference reference,
                                                  Input<?> expressionInput) {
        toInject.put(reference, expressionInput);
        final ColumnIdent ident = reference.column();
        if (!ident.isTopLevel()) {
            List<Reference> references = nestedReferenceByTopLevelColumnName.getOrDefault(
                ident.name(),
                new ArrayList<>());
            references.add(reference);
            nestedReferenceByTopLevelColumnName.put(ident.name(), references);
        }
    }

    public void setNextRow(T row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
    }

    private void validateValue(Reference target, Object generatedValue, Object providedValue) {
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

    public void validateValue(Reference target, Object providedValue) {
        validateValue(target, providedValue, validation == Validation.VALUE_MATCH);
    }

    public void validateValue(Reference target, Object providedValue, boolean validate) {
        if (providedValue == null) {
            return;
        }

        Input<?> input = toInject.get(target);
        if (input != null) {
            if (validate && target instanceof GeneratedReference) {
                validateValue(target, input.value(), providedValue);
            }
            toInject.remove(target);
        }

        if (ObjectType.ID == target.valueType().id()) {
            List<Reference> nestedGeneratedReferences = nestedReferenceByTopLevelColumnName.get(target.column().name());
            if (nestedGeneratedReferences != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> objectMap = (Map<String, Object>) providedValue;

                for (var generatedColumn : nestedGeneratedReferences) {
                    ColumnIdent generatedColumnIdent = generatedColumn.column();
                    assert (generatedColumnIdent.isTopLevel() == false) :
                        "We should be checking only nested columns at this point";
                    Object byPathValue = Maps.getByPath(objectMap, generatedColumnIdent.path());
                    // normally this should be done for validation == Validation.VALUE_MATCH
                    // however, for nested values, validation should be performed always
                    // as there is no validation happening in earlier stage (analyzer)
                    validateValue(generatedColumn, byPathValue, true);
                }
            }
        }
    }

    public Iterable<? extends Map.Entry<Reference, Input<?>>> toInject() {
        return toInject.entrySet();
    }
}
