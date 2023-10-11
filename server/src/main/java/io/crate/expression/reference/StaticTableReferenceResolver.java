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

package io.crate.expression.reference;

import java.util.Map;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.expressions.RowCollectExpressionFactory;

public class StaticTableReferenceResolver<R> implements ReferenceResolver<NestableCollectExpression<R, ?>> {

    private final Map<ColumnIdent, ? extends RowCollectExpressionFactory<R>> expressionFactories;

    public StaticTableReferenceResolver(Map<ColumnIdent, ? extends RowCollectExpressionFactory<R>> expressionFactories) {
        this.expressionFactories = expressionFactories;
    }

    @Override
    public NestableCollectExpression<R, ?> getImplementation(Reference ref) {
        return rowCollectExpressionFromFactoryMap(expressionFactories, ref);
    }

    private static <R> NestableCollectExpression<R, ?> rowCollectExpressionFromFactoryMap(
        Map<ColumnIdent, ? extends RowCollectExpressionFactory<R>> factories,
        Reference ref) {

        ColumnIdent columnIdent = ref.column();
        RowCollectExpressionFactory<R> factory = factories.get(columnIdent);
        if (factory != null) {
            return factory.create();
        }
        if (columnIdent.isRoot()) {
            return null;
        }
        return getImplementationByRootTraversal(factories, columnIdent);
    }

    private static <R> NestableCollectExpression<R, ?> getImplementationByRootTraversal(
        Map<ColumnIdent, ? extends RowCollectExpressionFactory<R>> innerFactories,
        ColumnIdent columnIdent) {

        RowCollectExpressionFactory<R> factory = innerFactories.get(columnIdent.getRoot());
        if (factory == null) {
            return null;
        }
        NestableInput<?> refImpl = factory.create();
        NestableInput<?> childByPath = NestableInput.getChildByPath(refImpl, columnIdent.path());
        assert childByPath instanceof NestableCollectExpression
            : "Child " + columnIdent.path() + " of " + refImpl + " must be a NestableCollectExpression";
        return (NestableCollectExpression<R, ?>) childByPath;
    }
}
