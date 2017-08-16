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

package io.crate.analyze.expressions;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.Expression;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class ExpressionAnalysisContext {

    private final Map<Expression, Object> arrayExpressionsChildren = new IdentityHashMap<>();

    public boolean hasAggregates = false;

    Function allocateFunction(FunctionInfo functionInfo, List<Symbol> arguments) {
        Function newFunction = new Function(functionInfo, arguments);
        hasAggregates = hasAggregates || functionInfo.type() == FunctionInfo.Type.AGGREGATE;
        return newFunction;
    }

    /**
     * Registers the given expression as the child of an ArrayComparisonExpression.
     * Can be used by downstream operators to check if an expression is part of an
     * {@link ArrayComparisonExpression}.
     * @param arrayExpressionChild the expression to register
     */
    void registerArrayComparisonChild(Expression arrayExpressionChild) {
        arrayExpressionsChildren.put(arrayExpressionChild, null);
    }

    /**
     * Checks if the given Expression is part of an {@link ArrayComparisonExpression}.
     * @return True if the given expression has previously been registered.
     */
    boolean isArrayComparisonChild(Expression expression) {
        return arrayExpressionsChildren.containsKey(expression);
    }

}
