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

import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SubqueryExpression;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * State which is passed during translation in the {@link ExpressionAnalyzer}.
 */
public class ExpressionAnalysisContext {

    private final ArrayChildVisitor arrayChildVisitor = new ArrayChildVisitor();
    private final Map<SubqueryExpression, Object> arrayExpressionsChildren = new IdentityHashMap<>();

    private boolean hasAggregates;
    private boolean allowEagerNormalize = true;

    void indicateAggregates() {
        hasAggregates = true;
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    public boolean isEagerNormalizationAllowed() {
        return allowEagerNormalize;
    }

    public void allowEagerNormalize(boolean value) {
        this.allowEagerNormalize = value;
    }

    /**
     * Registers the given expression as the child of an Array Expression.
     * Example of usage: Can be used by downstream operators to check if a SubqueryExpression is part of
     * an {@link ArrayComparisonExpression}.
     * @param arrayExpressionChild the expression to register
     */
    void registerArrayChild(Expression arrayExpressionChild) {
        arrayChildVisitor.process(arrayExpressionChild, null);
    }

    /**
     * Checks if the given SubqueryExpression is part of an Array Expression.
     * @return True if the given expression has previously been registered.
     */
    boolean isArrayChild(SubqueryExpression expression) {
        return arrayExpressionsChildren.containsKey(expression);
    }

    private class ArrayChildVisitor extends DefaultTraversalVisitor<Void, Void> {

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, Void context) {
            arrayExpressionsChildren.put(node, null);
            return null;
        }
    }
}
