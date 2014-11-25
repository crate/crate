/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.sql.tree.*;

public abstract class AbstractStatementAnalyzer<R, T extends AnalyzedStatement> extends DefaultTraversalVisitor<R, T> {

    @Deprecated
    public abstract AnalyzedStatement newAnalysis(ParameterContext parameterContext);

    @Override
    protected R visitNode(Node node, T context) {
        throw new UnsupportedOperationException("Unsupported statement.");
    }

    /*
     * not supported yet expressions
     *
     * remove those methods if expressions gets supported
     */
    @Override
    protected R visitExtract(Extract node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitCoalesceExpression(CoalesceExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWith(With node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWithQuery(WithQuery node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitWhenClause(WhenClause node, T context) {
        return visitNode(node, context);
    }

    @Override
    public R visitWindow(Window node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitNullIfExpression(NullIfExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitIfExpression(IfExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSearchedCaseExpression(SearchedCaseExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitUnion(Union node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitIntersect(Intersect node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitExcept(Except node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitTableSubquery(TableSubquery node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitJoin(Join node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitSampledRelation(SampledRelation node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitCurrentTime(CurrentTime node, T context) {
        return visitNode(node, context);
    }

    @Override
    protected R visitExists(ExistsPredicate node, T context) {
        return visitNode(node, context);
    }

    @Override
    public R visitInputReference(InputReference node, T context) {
        return visitNode(node, context);
    }

    @Override
    public R visitMatchPredicate(MatchPredicate node, T context) {
        return visitNode(node, context);
    }
}
