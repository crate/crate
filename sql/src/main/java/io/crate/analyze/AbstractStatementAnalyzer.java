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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.crate.metadata.TableIdent;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractStatementAnalyzer<R extends Object, T extends Analysis> extends DefaultTraversalVisitor<R, T> {
    protected static final OutputNameFormatter outputNameFormatter = new OutputNameFormatter();

    protected static final SubscriptVisitor visitor = new SubscriptVisitor();
    protected static final NegativeLiteralVisitor negativeLiteralVisitor = new NegativeLiteralVisitor();

    static class OutputNameFormatter extends ExpressionFormatter.Formatter {
        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {

            List<String> parts = new ArrayList<>();
            for (String part : node.getName().getParts()) {
                parts.add(part);
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return String.format("%s[%s]", process(node.name(), null), process(node.index(), null));
        }
    }

    public abstract Analysis newAnalysis(Analyzer.ParameterContext parameterContext);

    @Override
    protected R visitTable(Table node, T context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        TableIdent tableIdent = TableIdent.of(node);
        context.table(tableIdent);
        return null;
    }

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
    protected R visitCast(Cast node, T context) {
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
