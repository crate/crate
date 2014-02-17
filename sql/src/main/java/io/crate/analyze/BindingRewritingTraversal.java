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

public class BindingRewritingTraversal extends AstVisitor<Expression, Analysis> {

    private final AstVisitor<Expression, Analysis> rewriter;

    public BindingRewritingTraversal(AstVisitor<Expression, Analysis> rewriter) {
        this.rewriter = rewriter;
    }

    @Override
    protected Expression visitQuery(Query node, Analysis context) {
        process(node.getQueryBody(), context);
        return null;
    }

    @Override
    protected Expression visitQuerySpecification(QuerySpecification node, Analysis context) {
        process(node.getSelect(), context);
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }

        for (Expression expression : node.getGroupBy()) {
            process(expression, context);
        }

        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }

        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }

        return null;
    }

    @Override
    protected Expression visitSelect(Select node, Analysis context) {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }
        return null;
    }

    @Override
    protected Expression visitSingleColumn(SingleColumn node, Analysis context) {
        node.accept(rewriter, context);
        return null;
    }

    @Override
    protected Expression visitAllColumns(AllColumns node, Analysis context) {
        return super.visitAllColumns(node, context);
    }
}
