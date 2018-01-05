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

package io.crate.sql.tree;

public abstract class DefaultTraversalVisitor<R, C>
    extends AstVisitor<R, C> {
    @Override
    protected R visitExtract(Extract node, C context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitCast(Cast node, C context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitTryCast(TryCast node, C context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitArithmeticExpression(ArithmeticExpression node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, C context) {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return null;
    }

    @Override
    protected R visitComparisonExpression(ComparisonExpression node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context) {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        for (SortItem sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }

        return null;
    }

    @Override
    protected R visitWith(With node, C context) {
        for (WithQuery query : node.getQueries()) {
            process(query, context);
        }

        return null;
    }

    @Override
    protected R visitWithQuery(WithQuery node, C context) {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSelect(Select node, C context) {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn node, C context) {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected R visitWhenClause(WhenClause node, C context) {
        process(node.getOperand(), context);
        process(node.getResult(), context);

        return null;
    }

    @Override
    protected R visitInPredicate(InPredicate node, C context) {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return null;
    }

    @Override
    protected R visitFunctionCall(FunctionCall node, C context) {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
        process(node.getOperand(), context);
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        if (node.getDefaultValue() != null) {
            process(node.getDefaultValue(), context);
        }

        return null;
    }

    @Override
    protected R visitInListExpression(InListExpression node, C context) {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitIfExpression(IfExpression node, C context) {
        process(node.getCondition(), context);
        process(node.getTrueValue(), context);
        if (node.getFalseValue().isPresent()) {
            process(node.getFalseValue().get(), context);
        }

        return null;
    }

    @Override
    protected R visitNegativeExpression(NegativeExpression node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitNotExpression(NotExpression node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        if (node.getDefaultValue() != null) {
            process(node.getDefaultValue(), context);
        }

        return null;
    }

    @Override
    protected R visitLikePredicate(LikePredicate node, C context) {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        if (node.getEscape() != null) {
            process(node.getEscape(), context);
        }

        return null;
    }

    @Override
    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsNullPredicate(IsNullPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, C context) {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSortItem(SortItem node, C context) {
        return process(node.getSortKey(), context);
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context) {

        // visit the from first, since this qualifies the select
        for (Relation relation : node.getFrom()) {
            process(relation, context);
        }

        process(node.getSelect(), context);
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
    protected R visitUnion(Union node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitIntersect(Intersect node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitExcept(Except node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitTableSubquery(TableSubquery node, C context) {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation node, C context) {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitJoin(Join node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinOn) {
            process(((JoinOn) node.getCriteria().get()).getExpression(), context);
        }

        return null;
    }

    @Override
    public R visitInsertFromValues(InsertFromValues node, C context) {
        process(node.table(), context);
        for (ValuesList valuesList : node.valuesLists()) {
            process(valuesList, context);
        }
        return null;
    }

    @Override
    public R visitValuesList(ValuesList node, C context) {
        for (Expression value : node.values()) {
            process(value, context);
        }
        return null;
    }

    @Override
    public R visitUpdate(Update node, C context) {
        process(node.relation(), context);
        for (Assignment assignment : node.assignements()) {
            process(assignment, context);
        }
        if (node.whereClause().isPresent()) {
            process(node.whereClause().get(), context);
        }
        return null;
    }

    @Override
    public R visitDelete(Delete node, C context) {
        process(node.getRelation(), context);
        return null;
    }

    @Override
    public R visitCopyFrom(CopyFrom node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitCopyTo(CopyTo node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitAlterTable(AlterTable node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitInsertFromSubquery(InsertFromSubquery node, C context) {
        process(node.table(), context);
        process(node.subQuery(), context);
        return null;
    }

    @Override
    public R visitDropTable(DropTable node, C context) {
        process(node.table(), context);
        return super.visitDropTable(node, context);
    }

    @Override
    public R visitCreateTable(CreateTable node, C context) {
        process(node.name(), context);
        return null;
    }

    @Override
    public R visitShowCreateTable(ShowCreateTable node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitRefreshStatement(RefreshStatement node, C context) {
        for (Table nodeTable : node.tables()) {
            process(nodeTable, context);
        }
        return null;
    }

    @Override
    public R visitMatchPredicate(MatchPredicate node, C context) {
        for (MatchPredicateColumnIdent columnIdent : node.idents()) {
            process(columnIdent.columnIdent(), context);
            process(columnIdent.boost(), context);
        }
        process(node.value(), context);

        return null;
    }
}
