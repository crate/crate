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


import javax.annotation.Nullable;

public abstract class AstVisitor<R, C> {
    public R process(Node node, @Nullable C context) {
        return node.accept(this, context);
    }

    protected R visitNode(Node node, C context) {
        return null;
    }

    protected R visitExpression(Expression node, C context) {
        return visitNode(node, context);
    }

    protected R visitCurrentTime(CurrentTime node, C context) {
        return visitExpression(node, context);
    }

    protected R visitExtract(Extract node, C context) {
        return visitExpression(node, context);
    }

    protected R visitArithmeticExpression(ArithmeticExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitComparisonExpression(ComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLiteral(Literal node, C context) {
        return visitExpression(node, context);
    }

    protected R visitDateLiteral(DateLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitDoubleLiteral(DoubleLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitStatement(Statement node, C context) {
        return visitNode(node, context);
    }

    protected R visitQuery(Query node, C context) {
        return visitStatement(node, context);
    }

    protected R visitExplain(Explain node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowTables(ShowTables node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowSchemas(ShowSchemas node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowColumns(ShowColumns node, C context) {
        return visitStatement(node, context);
    }

    protected R visitTimeLiteral(TimeLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitSelect(Select node, C context) {
        return visitNode(node, context);
    }

    protected R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context) {
        return visitRelation(node, context);
    }

    protected R visitQuerySpecification(QuerySpecification node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitSetOperation(SetOperation node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitUnion(Union node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitIntersect(Intersect node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitExcept(Except node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitTimestampLiteral(TimestampLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitWhenClause(WhenClause node, C context) {
        return visitExpression(node, context);
    }

    protected R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitFunctionCall(FunctionCall node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitStringLiteral(StringLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitInListExpression(InListExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitQualifiedNameReference(QualifiedNameReference node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIfExpression(IfExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNullLiteral(NullLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitNegativeExpression(NegativeExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNotExpression(NotExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSelectItem(SelectItem node, C context) {
        return visitNode(node, context);
    }

    protected R visitSingleColumn(SingleColumn node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitAllColumns(AllColumns node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLongLiteral(LongLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSubqueryExpression(SubqueryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSortItem(SortItem node, C context) {
        return visitNode(node, context);
    }

    protected R visitTable(Table node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitTableSubquery(TableSubquery node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitAliasedRelation(AliasedRelation node, C context) {
        return visitRelation(node, context);
    }

    protected R visitJoin(Join node, C context) {
        return visitRelation(node, context);
    }

    protected R visitExists(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitCast(Cast node, C context) {
        return visitExpression(node, context);
    }

    protected R visitTryCast(TryCast node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSubscriptExpression(SubscriptExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitParameterExpression(ParameterExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitInsert(Insert node, C context) {
        return visitStatement(node, context);
    }

    public R visitValuesList(ValuesList node, C context) {
        return visitNode(node, context);
    }

    public R visitDelete(Delete node, C context) {
        return visitStatement(node, context);
    }

    public R visitUpdate(Update node, C context) {
        return visitStatement(node, context);
    }

    public R visitAssignment(Assignment node, C context) {
        return visitNode(node, context);
    }

    public R visitCopyFrom(CopyFrom node, C context) {
        return visitNode(node, context);
    }

    public R visitCreateTable(CreateTable node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateFunction(CreateFunction node, C context) {
        return visitStatement(node, context);
    }

    public R visitFunctionArgument(FunctionArgument node, C context) {
        return visitNode(node, context);
    }

    public R visitDropFunction(DropFunction node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropUser(DropUser node, C context) {
        return visitStatement(node, context);
    }

    public R visitGrantPrivilege(GrantPrivilege node, C context) {
        return visitStatement(node, context);
    }

    public R visitDenyPrivilege(DenyPrivilege node, C context) {
        return visitStatement(node, context);
    }

    public R visitRevokePrivilege(RevokePrivilege node, C context) {
        return visitStatement(node, context);
    }

    public R visitShowCreateTable(ShowCreateTable node, C context) {
        return visitStatement(node, context);
    }

    public R visitTableElement(TableElement node, C context) {
        return visitNode(node, context);
    }

    public R visitClusteredBy(ClusteredBy node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnDefinition(ColumnDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnType(ColumnType node, C context) {
        return visitNode(node, context);
    }

    public R visitObjectColumnType(ObjectColumnType node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnConstraint(ColumnConstraint node, C context) {
        return visitNode(node, context);
    }

    public R visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, C context) {
        return visitNode(node, context);
    }

    public R visitNotNullColumnConstraint(NotNullColumnConstraint node, C context) {
        return visitNode(node, context);
    }

    public R visitIndexColumnConstraint(IndexColumnConstraint node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnStorageDefinition(ColumnStorageDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitGenericProperties(GenericProperties node, C context) {
        return visitNode(node, context);
    }

    public R visitGenericProperty(GenericProperty node, C context) {
        return visitNode(node, context);
    }

    public R visitPrimaryKeyConstraint(PrimaryKeyConstraint node, C context) {
        return visitNode(node, context);
    }

    public R visitIndexDefinition(IndexDefinition node, C context) {
        return visitNode(node, context);
    }

    public R visitCollectionColumnType(CollectionColumnType node, C context) {
        return visitNode(node, context);
    }

    public R visitDropTable(DropTable node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateAnalyzer(CreateAnalyzer node, C context) {
        return visitStatement(node, context);
    }

    public R visitTokenizer(Tokenizer node, C context) {
        return visitNode(node, context);
    }

    public R visitCharFilters(CharFilters node, C context) {
        return visitNode(node, context);
    }

    public R visitTokenFilters(TokenFilters node, C context) {
        return visitNode(node, context);
    }

    public R visitCreateBlobTable(CreateBlobTable node, C context) {
        return visitNode(node, context);
    }

    public R visitDropBlobTable(DropBlobTable node, C context) {
        return visitNode(node, context);
    }

    public R visitRefreshStatement(RefreshStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitOptimizeStatement(OptimizeStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTable(AlterTable node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableOpenClose(AlterTableOpenClose node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableRename(AlterTableRename node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableReroute(AlterTableReroute node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterBlobTable(AlterBlobTable node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterClusterRerouteRetryFailed(AlterClusterRerouteRetryFailed node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterUser(AlterUser node, C context) {
        return visitStatement(node, context);
    }

    public R visitCopyTo(CopyTo node, C context) {
        return visitStatement(node, context);
    }

    public R visitPartitionedBy(PartitionedBy node, C context) {
        return visitNode(node, context);
    }

    public R visitArrayComparisonExpression(ArrayComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrayLiteral(ArrayLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitObjectLiteral(ObjectLiteral node, C context) {
        return visitLiteral(node, context);
    }

    public R visitArrayLikePredicate(ArrayLikePredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitSetStatement(SetStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitResetStatement(ResetStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableAddColumnStatement(AlterTableAddColumn node, C context) {
        return visitStatement(node, context);
    }

    public R visitRerouteMoveShard(RerouteMoveShard node, C context) {
        return visitNode(node, context);
    }

    public R visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard node, C context) {
        return visitNode(node, context);
    }

    public R visitRerouteCancelShard(RerouteCancelShard node, C context) {
        return visitNode(node, context);
    }

    public R visitAddColumnDefinition(AddColumnDefinition node, C context) {
        return visitTableElement(node, context);
    }

    public R visitInsertFromValues(InsertFromValues node, C context) {
        return visitInsert(node, context);
    }

    public R visitInsertFromSubquery(InsertFromSubquery node, C context) {
        return visitInsert(node, context);
    }

    public R visitMatchPredicate(MatchPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitMatchPredicateColumnIdent(MatchPredicateColumnIdent node, C context) {
        return visitExpression(node, context);
    }

    public R visitKillStatement(KillStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropRepository(DropRepository node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateRepository(CreateRepository node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropSnapshot(DropSnapshot node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateSnapshot(CreateSnapshot node, C context) {
        return visitStatement(node, context);
    }

    public R visitRestoreSnapshot(RestoreSnapshot node, C context) {
        return visitStatement(node, context);
    }

    public R visitTableFunction(TableFunction node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitBegin(BeginStatement node, C context) {
        return visitStatement(node, context);
    }

    public R visitShowTransaction(ShowTransaction showTransaction, C context) {
        return visitStatement(showTransaction, context);
    }

    public R visitCreateUser(CreateUser node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateIngestRule(CreateIngestRule node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropIngestRule(DropIngestRule node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateView(CreateView createView, C context) {
        return visitStatement(createView, context);
    }

    public R visitDropView(DropView dropView, C context) {
        return visitStatement(dropView, context);
    }
}
