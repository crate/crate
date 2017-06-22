/*
 * Licensed to Crate.io Inc. or its affiliates ("Crate.io") under one or
 * more contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Crate.io licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * However, if you have executed another commercial license agreement with
 * Crate.io these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import io.crate.sql.parser.antlr.v4.SqlBaseBaseVisitor;
import io.crate.sql.parser.antlr.v4.SqlBaseLexer;
import io.crate.sql.parser.antlr.v4.SqlBaseParser;
import io.crate.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

class AstBuilder extends SqlBaseBaseVisitor<Node> {

    private int parameterPosition = 1;

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context) {
        return visit(context.expr());
    }

    //  Statements

    @Override
    public Node visitBegin(SqlBaseParser.BeginContext context) {
        return new BeginStatement();
    }

    @Override
    public Node visitOptimize(SqlBaseParser.OptimizeContext context) {
        return new OptimizeStatement(
            visit(context.tableWithPartitions().tableWithPartition(), Table.class),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
        boolean notExists = context.EXISTS() != null;
        return new CreateTable(
            (Table) visit(context.table()),
            visit(context.tableElement(), TableElement.class),
            visit(context.crateTableOption(), CrateTableOption.class),
            visitIfPresent(context.withProperties(), GenericProperties.class),
            notExists);
    }

    @Override
    public Node visitCreateBlobTable(SqlBaseParser.CreateBlobTableContext context) {
        return new CreateBlobTable(
            (Table) visit(context.table()),
            visitIfPresent(context.numShards, ClusteredBy.class),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitCreateRepository(SqlBaseParser.CreateRepositoryContext context) {
        return new CreateRepository(
            getIdentText(context.name),
            getIdentText(context.type),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitCreateSnapshot(SqlBaseParser.CreateSnapshotContext context) {
        if (context.ALL() != null) {
            return new CreateSnapshot(
                getQualifiedName(context.qname()),
                visitIfPresent(context.withProperties(), GenericProperties.class));
        }
        return new CreateSnapshot(
            getQualifiedName(context.qname()),
            visit(context.tableWithPartitions().tableWithPartition(), Table.class),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitCreateAnalyzer(SqlBaseParser.CreateAnalyzerContext context) {
        return new CreateAnalyzer(
            getIdentText(context.name),
            getIdentTextIfPresent(context.extendedName),
            visit(context.analyzerElement(), AnalyzerElement.class)
        );
    }

    @Override
    public Node visitCreateUser(SqlBaseParser.CreateUserContext context) {
        return new CreateUser(getIdentText(context.name));
    }

    @Override
    public Node visitDropUser(SqlBaseParser.DropUserContext context) {
        return new DropUser(
            getIdentText(context.name),
            context.EXISTS() != null
        );
    }

    @Override
    public Node visitGrantPrivilege(SqlBaseParser.GrantPrivilegeContext context) {
        List<String> usernames = identsToStrings(context.ident());

        if (context.ALL() != null) {
            return new GrantPrivilege(usernames);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new GrantPrivilege(usernames, privilegeTypes);
        }
    }

    @Override
    public Node visitDenyPrivilege(SqlBaseParser.DenyPrivilegeContext context) {
        List<String> usernames = identsToStrings(context.ident());

        if (context.ALL() != null) {
            return new DenyPrivilege(usernames);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new DenyPrivilege(usernames, privilegeTypes);
        }
    }

    @Override
    public Node visitRevokePrivilege(SqlBaseParser.RevokePrivilegeContext context) {
        List<String> usernames = identsToStrings(context.ident());

        if (context.ALL() != null) {
            return new RevokePrivilege(usernames);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new RevokePrivilege(usernames, privilegeTypes);
        }
    }

    @Override
    public Node visitCharFilters(SqlBaseParser.CharFiltersContext context) {
        return new CharFilters(visit(context.namedProperties(), NamedProperties.class));
    }

    @Override
    public Node visitTokenFilters(SqlBaseParser.TokenFiltersContext context) {
        return new TokenFilters(visit(context.namedProperties(), NamedProperties.class));
    }

    @Override
    public Node visitTokenizer(SqlBaseParser.TokenizerContext context) {
        return new Tokenizer((NamedProperties) visit(context.namedProperties()));
    }

    @Override
    public Node visitNamedProperties(SqlBaseParser.NamedPropertiesContext context) {
        return new NamedProperties(
            getIdentText(context.ident()),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitRestore(SqlBaseParser.RestoreContext context) {
        if (context.ALL() != null) {
            return new RestoreSnapshot(
                getQualifiedName(context.qname()),
                visitIfPresent(context.withProperties(), GenericProperties.class));
        }
        return new RestoreSnapshot(getQualifiedName(context.qname()),
            visit(context.tableWithPartitions().tableWithPartition(), Table.class),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context) {
        return new ShowCreateTable((Table) visit(context.table()));
    }

    @Override
    public Node visitShowTransaction(SqlBaseParser.ShowTransactionContext context) {
        return new ShowTransaction();
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context) {
        return new DropTable((Table) visit(context.table()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropRepository(SqlBaseParser.DropRepositoryContext context) {
        return new DropRepository(getIdentText(context.ident()));
    }

    @Override
    public Node visitDropBlobTable(SqlBaseParser.DropBlobTableContext context) {
        return new DropBlobTable((Table) visit(context.table()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropSnapshot(SqlBaseParser.DropSnapshotContext context) {
        return new DropSnapshot(getQualifiedName(context.qname()));
    }

    @Override
    public Node visitCopyFrom(SqlBaseParser.CopyFromContext context) {
        return new CopyFrom(
            (Table) visit(context.tableWithPartition()),
            (Expression) visit(context.path),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitCopyTo(SqlBaseParser.CopyToContext context) {
        List<Expression> columns = Optional.ofNullable(context.columns())
            .map(list -> visit(list.primaryExpression(), Expression.class))
            .orElse(null);

        return new CopyTo(
            (Table) visit(context.tableWithPartition()),
            columns,
            visitIfPresent(context.where(), Expression.class),
            context.DIRECTORY() != null,
            (Expression) visit(context.path),
            visitIfPresent(context.withProperties(), GenericProperties.class));
    }

    @Override
    public Node visitInsert(SqlBaseParser.InsertContext context) {
        List<String> columns = identsToStrings(context.ident());

        if (context.insertSource().VALUES() != null) {
            return new InsertFromValues(
                (Table) visit(context.table()),
                visit(context.insertSource().values(), ValuesList.class),
                columns,
                visit(context.assignment(), Assignment.class));
        }
        return new InsertFromSubquery(
            (Table) visit(context.table()),
            (Query) visit(context.insertSource().query()),
            columns,
            visit(context.assignment(), Assignment.class));
    }

    @Override
    public Node visitValues(SqlBaseParser.ValuesContext context) {
        return new ValuesList(visit(context.expr(), Expression.class));
    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext context) {
        return new Delete(
            (Relation) visit(context.aliasedRelation()),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitUpdate(SqlBaseParser.UpdateContext context) {
        return new Update(
            (Relation) visit(context.aliasedRelation()),
            visit(context.assignment(), Assignment.class),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitSet(SqlBaseParser.SetContext context) {
        Assignment setAssignment = prepareSetAssignment(context);
        if (context.LOCAL() != null) {
            return new SetStatement(SetStatement.Scope.LOCAL, setAssignment);
        }
        return new SetStatement(SetStatement.Scope.SESSION, setAssignment);
    }

    private Assignment prepareSetAssignment(SqlBaseParser.SetContext context) {
        Expression settingName = new QualifiedNameReference(getQualifiedName(context.qname()));
        if (context.DEFAULT() != null) {
            return new Assignment(settingName, ImmutableList.of());
        }
        return new Assignment(settingName, visit(context.setExpr(), Expression.class));
    }

    @Override
    public Node visitSetGlobal(SqlBaseParser.SetGlobalContext context) {
        if (context.PERSISTENT() != null) {
            return new SetStatement(SetStatement.Scope.GLOBAL,
                SetStatement.SettingType.PERSISTENT,
                visit(context.setGlobalAssignment(), Assignment.class));
        }
        return new SetStatement(SetStatement.Scope.GLOBAL, visit(context.setGlobalAssignment(), Assignment.class));
    }

    @Override
    public Node visitResetGlobal(SqlBaseParser.ResetGlobalContext context) {
        return new ResetStatement(visit(context.primaryExpression(), Expression.class));
    }

    @Override
    public Node visitKill(SqlBaseParser.KillContext context) {
        if (context.ALL() != null) {
            return new KillStatement();
        }
        return new KillStatement((Expression) visit(context.jobId));
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context) {
        return new Explain((Statement) visit(context.statement()));
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context) {
        return new ShowTables(
            Optional.ofNullable(context.qname()).map(this::getQualifiedName),
            getTextIfPresent(context.pattern).map(AstBuilder::unquote),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context) {
        return new ShowSchemas(
            getTextIfPresent(context.pattern).map(AstBuilder::unquote),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
        return new ShowColumns(
            getQualifiedName(context.tableName),
            Optional.ofNullable(context.schema).map(this::getQualifiedName),
            visitIfPresent(context.where(), Expression.class),
            getTextIfPresent(context.pattern).map(AstBuilder::unquote));
    }

    @Override
    public Node visitRefreshTable(SqlBaseParser.RefreshTableContext context) {
        return new RefreshStatement(visit(context.tableWithPartitions().tableWithPartition(), Table.class));
    }

    @Override
    public Node visitTableOnly(SqlBaseParser.TableOnlyContext context) {
        return new Table(getQualifiedName(context.qname()));
    }

    @Override
    public Node visitTableWithPartition(SqlBaseParser.TableWithPartitionContext context) {
        return new Table(getQualifiedName(context.qname()), visit(context.assignment(), Assignment.class));
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context) {
        QualifiedName functionName = getQualifiedName(context.name);
        validateFunctionName(functionName);
        return new CreateFunction(
            functionName,
            context.REPLACE() != null,
            visit(context.functionArgument(), FunctionArgument.class),
            (ColumnType) visit(context.returnType),
            (Expression) visit(context.language),
            (Expression) visit(context.body));
    }

    @Override
    public Node visitDropFunction(SqlBaseParser.DropFunctionContext context) {
        QualifiedName functionName = getQualifiedName(context.name);
        validateFunctionName(functionName);

        return new DropFunction(
            functionName,
            context.EXISTS() != null,
            visit(context.functionArgument(), FunctionArgument.class));
    }

    // Column / Table definition

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context) {
        if (context.generatedColumnDefinition() != null) {
            return visit(context.generatedColumnDefinition());
        }
        return new ColumnDefinition(
            getIdentText(context.ident()),
            null,
            visitIfPresent(context.dataType(), ColumnType.class).orElse(null),
            visit(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitGeneratedColumnDefinition(SqlBaseParser.GeneratedColumnDefinitionContext context) {
        return new ColumnDefinition(
            getIdentText(context.ident()),
            visitIfPresent(context.generatedExpr, Expression.class).orElse(null),
            visitIfPresent(context.dataType(), ColumnType.class).orElse(null),
            visit(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitColumnConstraintPrimaryKey(SqlBaseParser.ColumnConstraintPrimaryKeyContext context) {
        return new PrimaryKeyColumnConstraint();
    }

    @Override
    public Node visitColumnConstraintNotNull(SqlBaseParser.ColumnConstraintNotNullContext context) {
        return new NotNullColumnConstraint();
    }

    @Override
    public Node visitPrimaryKeyConstraint(SqlBaseParser.PrimaryKeyConstraintContext context) {
        return new PrimaryKeyConstraint(visit(context.columns().primaryExpression(), Expression.class));
    }

    @Override
    public Node visitColumnIndexOff(SqlBaseParser.ColumnIndexOffContext context) {
        return IndexColumnConstraint.OFF;
    }

    @Override
    public Node visitColumnIndexConstraint(SqlBaseParser.ColumnIndexConstraintContext context) {
        return new IndexColumnConstraint(
            getIdentText(context.method),
            visitIfPresent(context.withProperties(), GenericProperties.class)
                .orElse(GenericProperties.EMPTY));
    }

    @Override
    public Node visitIndexDefinition(SqlBaseParser.IndexDefinitionContext context) {
        return new IndexDefinition(
            getIdentText(context.name),
            getIdentText(context.method),
            visit(context.columns().primaryExpression(), Expression.class),
            visitIfPresent(context.withProperties(), GenericProperties.class)
                .orElse(GenericProperties.EMPTY));
    }

    @Override
    public Node visitPartitionedBy(SqlBaseParser.PartitionedByContext context) {
        return new PartitionedBy(visit(context.columns().primaryExpression(), Expression.class));
    }

    @Override
    public Node visitClusteredBy(SqlBaseParser.ClusteredByContext context) {
        return new ClusteredBy(
            visitIfPresent(context.routing, Expression.class),
            visitIfPresent(context.numShards, Expression.class));
    }

    @Override
    public Node visitClusteredInto(SqlBaseParser.ClusteredIntoContext context) {
        return new ClusteredBy(null, visitIfPresent(context.numShards, Expression.class));
    }

    @Override
    public Node visitFunctionArgument(SqlBaseParser.FunctionArgumentContext context) {
        return new FunctionArgument(getIdentTextIfPresent(context.ident()), (ColumnType) visit(context.dataType()));
    }

    // Properties

    @Override
    public Node visitWithGenericProperties(SqlBaseParser.WithGenericPropertiesContext context) {
        return visitGenericProperties(context.genericProperties());
    }

    @Override
    public Node visitGenericProperties(SqlBaseParser.GenericPropertiesContext context) {
        GenericProperties properties = new GenericProperties();
        context.genericProperty().forEach(p -> properties.add((GenericProperty) visit(p)));
        return properties;
    }

    @Override
    public Node visitGenericProperty(SqlBaseParser.GenericPropertyContext context) {
        return new GenericProperty(getIdentText(context.ident()), (Expression) visit(context.expr()));
    }

    // Amending tables

    @Override
    public Node visitAlterTableProperties(SqlBaseParser.AlterTablePropertiesContext context) {
        Table name = (Table) visit(context.alterTableDefinition());
        if (context.SET() != null) {
            return new AlterTable(name, (GenericProperties) visit(context.genericProperties()));
        }
        return new AlterTable(name, identsToStrings(context.ident()));
    }

    @Override
    public Node visitAlterBlobTableProperties(SqlBaseParser.AlterBlobTablePropertiesContext context) {
        Table name = (Table) visit(context.alterTableDefinition());
        if (context.SET() != null) {
            return new AlterBlobTable(name, (GenericProperties) visit(context.genericProperties()));
        }
        return new AlterBlobTable(name, identsToStrings(context.ident()));
    }

    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context) {
        return new AlterTableAddColumn(
            (Table) visit(context.alterTableDefinition()),
            (AddColumnDefinition) visit(context.addColumnDefinition()));
    }

    @Override
    public Node visitAddColumnDefinition(SqlBaseParser.AddColumnDefinitionContext context) {
        if (context.addGeneratedColumnDefinition() != null) {
            return visit(context.addGeneratedColumnDefinition());
        }
        return new AddColumnDefinition(
            (Expression) visit(context.subscriptSafe()),
            null,
            visitIfPresent(context.dataType(), ColumnType.class).orElse(null),
            visit(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitAddGeneratedColumnDefinition(SqlBaseParser.AddGeneratedColumnDefinitionContext context) {
        return new AddColumnDefinition(
            (Expression) visit(context.subscriptSafe()),
            visitIfPresent(context.generatedExpr, Expression.class).orElse(null),
            visitIfPresent(context.dataType(), ColumnType.class).orElse(null),
            visit(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitAlterTableOpenClose(SqlBaseParser.AlterTableOpenCloseContext context) {
        return new AlterTableOpenClose(
            (Table) visit(context.alterTableDefinition()),
            context.OPEN() != null
        );
    }

    @Override
    public Node visitAlterTableRename(SqlBaseParser.AlterTableRenameContext context) {
        return new AlterTableRename(
            (Table) visit(context.alterTableDefinition()),
            getQualifiedName(context.qname())
        );
    }

    @Override
    public Node visitSetGlobalAssignment(SqlBaseParser.SetGlobalAssignmentContext context) {
        return new Assignment((Expression) visit(context.primaryExpression()), (Expression) visit(context.expr()));
    }

    @Override
    public Node visitAssignment(SqlBaseParser.AssignmentContext context) {
        Expression column = (Expression) visit(context.primaryExpression());
        // such as it is currently hard to restrict a left side of an assignment to subscript and
        // qname in the grammar, because of our current grammar structure which causes the
        // indirect left-side recursion when attempting to do so. We restrict it before initializing
        // an Assignment.
        if (column instanceof SubscriptExpression || column instanceof QualifiedNameReference) {
            return new Assignment(column, (Expression) visit(context.expr()));
        }
        throw new IllegalArgumentException(
            String.format(Locale.ENGLISH, "cannot use expression %s as a left side of an assignment", column));
    }

    // Query specification

    @Override
    public Node visitQuery(SqlBaseParser.QueryContext context) {
        Query body = (Query) visit(context.queryNoWith());
        return new Query(
            visitIfPresent(context.with(), With.class),
            body.getQueryBody(),
            body.getOrderBy(),
            body.getLimit(),
            body.getOffset());
    }

    @Override
    public Node visitWith(SqlBaseParser.WithContext context) {
        return new With(context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context) {
        return new WithQuery(
            getIdentText(context.name),
            (Query) visit(context.query()),
            Optional.ofNullable(getColumnAliases(context.aliasedColumns())).orElse(ImmutableList.of()));
    }

    @Override
    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context) {
        QueryBody term = (QueryBody) visit(context.queryTerm());
        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                Optional.empty(),
                new QuerySpecification(
                    query.getSelect(),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    visit(context.sortItem(), SortItem.class),
                    visitIfPresent(context.limit, Expression.class),
                    visitIfPresent(context.offset, Expression.class)),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty());
        }
        return new Query(
            Optional.empty(),
            term,
            visit(context.sortItem(), SortItem.class),
            visitIfPresent(context.limit, Expression.class),
            visitIfPresent(context.offset, Expression.class));
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = null;
        if (context.FROM() != null) {
            relations = visit(context.relation(), Relation.class);
        }

        return new QuerySpecification(
            new Select(isDistinct(context.setQuant()), selectItems),
            relations,
            visitIfPresent(context.where(), Expression.class),
            visit(context.expr(), Expression.class),
            visitIfPresent(context.having, Expression.class),
            ImmutableList.of(),
            Optional.empty(),
            Optional.empty());
    }

    @Override
    public Node visitWhere(SqlBaseParser.WhereContext context) {
        return visit(context.condition);
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext context) {
        return new SortItem(
            (Expression) visit(context.expr()),
            Optional.ofNullable(context.ordering)
                .map(AstBuilder::getOrderingType)
                .orElse(SortItem.Ordering.ASCENDING),
            Optional.ofNullable(context.nullOrdering)
                .map(AstBuilder::getNullOrderingType)
                .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuant() == null || context.setQuant().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
        if (context.qname() != null) {
            return new AllColumns(getQualifiedName(context.qname()));
        }
        return new AllColumns();
    }

    @Override
    public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
        return new SingleColumn((Expression) visit(context.expr()), getIdentTextIfPresent(context.ident()));
    }

    /*
    * case sensitivity like it is in postgres
    * see also http://www.thenextage.com/wordpress/postgresql-case-sensitivity-part-1-the-ddl/
    *
    * unfortunately this has to be done in the parser because afterwards the
    * knowledge of the IDENT / QUOTED_IDENT difference is lost
    */
    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
        return new StringLiteral(context.IDENTIFIER().getText().replace("``", "`").toLowerCase(Locale.ENGLISH));
    }

    @Override
    public Node visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext context) {
        return new StringLiteral(context.getText().replace("\"\"", "\""));
    }

    private String getIdentText(SqlBaseParser.IdentContext ident) {
        StringLiteral literal = (StringLiteral) visit(ident);
        return literal.getValue();
    }

    private Optional<String> getIdentTextIfPresent(SqlBaseParser.IdentContext ident) {
        return Optional.ofNullable(ident).map(this::getIdentText);
    }

    @Override
    public Node visitTable(SqlBaseParser.TableContext context) {
        if (context.qname() != null) {
            return new Table(getQualifiedName(context.qname()), visit(context.parameterOrLiteral(), Assignment.class));
        }
        FunctionCall fc = new FunctionCall(
            getQualifiedName(context.ident()), visit(context.parameterOrLiteral(), Expression.class));
        return new TableFunction(fc);
    }

    // Boolean expressions

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
        return new NotExpression((Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
        return new LogicalBinaryExpression(
            getLogicalBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    // From clause

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (context.NATURAL() != null) {
            right = (Relation) visit(context.right);
            criteria = new NaturalJoin();
        } else {
            right = (Relation) visit(context.rightRelation);
            if (context.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
            } else if (context.joinCriteria().USING() != null) {
                List<String> columns = identsToStrings(context.joinCriteria().ident());
                criteria = new JoinUsing(columns);
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }
        return new Join(getJoinType(context.joinType()), left, right, Optional.of(criteria));
    }

    private static Join.Type getJoinType(SqlBaseParser.JoinTypeContext joinTypeContext) {
        Join.Type joinType;
        if (joinTypeContext.LEFT() != null) {
            joinType = Join.Type.LEFT;
        } else if (joinTypeContext.RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        } else if (joinTypeContext.FULL() != null) {
            joinType = Join.Type.FULL;
        } else {
            joinType = Join.Type.INNER;
        }
        return joinType;
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.ident() == null) {
            return child;
        }
        return new AliasedRelation(child, getIdentText(context.ident()), getColumnAliases(context.aliasedColumns()));
    }

    @Override
    public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
        return new TableSubquery((Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
        return visit(context.relation());
    }

    // Predicates

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }
        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext context) {
        return new ComparisonExpression(
            getComparisonOperator(((TerminalNode) context.cmpOp().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
        Expression expression = new ComparisonExpression(
            ComparisonExpression.Type.IS_DISTINCT_FROM,
            (Expression) visit(context.value),
            (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(expression);
        }
        return expression;
    }

    @Override
    public Node visitBetween(SqlBaseParser.BetweenContext context) {
        Expression expression = new BetweenPredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.lower),
            (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(expression);
        }
        return expression;
    }

    @Override
    public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(child);
        }
        return new IsNotNullPredicate(child);
    }

    @Override
    public Node visitLike(SqlBaseParser.LikeContext context) {
        Expression escape = null;
        if (context.escape != null) {
            escape = (Expression) visit(context.escape);
        }

        Expression result = new LikePredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.pattern),
            escape);

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }
        return result;
    }

    @Override
    public Node visitArrayLike(SqlBaseParser.ArrayLikeContext context) {
        boolean inverse = context.NOT() != null;
        return new ArrayLikePredicate(
            getComparisonQuantifier(((TerminalNode) context.setCmpQuantifier().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.v),
            visitIfPresent(context.escape, Expression.class).orElse(null),
            inverse);
    }

    @Override
    public Node visitInList(SqlBaseParser.InListContext context) {
        Expression result = new InPredicate(
            (Expression) visit(context.value),
            new InListExpression(visit(context.expr(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }
        return result;
    }

    @Override
    public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
        Expression result = new InPredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.subqueryExpression()));

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }
        return result;
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext context) {
        return new ExistsPredicate((Query) visit(context.query()));
    }

    @Override
    public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context) {
        return new ArrayComparisonExpression(
            getComparisonOperator(((TerminalNode) context.cmpOp().getChild(0)).getSymbol()),
            getComparisonQuantifier(((TerminalNode) context.setCmpQuantifier().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.parenthesizedPrimaryExpressionOrSubquery()));
    }

    @Override
    public Node visitMatch(SqlBaseParser.MatchContext context) {
        SqlBaseParser.MatchPredicateIdentsContext predicateIdents = context.matchPredicateIdents();
        List<MatchPredicateColumnIdent> idents;

        if (predicateIdents.matchPred != null) {
            idents = ImmutableList.of((MatchPredicateColumnIdent) visit(predicateIdents.matchPred));
        } else {
            idents = visit(predicateIdents.matchPredicateIdent(), MatchPredicateColumnIdent.class);
        }
        return new MatchPredicate(
            idents,
            (Expression) visit(context.term),
            getIdentTextIfPresent(context.matchType).orElse(null),
            visitIfPresent(context.withProperties(), GenericProperties.class).orElse(null));
    }

    @Override
    public Node visitMatchPredicateIdent(SqlBaseParser.MatchPredicateIdentContext context) {
        return new MatchPredicateColumnIdent(
            (Expression) visit(context.subscriptSafe()),
            visitIfPresent(context.boost, Expression.class).orElse(null));
    }

    // Value expressions

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
        switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return new NegativeExpression((Expression) visit(context.valueExpression()));
            case SqlBaseLexer.PLUS:
                return visit(context.valueExpression());
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
        return new ArithmeticExpression(
            getArithmeticBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
        return new FunctionCall(
            QualifiedName.of("concat"), ImmutableList.of(
            (Expression) visit(context.left),
            (Expression) visit(context.right)));
    }

    @Override
    public Node visitDoubleColonCast(SqlBaseParser.DoubleColonCastContext context) {
        return new Cast((Expression) visit(context.valueExpression()), (ColumnType) visit(context.dataType()));
    }

    // Primary expressions

    @Override
    public Node visitCast(SqlBaseParser.CastContext context) {
        if (context.TRY_CAST() != null) {
            return new TryCast((Expression) visit(context.expr()), (ColumnType) visit(context.dataType()));
        } else {
            return new Cast((Expression) visit(context.expr()), (ColumnType) visit(context.dataType()));
        }
    }

    @Override
    public Node visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext context) {
        CurrentTime.Type type = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(type, Integer.parseInt(context.precision.getText()));
        }
        return new CurrentTime(type);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext context) {
        return new Extract((Expression) visit(context.expr()), (Expression) visit(context.identExpr()));
    }

    @Override
    public Node visitSubstring(SqlBaseParser.SubstringContext context) {
        return new FunctionCall(QualifiedName.of("substr"), visit(context.expr(), Expression.class));
    }

    @Override
    public Node visitCurrentSchema(SqlBaseParser.CurrentSchemaContext context) {
        return new FunctionCall(QualifiedName.of("current_schema"), ImmutableList.of());

    }

    @Override
    public Node visitNestedExpression(SqlBaseParser.NestedExpressionContext context) {
        return visit(context.expr());
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
        return new SubqueryExpression((Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedPrimaryExpression(SqlBaseParser.ParenthesizedPrimaryExpressionContext context) {
        return visit(context.primaryExpression());
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context) {
        return new QualifiedNameReference(
            QualifiedName.of(identsToStrings(context.ident()))
        );
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
        return new QualifiedNameReference(QualifiedName.of(getIdentText(context.ident())));
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
        return new SubscriptExpression((Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubscriptSafe(SqlBaseParser.SubscriptSafeContext context) {
        if (context.qname() != null) {
            return new QualifiedNameReference(getQualifiedName(context.qname()));
        }
        return new SubscriptExpression((Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitQname(SqlBaseParser.QnameContext context) {
        return new QualifiedNameReference(getQualifiedName(context));
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
        return new SimpleCaseExpression(
            (Expression) visit(context.valueExpression()),
            visit(context.whenClause(), WhenClause.class),
            visitIfPresent(context.elseExpr, Expression.class).orElse(null));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
        return new SearchedCaseExpression(
            visit(context.whenClause(), WhenClause.class),
            visitIfPresent(context.elseExpr, Expression.class).orElse(null));
    }

    @Override
    public Node visitIfCase(SqlBaseParser.IfCaseContext context) {
        return new IfExpression(
            (Expression) visit(context.condition),
            (Expression) visit(context.trueValue),
            visitIfPresent(context.falseValue, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
        return new WhenClause((Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
        return new FunctionCall(
            getQualifiedName(context.qname()),
            isDistinct(context.setQuant()),
            visit(context.expr(), Expression.class));
    }

    // Literals

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
        return NullLiteral.INSTANCE;
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context) {
        return new StringLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
        return new LongLiteral(context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
        return new DoubleLiteral(context.getText());
    }

    @Override
    public Node visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext context) {
        return context.TRUE() != null ? BooleanLiteral.TRUE_LITERAL : BooleanLiteral.FALSE_LITERAL;
    }

    @Override
    public Node visitArrayLiteral(SqlBaseParser.ArrayLiteralContext context) {
        return new ArrayLiteral(visit(context.expr(), Expression.class));
    }

    @Override
    public Node visitDateLiteral(SqlBaseParser.DateLiteralContext context) {
        return new DateLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitTimeLiteral(SqlBaseParser.TimeLiteralContext context) {
        return new TimeLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitTimestampLiteral(SqlBaseParser.TimestampLiteralContext context) {
        return new TimestampLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitObjectLiteral(SqlBaseParser.ObjectLiteralContext context) {
        Multimap<String, Expression> objAttributes = LinkedListMultimap.create();
        context.objectKeyValue().forEach(attr ->
            objAttributes.put(getIdentText(attr.key), (Expression) visit(attr.value))
        );
        return new ObjectLiteral(objAttributes);
    }

    @Override
    public Node visitParameterPlaceholder(SqlBaseParser.ParameterPlaceholderContext context) {
        return new ParameterExpression(parameterPosition++);
    }

    @Override
    public Node visitPositionalParameter(SqlBaseParser.PositionalParameterContext context) {
        return new ParameterExpression(Integer.valueOf(context.integerLiteral().getText()));
    }

    @Override
    public Node visitOn(SqlBaseParser.OnContext context) {
        return BooleanLiteral.TRUE_LITERAL;
    }

    // Data types

    @Override
    public Node visitDataType(SqlBaseParser.DataTypeContext context) {
        if (context.objectTypeDefinition() != null) {
            return new ObjectColumnType(
                getObjectType(context.objectTypeDefinition().type),
                visit(context.objectTypeDefinition().columnDefinition(), ColumnDefinition.class));
        } else if (context.arrayTypeDefinition() != null) {
            return CollectionColumnType.array((ColumnType) visit(context.arrayTypeDefinition().dataType()));
        } else if (context.setTypeDefinition() != null) {
            return CollectionColumnType.set((ColumnType) visit(context.setTypeDefinition().dataType()));
        }
        return new ColumnType(context.getText().toLowerCase(Locale.ENGLISH));
    }

    private String getObjectType(Token type) {
        if (type == null) return null;
        switch (type.getType()) {
            case SqlBaseLexer.DYNAMIC:
                return type.getText().toLowerCase(Locale.ENGLISH);
            case SqlBaseLexer.STRICT:
                return type.getText().toLowerCase(Locale.ENGLISH);
            case SqlBaseLexer.IGNORED:
                return type.getText().toLowerCase(Locale.ENGLISH);
        }

        throw new UnsupportedOperationException("Unsupported object type: " + type.getText());
    }

    // Helpers

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
            .map(this::visit)
            .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
            .map(this::visit)
            .map(clazz::cast)
            .collect(toList());
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
            .replace("''", "'");
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QnameContext context) {
        return QualifiedName.of(identsToStrings(context.ident()));
    }

    private QualifiedName getQualifiedName(SqlBaseParser.IdentContext context) {
        return QualifiedName.of(getIdentText(context));
    }

    private List<String> identsToStrings(List<SqlBaseParser.IdentContext> idents) {
        return idents.stream()
            .map(this::getIdentText)
            .collect(toList());
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantContext setQuantifier) {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context) {
        return Optional.ofNullable(context).map(ParseTree::getText);
    }

    private List<String> getColumnAliases(SqlBaseParser.AliasedColumnsContext columnAliasesContext) {
        if (columnAliasesContext == null) {
            return null;
        }
        return identsToStrings(columnAliasesContext.ident());
    }

    private static ArithmeticExpression.Type getArithmeticBinaryOperator(Token operator) {
        switch (operator.getType()) {
            case SqlBaseLexer.PLUS:
                return ArithmeticExpression.Type.ADD;
            case SqlBaseLexer.MINUS:
                return ArithmeticExpression.Type.SUBTRACT;
            case SqlBaseLexer.ASTERISK:
                return ArithmeticExpression.Type.MULTIPLY;
            case SqlBaseLexer.SLASH:
                return ArithmeticExpression.Type.DIVIDE;
            case SqlBaseLexer.PERCENT:
                return ArithmeticExpression.Type.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Type getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpression.Type.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpression.Type.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpression.Type.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpression.Type.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
            case SqlBaseLexer.REGEX_MATCH:
                return ComparisonExpression.Type.REGEX_MATCH;
            case SqlBaseLexer.REGEX_NO_MATCH:
                return ComparisonExpression.Type.REGEX_NO_MATCH;
            case SqlBaseLexer.REGEX_MATCH_CI:
                return ComparisonExpression.Type.REGEX_MATCH_CI;
            case SqlBaseLexer.REGEX_NO_MATCH_CI:
                return ComparisonExpression.Type.REGEX_NO_MATCH_CI;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Type getDateTimeFunctionType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE:
                return CurrentTime.Type.DATE;
            case SqlBaseLexer.CURRENT_TIME:
                return CurrentTime.Type.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Type.TIMESTAMP;
        }

        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static ArrayComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.ALL:
                return ArrayComparisonExpression.Quantifier.ALL;
            case SqlBaseLexer.ANY:
                return ArrayComparisonExpression.Quantifier.ANY;
            case SqlBaseLexer.SOME:
                return ArrayComparisonExpression.Quantifier.ANY;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private static void validateFunctionName(QualifiedName functionName) {
        if (functionName.getParts().size() > 2) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The function name is not correct! " +
                "name [%s] does not conform the [[schema_name .] function_name] format.", functionName));
        }
    }
}
