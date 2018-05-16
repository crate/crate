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
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.AlterClusterRerouteRetryFailed;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.AnalyzerElement;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.CharFilters;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.CrateTableOption;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreateIngestRule;
import io.crate.sql.tree.CreateRepository;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DateLiteral;
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropIngestRule;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.Except;
import io.crate.sql.tree.ExistsPredicate;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.FunctionArgument;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.Intersect;
import io.crate.sql.tree.IsNotNullPredicate;
import io.crate.sql.tree.IsNullPredicate;
import io.crate.sql.tree.Join;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.JoinUsing;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.LikePredicate;
import io.crate.sql.tree.LogicalBinaryExpression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.MatchPredicate;
import io.crate.sql.tree.MatchPredicateColumnIdent;
import io.crate.sql.tree.NamedProperties;
import io.crate.sql.tree.NaturalJoin;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NotExpression;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QueryBody;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.Relation;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteCancelShard;
import io.crate.sql.tree.RerouteMoveShard;
import io.crate.sql.tree.RerouteOption;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.SearchedCaseExpression;
import io.crate.sql.tree.Select;
import io.crate.sql.tree.SelectItem;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.SimpleCaseExpression;
import io.crate.sql.tree.SingleColumn;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableElement;
import io.crate.sql.tree.TableFunction;
import io.crate.sql.tree.TableSubquery;
import io.crate.sql.tree.TimeLiteral;
import io.crate.sql.tree.TimestampLiteral;
import io.crate.sql.tree.TokenFilters;
import io.crate.sql.tree.Tokenizer;
import io.crate.sql.tree.TryCast;
import io.crate.sql.tree.Union;
import io.crate.sql.tree.Update;
import io.crate.sql.tree.ValuesList;
import io.crate.sql.tree.WhenClause;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

class AstBuilder extends SqlBaseBaseVisitor<Node> {

    private int parameterPosition = 1;
    private static final String CLUSTER = "CLUSTER";
    private static final String SCHEMA = "SCHEMA";
    private static final String TABLE = "TABLE";
    private static final String VIEW = "VIEW";


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
            visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
        boolean notExists = context.EXISTS() != null;
        return new CreateTable(
            (Table) visit(context.table()),
            visitCollection(context.tableElement(), TableElement.class),
            visitCollection(context.crateTableOption(), CrateTableOption.class),
            extractGenericProperties(context.withProperties()),
            notExists);
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext ctx) {
        return new CreateView(
            getQualifiedName(ctx.qname()),
            (Query) visit(ctx.query()),
            ctx.REPLACE() != null
        );
    }

    @Override
    public Node visitDropView(SqlBaseParser.DropViewContext ctx) {
        return new DropView(getQualifiedNames(ctx.qnames()), ctx.EXISTS() != null);
    }

    @Override
    public Node visitCreateBlobTable(SqlBaseParser.CreateBlobTableContext context) {
        return new CreateBlobTable(
            (Table) visit(context.table()),
            visitIfPresent(context.numShards, ClusteredBy.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitCreateRepository(SqlBaseParser.CreateRepositoryContext context) {
        return new CreateRepository(
            getIdentText(context.name),
            getIdentText(context.type),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitCreateSnapshot(SqlBaseParser.CreateSnapshotContext context) {
        if (context.ALL() != null) {
            return new CreateSnapshot(
                getQualifiedName(context.qname()),
                extractGenericProperties(context.withProperties()));
        }
        return new CreateSnapshot(
            getQualifiedName(context.qname()),
            visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitCreateAnalyzer(SqlBaseParser.CreateAnalyzerContext context) {
        return new CreateAnalyzer(
            getIdentText(context.name),
            getIdentText(context.extendedName),
            visitCollection(context.analyzerElement(), AnalyzerElement.class)
        );
    }

    @Override
    public Node visitCreateUser(SqlBaseParser.CreateUserContext context) {
        return new CreateUser(
            getIdentText(context.name),
            extractGenericProperties(context.withProperties()));
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
        List<String> usernames = identsToStrings(context.userNames().ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges((context.ON() == null), context.clazz(), context.qname());
        if (context.ALL() != null) {
            return new GrantPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new GrantPrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @Override
    public Node visitDenyPrivilege(SqlBaseParser.DenyPrivilegeContext context) {
        List<String> usernames = identsToStrings(context.userNames().ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges((context.ON() == null), context.clazz(), context.qname());
        if (context.ALL() != null) {
            return new DenyPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new DenyPrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @Override
    public Node visitRevokePrivilege(SqlBaseParser.RevokePrivilegeContext context) {
        List<String> usernames = identsToStrings(context.userNames().ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges((context.ON() == null), context.clazz(), context.qname());
        if (context.ALL() != null) {
            return new RevokePrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privilegeTypes().ident());
            return new RevokePrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @Override
    public Node visitCharFilters(SqlBaseParser.CharFiltersContext context) {
        return new CharFilters(visitCollection(context.namedProperties(), NamedProperties.class));
    }

    @Override
    public Node visitTokenFilters(SqlBaseParser.TokenFiltersContext context) {
        return new TokenFilters(visitCollection(context.namedProperties(), NamedProperties.class));
    }

    @Override
    public Node visitTokenizer(SqlBaseParser.TokenizerContext context) {
        return new Tokenizer((NamedProperties) visit(context.namedProperties()));
    }

    @Override
    public Node visitNamedProperties(SqlBaseParser.NamedPropertiesContext context) {
        return new NamedProperties(
            getIdentText(context.ident()),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitRestore(SqlBaseParser.RestoreContext context) {
        if (context.ALL() != null) {
            return new RestoreSnapshot(
                getQualifiedName(context.qname()),
                extractGenericProperties(context.withProperties()));
        }
        return new RestoreSnapshot(getQualifiedName(context.qname()),
            visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
            extractGenericProperties(context.withProperties()));
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
        boolean returnSummary = context.SUMMARY() != null;
        return new CopyFrom(
            (Table) visit(context.tableWithPartition()),
            (Expression) visit(context.path),
            extractGenericProperties(context.withProperties()),
            returnSummary);
    }

    @Override
    public Node visitCopyTo(SqlBaseParser.CopyToContext context) {
        return new CopyTo(
            (Table) visit(context.tableWithPartition()),
            context.columns() == null ? Collections.emptyList() : visitCollection(context.columns().primaryExpression(), Expression.class),
            visitIfPresent(context.where(), Expression.class),
            context.DIRECTORY() != null,
            (Expression) visit(context.path),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitInsert(SqlBaseParser.InsertContext context) {
        List<String> columns = identsToStrings(context.ident());

        Table table;
        try {
            table = (Table) visit(context.table());
        } catch (ClassCastException e) {
            TableFunction tf = (TableFunction) visit(context.table());
            for (Expression ex : tf.functionCall().getArguments()) {
                if (!(ex instanceof QualifiedNameReference)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "invalid table column reference %s", ex.toString()));
                }
            }
            throw e;
        }

        if (context.insertSource().VALUES() != null) {
            return new InsertFromValues(
                table,
                visitCollection(context.insertSource().values(), ValuesList.class),
                columns,
                createDuplicateKeyContext(context));
        }
        return new InsertFromSubquery(
            table,
            (Query) visit(context.insertSource().query()),
            columns,
            createDuplicateKeyContext(context));
    }

    /**
     * Creates a {@link io.crate.sql.tree.Insert.DuplicateKeyContext} based on the Insert
     */
    private Insert.DuplicateKeyContext createDuplicateKeyContext(SqlBaseParser.InsertContext context) {
        if (context.onDuplicate() != null) {
            return new Insert.DuplicateKeyContext(
                Insert.DuplicateKeyContext.Type.ON_DUPLICATE_KEY_UPDATE,
                visitCollection(context.onDuplicate().assignment(), Assignment.class),
                Collections.emptyList());
        } else if (context.onConflict() != null) {
            SqlBaseParser.OnConflictContext onConflictContext = context.onConflict();
            final List<String> conflictColumns;
            if (onConflictContext.conflictTarget() != null) {
                conflictColumns = onConflictContext.conflictTarget().qname().stream()
                    .map(RuleContext::getText)
                    .collect(toList());
            } else {
                conflictColumns = Collections.emptyList();
            }
            if (onConflictContext.NOTHING() != null) {
                return new Insert.DuplicateKeyContext(
                    Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_NOTHING,
                    Collections.emptyList(),
                    conflictColumns);
            } else {
                if (conflictColumns == null) {
                    throw new IllegalStateException("ON CONFLICT <conflict_target> <- conflict_target missing");
                }
                return new Insert.DuplicateKeyContext(
                    Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_UPDATE_SET,
                    visitCollection(onConflictContext.assignment(), Assignment.class),
                    conflictColumns);
            }
        } else {
            return Insert.DuplicateKeyContext.NONE;
        }
    }

    @Override
    public Node visitValues(SqlBaseParser.ValuesContext context) {
        return new ValuesList(visitCollection(context.expr(), Expression.class));
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
            visitCollection(context.assignment(), Assignment.class),
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
        return new Assignment(settingName, visitCollection(context.setExpr(), Expression.class));
    }

    @Override
    public Node visitSetGlobal(SqlBaseParser.SetGlobalContext context) {
        if (context.PERSISTENT() != null) {
            return new SetStatement(SetStatement.Scope.GLOBAL,
                SetStatement.SettingType.PERSISTENT,
                visitCollection(context.setGlobalAssignment(), Assignment.class));
        }
        return new SetStatement(SetStatement.Scope.GLOBAL, visitCollection(context.setGlobalAssignment(), Assignment.class));
    }

    @Override
    public Node visitSetSessionTransactionMode(SqlBaseParser.SetSessionTransactionModeContext ctx) {
        Assignment assignment = new Assignment(
            new StringLiteral("transaction_mode"),
            visitCollection(ctx.setExpr(), Expression.class));
        return new SetStatement(SetStatement.Scope.SESSION_TRANSACTION_MODE, assignment);
    }

    @Override
    public Node visitResetGlobal(SqlBaseParser.ResetGlobalContext context) {
        return new ResetStatement(visitCollection(context.primaryExpression(), Expression.class));
    }

    @Override
    public Node visitKill(SqlBaseParser.KillContext context) {
        if (context.ALL() != null) {
            return new KillStatement();
        }
        return new KillStatement((Expression) visit(context.jobId));
    }

    @Override
    public Node visitDeallocate(SqlBaseParser.DeallocateContext context) {
        if (context.ALL() != null) {
            return new DeallocateStatement();
        }
        return new DeallocateStatement((Expression) visit(context.prepStmt));
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context) {
        return new Explain((Statement) visit(context.statement()), context.ANALYZE() != null);
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context) {
        return new ShowTables(
            context.qname() == null ? null : getQualifiedName(context.qname()),
            getUnquotedText(context.pattern),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context) {
        return new ShowSchemas(
            getUnquotedText(context.pattern),
            visitIfPresent(context.where(), Expression.class));
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
        return new ShowColumns(
            getQualifiedName(context.tableName),
            context.schema == null ? null : getQualifiedName(context.schema),
            visitIfPresent(context.where(), Expression.class),
            getUnquotedText(context.pattern));
    }

    @Override
    public Node visitRefreshTable(SqlBaseParser.RefreshTableContext context) {
        return new RefreshStatement(visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class));
    }

    @Override
    public Node visitTableOnly(SqlBaseParser.TableOnlyContext context) {
        return new Table(getQualifiedName(context.qname()));
    }

    @Override
    public Node visitTableWithPartition(SqlBaseParser.TableWithPartitionContext context) {
        return new Table(getQualifiedName(context.qname()), visitCollection(context.assignment(), Assignment.class));
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context) {
        QualifiedName functionName = getQualifiedName(context.name);
        validateFunctionName(functionName);
        return new CreateFunction(
            functionName,
            context.REPLACE() != null,
            visitCollection(context.functionArgument(), FunctionArgument.class),
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
            visitCollection(context.functionArgument(), FunctionArgument.class));
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
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitGeneratedColumnDefinition(SqlBaseParser.GeneratedColumnDefinitionContext context) {
        return new ColumnDefinition(
            getIdentText(context.ident()),
            visitOptionalContext(context.generatedExpr, Expression.class),
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
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
        return new PrimaryKeyConstraint(visitCollection(context.columns().primaryExpression(), Expression.class));
    }

    @Override
    public Node visitColumnIndexOff(SqlBaseParser.ColumnIndexOffContext context) {
        return IndexColumnConstraint.OFF;
    }

    @Override
    public Node visitColumnIndexConstraint(SqlBaseParser.ColumnIndexConstraintContext context) {
        return new IndexColumnConstraint(
            getIdentText(context.method),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitIndexDefinition(SqlBaseParser.IndexDefinitionContext context) {
        return new IndexDefinition(
            getIdentText(context.name),
            getIdentText(context.method),
            visitCollection(context.columns().primaryExpression(), Expression.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitColumnStorageDefinition(SqlBaseParser.ColumnStorageDefinitionContext ctx) {
        return new ColumnStorageDefinition(extractGenericProperties(ctx.withProperties()));
    }

    @Override
    public Node visitPartitionedBy(SqlBaseParser.PartitionedByContext context) {
        return new PartitionedBy(visitCollection(context.columns().primaryExpression(), Expression.class));
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
        return new FunctionArgument(getIdentText(context.ident()), (ColumnType) visit(context.dataType()));
    }

    @Override
    public Node visitRerouteMoveShard(SqlBaseParser.RerouteMoveShardContext context) {
        return new RerouteMoveShard(
            (Expression) visit(context.shardId),
            (Expression) visit(context.fromNodeId),
            (Expression) visit(context.toNodeId));
    }

    @Override
    public Node visitRerouteAllocateReplicaShard(SqlBaseParser.RerouteAllocateReplicaShardContext context) {
        return new RerouteAllocateReplicaShard(
            (Expression) visit(context.shardId),
            (Expression) visit(context.nodeId));
    }

    @Override
    public Node visitRerouteCancelShard(SqlBaseParser.RerouteCancelShardContext context) {
        return new RerouteCancelShard(
            (Expression) visit(context.shardId),
            (Expression) visit(context.nodeId),
            extractGenericProperties(context.withProperties()));
    }

    // Properties

    private GenericProperties extractGenericProperties(ParserRuleContext context) {
        return visitIfPresent(context, GenericProperties.class).orElse(GenericProperties.EMPTY);
    }

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
            return new AlterTable(name, extractGenericProperties(context.genericProperties()));
        }
        return new AlterTable(name, identsToStrings(context.ident()));
    }

    @Override
    public Node visitAlterBlobTableProperties(SqlBaseParser.AlterBlobTablePropertiesContext context) {
        Table name = (Table) visit(context.alterTableDefinition());
        if (context.SET() != null) {
            return new AlterBlobTable(name, extractGenericProperties(context.genericProperties()));
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
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitAddGeneratedColumnDefinition(SqlBaseParser.AddGeneratedColumnDefinitionContext context) {
        return new AddColumnDefinition(
            (Expression) visit(context.subscriptSafe()),
            visitOptionalContext(context.generatedExpr, Expression.class),
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
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
    public Node visitAlterTableReroute(SqlBaseParser.AlterTableRerouteContext context) {
        return new AlterTableReroute(
            (Table) visit(context.alterTableDefinition()),
            (RerouteOption) visit(context.rerouteOption()));
    }

    @Override
    public Node visitAlterClusterRerouteRetryFailed(SqlBaseParser.AlterClusterRerouteRetryFailedContext context) {
        return new AlterClusterRerouteRetryFailed();
    }

    @Override
    public Node visitAlterUser(SqlBaseParser.AlterUserContext context) {
        return new AlterUser(
            getIdentText(context.name),
            extractGenericProperties(context.genericProperties())
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
        QueryBody term = (QueryBody) visit(context.queryTerm());
        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                new QuerySpecification(
                    query.getSelect(),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    visitCollection(context.sortItem(), SortItem.class),
                    visitIfPresent(context.limit, Expression.class),
                    visitIfPresent(context.offset, Expression.class)),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty());
        }
        return new Query(
            term,
            visitCollection(context.sortItem(), SortItem.class),
            visitIfPresent(context.limit, Expression.class),
            visitIfPresent(context.offset, Expression.class));
    }

    @Override
    public Node visitQuerySpec(SqlBaseParser.QuerySpecContext context) {
        List<SelectItem> selectItems = visitCollection(context.selectItem(), SelectItem.class);
        List<Relation> relations = context.FROM() == null ? ImmutableList.of() : visitCollection(context.relation(), Relation.class);

        return new QuerySpecification(
            new Select(isDistinct(context.setQuant()), selectItems),
            relations,
            visitIfPresent(context.where(), Expression.class),
            visitCollection(context.expr(), Expression.class),
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
        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                QueryBody left = (QueryBody) visit(context.left);
                QueryBody right = (QueryBody) visit(context.right);
                boolean isDistinct = context.setQuant() == null || context.setQuant().ALL() == null;
                return new Union(left, right, isDistinct);
            case SqlBaseLexer.INTERSECT:
                QuerySpecification first = (QuerySpecification) visit(context.first);
                QuerySpecification second = (QuerySpecification) visit(context.second);
                return new Intersect(first, second);
            case SqlBaseLexer.EXCEPT:
                first = (QuerySpecification) visit(context.first);
                second = (QuerySpecification) visit(context.second);
                return new Except(first, second);
            default:
                throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
        }
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
        return new SingleColumn((Expression) visit(context.expr()), getIdentText(context.ident()));
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

    @Nullable
    private String getIdentText(@Nullable SqlBaseParser.IdentContext ident) {
        if (ident != null) {
            StringLiteral literal = (StringLiteral) visit(ident);
            return literal.getValue();
        }
        return null;
    }

    @Override
    public Node visitTable(SqlBaseParser.TableContext context) {
        if (context.qname() != null) {
            return new Table(getQualifiedName(context.qname()), visitCollection(context.valueExpression(), Assignment.class));
        }
        FunctionCall fc = new FunctionCall(
            getQualifiedName(context.ident()), visitCollection(context.valueExpression(), Expression.class));
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
            visitOptionalContext(context.escape, Expression.class),
            inverse);
    }

    @Override
    public Node visitInList(SqlBaseParser.InListContext context) {
        Expression result = new InPredicate(
            (Expression) visit(context.value),
            new InListExpression(visitCollection(context.expr(), Expression.class)));

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
            idents = visitCollection(predicateIdents.matchPredicateIdent(), MatchPredicateColumnIdent.class);
        }
        return new MatchPredicate(
            idents,
            (Expression) visit(context.term),
            getIdentText(context.matchType),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitMatchPredicateIdent(SqlBaseParser.MatchPredicateIdentContext context) {
        return new MatchPredicateColumnIdent(
            (Expression) visit(context.subscriptSafe()),
            visitOptionalContext(context.boost, Expression.class));
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
        return new Extract((Expression) visit(context.expr()),
            (StringLiteral) visit(context.stringLiteralOrIdentifier()));
    }

    @Override
    public Node visitSubstring(SqlBaseParser.SubstringContext context) {
        return new FunctionCall(QualifiedName.of("substr"), visitCollection(context.expr(), Expression.class));
    }

    @Override
    public Node visitCurrentSchema(SqlBaseParser.CurrentSchemaContext context) {
        return new FunctionCall(QualifiedName.of("current_schema"), ImmutableList.of());
    }

    @Override
    public Node visitCurrentUser(SqlBaseParser.CurrentUserContext ctx) {
        return new FunctionCall(QualifiedName.of("current_user"), ImmutableList.of());
    }

    @Override
    public Node visitSessionUser(SqlBaseParser.SessionUserContext ctx) {
        return new FunctionCall(QualifiedName.of("session_user"), ImmutableList.of());
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
            visitCollection(context.whenClause(), WhenClause.class),
            visitOptionalContext(context.elseExpr, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
        return new SearchedCaseExpression(
            visitCollection(context.whenClause(), WhenClause.class),
            visitOptionalContext(context.elseExpr, Expression.class));
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
            visitCollection(context.expr(), Expression.class));
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
        return new ArrayLiteral(visitCollection(context.expr(), Expression.class));
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

    @Override
    public Node visitDropIngestRule(SqlBaseParser.DropIngestRuleContext ctx) {
        return new DropIngestRule(getIdentText(ctx.rule_name), ctx.EXISTS() != null);
    }

    @Override
    public Node visitCreateIngestRule(SqlBaseParser.CreateIngestRuleContext ctx) {
        return new CreateIngestRule(getIdentText(ctx.rule_name),
            getIdentText(ctx.source_ident),
            getQualifiedName(ctx.table_ident),
            visitIfPresent(ctx.where(), Expression.class));
    }

    // Data types

    @Override
    public Node visitDataType(SqlBaseParser.DataTypeContext context) {
        if (context.objectTypeDefinition() != null) {
            return new ObjectColumnType(
                getObjectType(context.objectTypeDefinition().type),
                visitCollection(context.objectTypeDefinition().columnDefinition(), ColumnDefinition.class));
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
            default:
                throw new UnsupportedOperationException("Unsupported object type: " + type.getText());
        }
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

    @Nullable
    private <T> T visitOptionalContext(@Nullable ParserRuleContext context, Class<T> clazz) {
        if (context != null) {
            return clazz.cast(visit(context));
        }
        return null;
    }

    private <T> Optional<T> visitIfPresent(@Nullable ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
            .map(this::visit)
            .map(clazz::cast);
    }

    private <T> List<T> visitCollection(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
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

    private List<QualifiedName> getQualifiedNames(SqlBaseParser.QnamesContext context) {
        ArrayList<QualifiedName> names = new ArrayList<>(context.qname().size());
        for (SqlBaseParser.QnameContext qnameContext : context.qname()) {
            names.add(getQualifiedName(qnameContext));
        }
        return names;
    }

    private List<String> identsToStrings(List<SqlBaseParser.IdentContext> idents) {
        return idents.stream()
            .map(this::getIdentText)
            .collect(toList());
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantContext setQuantifier) {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    @Nullable
    private static String getUnquotedText(@Nullable ParserRuleContext context) {
        return context != null ? unquote(context.getText()) : null;
    }

    private List<String> getColumnAliases(SqlBaseParser.AliasedColumnsContext columnAliasesContext) {
        if (columnAliasesContext == null) {
            return ImmutableList.of();
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
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
        }
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
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + symbol.getText());
        }
    }

    private static CurrentTime.Type getDateTimeFunctionType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE:
                return CurrentTime.Type.DATE;
            case SqlBaseLexer.CURRENT_TIME:
                return CurrentTime.Type.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Type.TIMESTAMP;
            default:
                throw new UnsupportedOperationException("Unsupported special function: " + token.getText());
        }
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + token.getText());
        }
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
            default:
                throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
        }
    }

    private static SortItem.Ordering getOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
            default:
                throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
        }
    }

    private static String getClazz(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.SCHEMA:
                return SCHEMA;
            case SqlBaseLexer.TABLE:
                return TABLE;
            case SqlBaseLexer.VIEW:
                return VIEW;
            default:
                throw new IllegalArgumentException("Unsupported privilege class: " + token.getText());

        }
    }

    private static ArrayComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.ALL:
                return ArrayComparisonExpression.Quantifier.ALL;
            case SqlBaseLexer.ANY:
                return ArrayComparisonExpression.Quantifier.ANY;
            case SqlBaseLexer.SOME:
                return ArrayComparisonExpression.Quantifier.ANY;
            default:
                throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
        }
    }

    private List<QualifiedName> getIdents(List<SqlBaseParser.QnameContext> qnames) {
        return qnames.stream().map(this::getQualifiedName).collect(toList());
    }

    private static void validateFunctionName(QualifiedName functionName) {
        if (functionName.getParts().size() > 2) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The function name is not correct! " +
                "name [%s] does not conform the [[schema_name .] function_name] format.", functionName));
        }
    }

    private ClassAndIdent getClassAndIdentsForPrivileges(boolean onCluster,
                                                         SqlBaseParser.ClazzContext clazz,
                                                         List<SqlBaseParser.QnameContext> qname) {
        if (onCluster) {
            return new ClassAndIdent(CLUSTER, Collections.emptyList());
        } else {
            return new ClassAndIdent(getClazz(clazz.getStart()), getIdents(qname));
        }
    }

    private static class ClassAndIdent {
        private final String clazz;
        private final List<QualifiedName> idents;

        ClassAndIdent(String clazz, List<QualifiedName> idents) {
            this.clazz = clazz;
            this.idents = idents;
        }
    }
}
