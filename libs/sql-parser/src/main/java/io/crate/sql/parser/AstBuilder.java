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

package io.crate.sql.parser;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists2;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.antlr.SqlBaseLexer;
import io.crate.sql.parser.antlr.SqlBaseParser;
import io.crate.sql.parser.antlr.SqlBaseParser.BitStringContext;
import io.crate.sql.parser.antlr.SqlBaseParser.CloseContext;
import io.crate.sql.parser.antlr.SqlBaseParser.ColumnConstraintNullContext;
import io.crate.sql.parser.antlr.SqlBaseParser.ConflictTargetContext;
import io.crate.sql.parser.antlr.SqlBaseParser.DeclareContext;
import io.crate.sql.parser.antlr.SqlBaseParser.DeclareCursorParamsContext;
import io.crate.sql.parser.antlr.SqlBaseParser.DirectionContext;
import io.crate.sql.parser.antlr.SqlBaseParser.DiscardContext;
import io.crate.sql.parser.antlr.SqlBaseParser.FetchContext;
import io.crate.sql.parser.antlr.SqlBaseParser.IsolationLevelContext;
import io.crate.sql.parser.antlr.SqlBaseParser.QueryContext;
import io.crate.sql.parser.antlr.SqlBaseParser.QueryOptParensContext;
import io.crate.sql.parser.antlr.SqlBaseParser.SetTransactionContext;
import io.crate.sql.parser.antlr.SqlBaseParser.StatementsContext;
import io.crate.sql.parser.antlr.SqlBaseParser.TransactionModeContext;
import io.crate.sql.parser.antlr.SqlBaseParserBaseVisitor;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.AlterClusterRerouteRetryFailed;
import io.crate.sql.tree.AlterPublication;
import io.crate.sql.tree.AlterSubscription;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.AlterTableDropColumn;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.AnalyzeStatement;
import io.crate.sql.tree.AnalyzerElement;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparison.Quantifier;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.ArraySliceExpression;
import io.crate.sql.tree.ArraySubQueryExpression;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BitString;
import io.crate.sql.tree.BitwiseExpression;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.CharFilters;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.Close;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CommitStatement;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreatePublication;
import io.crate.sql.tree.CreateRepository;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.CreateSubscription;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateTableAs;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.Declare;
import io.crate.sql.tree.Declare.Hold;
import io.crate.sql.tree.DecommissionNodeStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DiscardStatement;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropCheckConstraint;
import io.crate.sql.tree.DropColumnDefinition;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropPublication;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropSubscription;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.Except;
import io.crate.sql.tree.ExistsPredicate;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.Fetch;
import io.crate.sql.tree.Fetch.ScrollMode;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.FunctionArgument;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.Intersect;
import io.crate.sql.tree.IntervalLiteral;
import io.crate.sql.tree.IsNotNullPredicate;
import io.crate.sql.tree.IsNullPredicate;
import io.crate.sql.tree.Join;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.JoinType;
import io.crate.sql.tree.JoinUsing;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.LikePredicate;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.LogicalBinaryExpression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.MatchPredicate;
import io.crate.sql.tree.MatchPredicateColumnIdent;
import io.crate.sql.tree.MultiStatement;
import io.crate.sql.tree.NamedProperties;
import io.crate.sql.tree.NaturalJoin;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NotExpression;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.NullColumnConstraint;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.PromoteReplica;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QueryBody;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.RecordSubscript;
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
import io.crate.sql.tree.SetSessionAuthorizationStatement;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.SetTransactionStatement;
import io.crate.sql.tree.SetTransactionStatement.TransactionMode;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.SimpleCaseExpression;
import io.crate.sql.tree.SingleColumn;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableElement;
import io.crate.sql.tree.TableFunction;
import io.crate.sql.tree.TableSubquery;
import io.crate.sql.tree.TokenFilters;
import io.crate.sql.tree.Tokenizer;
import io.crate.sql.tree.TrimMode;
import io.crate.sql.tree.TryCast;
import io.crate.sql.tree.Union;
import io.crate.sql.tree.Update;
import io.crate.sql.tree.Values;
import io.crate.sql.tree.ValuesList;
import io.crate.sql.tree.WhenClause;
import io.crate.sql.tree.Window;
import io.crate.sql.tree.WindowFrame;
import io.crate.sql.tree.With;
import io.crate.sql.tree.WithQuery;

class AstBuilder extends SqlBaseParserBaseVisitor<Node> {

    public static final String UNSUPPORTED_OP_STR = "Unsupported operator: ";
    private int parameterPosition = 1;
    private static final String CLUSTER = "CLUSTER";
    private static final String SCHEMA = "SCHEMA";
    private static final String TABLE = "TABLE";
    private static final String VIEW = "VIEW";

    @Nullable
    private final Function<String, Expression> parseStringLiteral;

    public AstBuilder(@Nullable Function<String, Expression> parseStringLiteral) {
        this.parseStringLiteral = parseStringLiteral;
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    @Override
    public Node visitStatements(StatementsContext ctx) {
        return new MultiStatement(visitCollection(ctx.statement(), Statement.class));
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
    public Node visitStartTransaction(SqlBaseParser.StartTransactionContext context) {
        return new BeginStatement();
    }

    @Override
    public Node visitAnalyze(SqlBaseParser.AnalyzeContext ctx) {
        return new AnalyzeStatement();
    }

    @Override
    public Node visitDeclare(DeclareContext ctx) {
        final Hold hold;
        if (ctx.HOLD() == null) {
            hold = Hold.WITHOUT;
        } else {
            hold = ctx.WITH() == null ? Hold.WITHOUT : Hold.WITH;
        }
        DeclareCursorParamsContext declareCursorParams = ctx.declareCursorParams();
        // SENSITIVITY is ignored. In CrateDB all cursors are insensitive

        Query query = (Query) ctx.queryNoWith().accept(this);
        return new Declare(
            getIdentText(ctx.ident()),
            hold,
            !declareCursorParams.BINARY().isEmpty(),
            declareCursorParams.NO().isEmpty() && !declareCursorParams.SCROLL().isEmpty(),
            query
        );
    }

    @Override
    public Node visitFetch(FetchContext ctx) {
        String cursorName = getIdentText(ctx.ident());
        DirectionContext direction = ctx.direction();

        ScrollMode scrollMode;
        long count = 1;
        // See ScrollMode description
        if (direction == null) {
            scrollMode = Fetch.ScrollMode.MOVE;
        } else if (direction.FIRST() != null) {
            scrollMode = Fetch.ScrollMode.ABSOLUTE;
            count = 1;
        } else if (direction.LAST() != null) {
            scrollMode = Fetch.ScrollMode.ABSOLUTE;
            count = -1;
        } else if (direction.ABSOLUTE() != null) {
            scrollMode = Fetch.ScrollMode.ABSOLUTE;
            count = Long.parseLong(direction.integerLiteral().getText());
        } else if (direction.RELATIVE() != null) {
            scrollMode = ScrollMode.RELATIVE;
            count = Long.parseLong(direction.integerLiteral().getText());
            if (direction.MINUS() != null) {
                count *= -1;
            }
        } else {
            scrollMode = Fetch.ScrollMode.MOVE;
            if (direction.ALL() != null) {
                count = Long.MAX_VALUE;
            } else if (direction.integerLiteral() != null) {
                count = Long.parseLong(direction.integerLiteral().getText());
            }
            if (direction.BACKWARD() != null) {
                count *= -1;
            }
        }
        return new Fetch(scrollMode, count, cursorName);
    }

    @Override
    public Node visitClose(CloseContext ctx) {
        if (ctx.ALL() == null) {
            return new Close(getIdentText(ctx.ident()));
        } else {
            return new Close(null);
        }
    }

    @Override
    public Node visitDiscard(DiscardContext ctx) {
        final DiscardStatement.Target target;
        if (ctx.ALL() != null) {
            target = DiscardStatement.Target.ALL;
        } else if (ctx.PLANS() != null) {
            target = DiscardStatement.Target.PLANS;
        } else if (ctx.SEQUENCES() != null) {
            target = DiscardStatement.Target.SEQUENCES;
        } else if (ctx.TEMP() != null || ctx.TEMPORARY() != null) {
            target = DiscardStatement.Target.TEMPORARY;
        } else {
            throw new IllegalStateException("Unexpected DiscardContext: " + ctx);
        }
        return new DiscardStatement(target);
    }

    @Override
    public Node visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext context) {
        IntervalLiteral.IntervalField startField = getIntervalFieldType((Token) context.from.getChild(0).getPayload());
        IntervalLiteral.IntervalField endField = null;
        if (context.to != null) {
            Token token = (Token) context.to.getChild(0).getPayload();
            endField = getIntervalFieldType(token);
        }

        if (endField != null && (startField.compareTo(endField) > 0)) {
            throw new IllegalArgumentException("Startfield must be less significant than Endfield");
        }

        IntervalLiteral.Sign sign = IntervalLiteral.Sign.PLUS;
        if (context.sign != null) {
            sign = getIntervalSign(context.sign);
        }

        return new IntervalLiteral(
            ((StringLiteral) visit(context.stringLiteral())).getValue(),
            sign,
            startField,
            endField);
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.MINUS -> IntervalLiteral.Sign.MINUS;
            case SqlBaseLexer.PLUS -> IntervalLiteral.Sign.PLUS;
            default -> throw new IllegalArgumentException("Unsupported sign: " + token.getText());
        };
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.YEAR -> IntervalLiteral.IntervalField.YEAR;
            case SqlBaseLexer.MONTH -> IntervalLiteral.IntervalField.MONTH;
            case SqlBaseLexer.DAY -> IntervalLiteral.IntervalField.DAY;
            case SqlBaseLexer.HOUR -> IntervalLiteral.IntervalField.HOUR;
            case SqlBaseLexer.MINUTE -> IntervalLiteral.IntervalField.MINUTE;
            case SqlBaseLexer.SECOND -> IntervalLiteral.IntervalField.SECOND;
            default -> throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
        };
    }

    @Override
    public Node visitCommit(SqlBaseParser.CommitContext context) {
        return new CommitStatement();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitOptimize(SqlBaseParser.OptimizeContext context) {
        return new OptimizeStatement(
            visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
            extractGenericProperties(context.withProperties()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
        boolean notExists = context.EXISTS() != null;
        SqlBaseParser.PartitionedByOrClusteredIntoContext tableOptsCtx = context.partitionedByOrClusteredInto();
        Optional<ClusteredBy> clusteredBy = visitIfPresent(tableOptsCtx.clusteredBy(), ClusteredBy.class);
        Optional<PartitionedBy> partitionedBy = visitIfPresent(tableOptsCtx.partitionedBy(), PartitionedBy.class);
        var tableElements = Lists2.map(context.tableElement(), x -> (TableElement<Expression>) visit(x));
        return new CreateTable(
            (Table<?>) visit(context.table()),
            tableElements,
            partitionedBy,
            clusteredBy,
            extractGenericProperties(context.withProperties()),
            notExists);
    }

    @Override
    public Node visitCreateTableAs(SqlBaseParser.CreateTableAsContext context) {
        return new CreateTableAs<>(
            (Table<?>) visit(context.table()),
            (Query) visit(context.insertSource().query())
        );
    }

    @Override
    public Node visitAlterClusterSwapTable(SqlBaseParser.AlterClusterSwapTableContext ctx) {
        return new SwapTable<>(
            getQualifiedName(ctx.source),
            getQualifiedName(ctx.target),
            extractGenericProperties(ctx.withProperties())
        );
    }

    @Override
    public Node visitAlterClusterGCDanglingArtifacts(SqlBaseParser.AlterClusterGCDanglingArtifactsContext ctx) {
        return GCDanglingArtifacts.INSTANCE;
    }

    @Override
    public Node visitAlterClusterDecommissionNode(SqlBaseParser.AlterClusterDecommissionNodeContext ctx) {
        return new DecommissionNodeStatement<>(visit(ctx.node));
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext ctx) {
        return new CreateView(
            getQualifiedName(ctx.qname()),
            (Query) visit(ctx.queryOptParens()),
            ctx.REPLACE() != null
        );
    }

    @Override
    public Node visitQueryOptParens(QueryOptParensContext ctx) {
        QueryContext query = ctx.query();
        if (query == null) {
            return ctx.queryOptParens().accept(this);
        }
        return query.accept(this);
    }

    @Override
    public Node visitDropView(SqlBaseParser.DropViewContext ctx) {
        return new DropView(getQualifiedNames(ctx.qnames()), ctx.EXISTS() != null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitCreateBlobTable(SqlBaseParser.CreateBlobTableContext context) {
        return new CreateBlobTable(
            (Table<?>) visit(context.table()),
            visitIfPresent(context.numShards, ClusteredBy.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitCreateRepository(SqlBaseParser.CreateRepositoryContext context) {
        return new CreateRepository<>(
            getIdentText(context.name),
            getIdentText(context.type),
            extractGenericProperties(context.withProperties()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCreateSnapshot(SqlBaseParser.CreateSnapshotContext context) {
        if (context.ALL() != null) {
            return new CreateSnapshot<>(
                getQualifiedName(context.qname()),
                extractGenericProperties(context.withProperties()));
        }
        return new CreateSnapshot(
            getQualifiedName(context.qname()),
            visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
            extractGenericProperties(context.withProperties()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCreateAnalyzer(SqlBaseParser.CreateAnalyzerContext context) {
        return new CreateAnalyzer(
            getIdentText(context.name),
            getIdentText(context.extendedName),
            visitCollection(context.analyzerElement(), AnalyzerElement.class)
        );
    }

    @Override
    public Node visitDropAnalyzer(SqlBaseParser.DropAnalyzerContext ctx) {
        return new DropAnalyzer(getIdentText(ctx.name));
    }

    @Override
    public Node visitCreateUser(SqlBaseParser.CreateUserContext context) {
        return new CreateUser<>(
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
        List<String> usernames = identsToStrings(context.users.ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges(
            context.ON() == null,
            context.clazz(),
            context.qnames());
        if (context.ALL() != null) {
            return new GrantPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.priviliges.ident());
            return new GrantPrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @Override
    public Node visitDenyPrivilege(SqlBaseParser.DenyPrivilegeContext context) {
        List<String> usernames = identsToStrings(context.users.ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges(
            context.ON() == null,
            context.clazz(),
            context.qnames());
        if (context.ALL() != null) {
            return new DenyPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.priviliges.ident());
            return new DenyPrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @Override
    public Node visitRevokePrivilege(SqlBaseParser.RevokePrivilegeContext context) {
        List<String> usernames = identsToStrings(context.users.ident());
        ClassAndIdent clazzAndIdent = getClassAndIdentsForPrivileges(
            context.ON() == null,
            context.clazz(),
            context.qnames());
        if (context.ALL() != null) {
            return new RevokePrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
        } else {
            List<String> privilegeTypes = identsToStrings(context.privileges.ident());
            return new RevokePrivilege(usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCharFilters(SqlBaseParser.CharFiltersContext context) {
        return new CharFilters(visitCollection(context.namedProperties(), NamedProperties.class));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitTokenFilters(SqlBaseParser.TokenFiltersContext context) {
        return new TokenFilters(visitCollection(context.namedProperties(), NamedProperties.class));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Node visitTokenizer(SqlBaseParser.TokenizerContext context) {
        return new Tokenizer<>((NamedProperties<Expression>) visit(context.namedProperties()));
    }

    @Override
    public Node visitNamedProperties(SqlBaseParser.NamedPropertiesContext context) {
        return new NamedProperties<>(
            getIdentText(context.ident()),
            extractGenericProperties(context.withProperties()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitRestore(SqlBaseParser.RestoreContext context) {
        if (context.ALL() != null) {
            return new RestoreSnapshot<>(
                getQualifiedName(context.qname()),
                RestoreSnapshot.Mode.ALL,
                extractGenericProperties(context.withProperties()));
        }
        if (context.METADATA() != null) {
            return new RestoreSnapshot<>(
                getQualifiedName(context.qname()),
                RestoreSnapshot.Mode.METADATA,
                extractGenericProperties(context.withProperties()));
        }
        if (context.TABLE() != null) {
            return new RestoreSnapshot(
                getQualifiedName(context.qname()),
                RestoreSnapshot.Mode.TABLE,
                extractGenericProperties(context.withProperties()),
                List.of(),
                visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class)
            );
        }
        return new RestoreSnapshot<>(
            getQualifiedName(context.qname()),
            RestoreSnapshot.Mode.CUSTOM,
            extractGenericProperties(context.withProperties()),
            identsToStrings(context.metatypes.ident())
        );
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context) {
        return new ShowCreateTable<>((Table<?>) visit(context.table()));
    }

    @Override
    public Node visitShowTransaction(SqlBaseParser.ShowTransactionContext context) {
        return new ShowTransaction();
    }

    @Override
    public Node visitShowSessionParameter(SqlBaseParser.ShowSessionParameterContext ctx) {
        if (ctx.ALL() != null) {
            return new ShowSessionParameter(null);
        } else {
            return new ShowSessionParameter(getQualifiedName(ctx.qname()));
        }
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context) {
        return new DropTable<>((Table<?>) visit(context.table()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropRepository(SqlBaseParser.DropRepositoryContext context) {
        return new DropRepository(getIdentText(context.ident()));
    }

    @Override
    public Node visitDropBlobTable(SqlBaseParser.DropBlobTableContext context) {
        return new DropBlobTable<>((Table<?>) visit(context.table()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropSnapshot(SqlBaseParser.DropSnapshotContext context) {
        return new DropSnapshot(getQualifiedName(context.qname()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCopyFrom(SqlBaseParser.CopyFromContext context) {
        boolean returnSummary = context.SUMMARY() != null;
        return new CopyFrom(
            (Table<?>) visit(context.tableWithPartition()),
            context.ident() == null ? emptyList() : identsToStrings(context.ident()),
            visit(context.path),
            extractGenericProperties(context.withProperties()),
            returnSummary);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCopyTo(SqlBaseParser.CopyToContext context) {
        return new CopyTo(
            (Table<?>) visit(context.tableWithPartition()),
            context.columns() == null ? emptyList() : visitCollection(context.columns().primaryExpression(), Expression.class),
            visitIfPresent(context.where(), Expression.class),
            context.DIRECTORY() != null,
            visit(context.path),
            extractGenericProperties(context.withProperties()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitInsert(SqlBaseParser.InsertContext context) throws IllegalArgumentException {
        List<String> columns = identsToStrings(context.ident());

        Table<?> table;
        Node node = visit(context.table());
        try {
            table = (Table<?>) node;
        } catch (ClassCastException e) {
            TableFunction tf = (TableFunction) node;
            for (Expression ex : tf.functionCall().getArguments()) {
                if (ex instanceof QualifiedNameReference ref && ref.getName().getParts().size() > 1) {
                    throw new IllegalArgumentException(
                        "Column references used in INSERT INTO <tbl> (...) must use the column name. " +
                        "They cannot qualify catalog, schema or table. Got `" + ref.getName().toString() + "`");
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                     "Invalid column reference %s used in INSERT INTO statement",
                                                                     ex.toString()));
                }
            }
            throw e;
        }
        return new Insert(
            table,
            (Query) visit(context.insertSource().query()),
            columns,
            getReturningItems(context.returning()),
            createDuplicateKeyContext(context)
        );
    }

    /**
     * Creates a {@link io.crate.sql.tree.Insert.DuplicateKeyContext} based on the Insert
     */
    @SuppressWarnings("unchecked")
    private Insert.DuplicateKeyContext<?> createDuplicateKeyContext(SqlBaseParser.InsertContext context) {
        if (context.onConflict() != null) {
            SqlBaseParser.OnConflictContext onConflictContext = context.onConflict();
            ConflictTargetContext conflictTarget = onConflictContext.conflictTarget();
            final List<Expression> conflictColumns;
            if (conflictTarget == null) {
                conflictColumns = List.of();
            } else {
                conflictColumns = visitCollection(conflictTarget.subscriptSafe(), Expression.class);
            }
            if (onConflictContext.NOTHING() != null) {
                return new Insert.DuplicateKeyContext<>(
                    Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_NOTHING,
                    emptyList(),
                    conflictColumns);
            } else {
                if (conflictColumns.isEmpty()) {
                    throw new IllegalStateException("ON CONFLICT <conflict_target> <- conflict_target missing");
                }
                var assignments = Lists2.map(onConflictContext.assignment(), x -> (Assignment<Expression>) visit(x));
                return new Insert.DuplicateKeyContext<>(
                    Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_UPDATE_SET,
                    assignments,
                    conflictColumns);
            }
        } else {
            return Insert.DuplicateKeyContext.none();
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

    @SuppressWarnings("unchecked")
    @Override
    public Node visitUpdate(SqlBaseParser.UpdateContext context) {
        var assignments = Lists2.map(context.assignment(), x -> (Assignment<Expression>) visit(x));
        return new Update(
            (Relation) visit(context.aliasedRelation()),
            assignments,
            visitIfPresent(context.where(), Expression.class),
            getReturningItems(context.returning())
            );
    }

    @Override
    public Node visitSet(SqlBaseParser.SetContext context) {
        Assignment<?> setAssignment = prepareSetAssignment(context);
        if (context.LOCAL() != null) {
            return new SetStatement<>(SetStatement.Scope.LOCAL, setAssignment);
        }
        return new SetStatement<>(SetStatement.Scope.SESSION, setAssignment);
    }

    private Assignment<?> prepareSetAssignment(SqlBaseParser.SetContext context) {
        Expression settingName = new QualifiedNameReference(getQualifiedName(context.qname()));
        if (context.DEFAULT() != null) {
            return new Assignment<>(settingName, List.of());
        }
        return new Assignment<>(settingName, visitCollection(context.setExpr(), Expression.class));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Node visitSetGlobal(SqlBaseParser.SetGlobalContext context) {
        var assignments = Lists2.map(context.setGlobalAssignment(), x -> (Assignment<Expression>) visit(x));
        if (context.PERSISTENT() != null) {
            return new SetStatement<>(SetStatement.Scope.GLOBAL,
                SetStatement.SettingType.PERSISTENT,
                assignments);
        }
        return new SetStatement<>(SetStatement.Scope.GLOBAL, assignments);
    }

    @Override
    public Node visitSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx) {
        Expression value;
        if (ctx.DEFAULT() != null) {
            value = new StringLiteral((ctx.DEFAULT().getText()));
        } else if (ctx.LOCAL() != null) {
            value = new StringLiteral((ctx.LOCAL().getText()));
        } else {
            value = (Expression) visit(ctx.stringLiteral());
        }
        Assignment<Expression> assignment = new Assignment<>(new StringLiteral("time zone"), value);
        return new SetStatement<>(SetStatement.Scope.TIME_ZONE, List.of(assignment));
    }

    @Override
    public Node visitSetTransaction(SetTransactionContext ctx) {
        List<TransactionMode> modes = Lists2.map(ctx.transactionMode(), AstBuilder::getTransactionMode);
        return new SetTransactionStatement(modes);
    }

    private static TransactionMode getTransactionMode(TransactionModeContext transactionModeCtx) {
        if (transactionModeCtx.ISOLATION() != null) {
            IsolationLevelContext isolationLevel = transactionModeCtx.isolationLevel();
            if (isolationLevel.COMMITTED() != null) {
                return SetTransactionStatement.IsolationLevel.READ_COMMITTED;
            } else if (isolationLevel.UNCOMMITTED() != null) {
                return SetTransactionStatement.IsolationLevel.READ_UNCOMMITTED;
            } else if (isolationLevel.REPEATABLE() != null) {
                return SetTransactionStatement.IsolationLevel.REPEATABLE_READ;
            } else {
                return SetTransactionStatement.IsolationLevel.SERIALIZABLE;
            }
        } else if (transactionModeCtx.READ() != null) {
            return SetTransactionStatement.ReadMode.READ_ONLY;
        } else if (transactionModeCtx.WRITE() != null) {
            return SetTransactionStatement.ReadMode.READ_WRITE;
        } else if (transactionModeCtx.NOT() != null) {
            return new SetTransactionStatement.Deferrable(true);
        } else if (transactionModeCtx.DEFERRABLE() != null) {
            return new SetTransactionStatement.Deferrable(false);
        } else {
            throw new IllegalStateException("Unexpected TransactionModeContext: " + transactionModeCtx);
        }
    }

    @Override
    public Node visitResetGlobal(SqlBaseParser.ResetGlobalContext context) {
        return new ResetStatement<>(visitCollection(context.primaryExpression(), Expression.class));
    }

    @Override
    public Node visitSetSessionAuthorization(SqlBaseParser.SetSessionAuthorizationContext context) {
        SetSessionAuthorizationStatement.Scope scope;
        if (context.LOCAL() != null) {
            scope = SetSessionAuthorizationStatement.Scope.LOCAL;
        } else {
            scope = SetSessionAuthorizationStatement.Scope.SESSION;
        }

        if (context.DEFAULT() != null) {
            return new SetSessionAuthorizationStatement(scope);
        } else {
            var userNameLiteral = visit(context.username);
            assert userNameLiteral instanceof StringLiteral
                : "username must be a StringLiteral because " +
                  "the parser grammar is restricted to string literals";
            var userName = ((StringLiteral) userNameLiteral).getValue();
            return new SetSessionAuthorizationStatement(userName, scope);
        }
    }

    @Override
    public Node visitResetSessionAuthorization(SqlBaseParser.ResetSessionAuthorizationContext ctx) {
        return new SetSessionAuthorizationStatement(SetSessionAuthorizationStatement.Scope.SESSION);
    }

    @Override
    public Node visitKill(SqlBaseParser.KillContext context) {
        return new KillStatement<>(
            visitIfPresent(context.jobId, Expression.class).orElse(null));
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
        if (context.ANALYZE() != null) {
            return new Explain((Statement) visit(context.statement()), true, Map.of(Explain.Option.ANALYZE, true));
        } else if (context.explainOptions() == null) {
            return new Explain((Statement) visit(context.statement()), false, Map.of(Explain.Option.COSTS, true));
        } else {
            var options = new LinkedHashMap<Explain.Option, Boolean>();
            for (SqlBaseParser.ExplainOptionsContext explainOptions : context.explainOptions()) {
                for (SqlBaseParser.ExplainOptionContext explainOptionContext : explainOptions.explainOption()) {
                    if (explainOptionContext.COSTS() != null) {
                        options.put(Explain.Option.COSTS, getBooleanOrNull(explainOptionContext));
                    } else if (explainOptionContext.ANALYZE() != null) {
                        options.put(Explain.Option.ANALYZE, getBooleanOrNull(explainOptionContext));
                    }
                }
            }
            return new Explain((Statement) visit(context.statement()), false, options);
        }
    }

    private Boolean getBooleanOrNull(SqlBaseParser.ExplainOptionContext context) {
        if (context.booleanLiteral() == null) {
            return null;
        } else {
            return ((BooleanLiteral) visitBooleanLiteral(context.booleanLiteral())).getValue();
        }
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitRefreshTable(SqlBaseParser.RefreshTableContext context) {
        return new RefreshStatement(visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class));
    }

    @Override
    public Node visitTableOnly(SqlBaseParser.TableOnlyContext context) {
        return new Table<>(getQualifiedName(context.qname()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitTableWithPartition(SqlBaseParser.TableWithPartitionContext context) {
        return new Table(getQualifiedName(context.qname()), visitCollection(context.assignment(), Assignment.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context) {
        QualifiedName functionName = getQualifiedName(context.name);
        validateFunctionName(functionName);
        return new CreateFunction(
            functionName,
            context.REPLACE() != null,
            visitCollection(context.functionArgument(), FunctionArgument.class),
            (ColumnType<?>) visit(context.returnType),
            visit(context.language),
            visit(context.body));
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

    @Override
    public Node visitCreatePublication(SqlBaseParser.CreatePublicationContext ctx) {
        List<QualifiedName> tables = ctx.qname().stream()
            .map(this::getQualifiedName)
            .toList();
        return new CreatePublication(getIdentText(ctx.name), ctx.ALL() != null, tables);
    }

    @Override
    public Node visitDropPublication(SqlBaseParser.DropPublicationContext ctx) {
        return new DropPublication(getIdentText(ctx.name), ctx.EXISTS() != null);
    }

    @Override
    public Node visitAlterPublication(SqlBaseParser.AlterPublicationContext ctx) {
        AlterPublication.Operation op;
        if (ctx.ADD() != null) {
            op = AlterPublication.Operation.ADD;
        } else if (ctx.SET() != null) {
            op = AlterPublication.Operation.SET;
        } else {
            op = AlterPublication.Operation.DROP;
        }
        List<QualifiedName> tables = ctx.qname().stream()
            .map(this::getQualifiedName)
            .toList();

        return new AlterPublication(getIdentText(ctx.name), op, tables);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitCreateSubscription(SqlBaseParser.CreateSubscriptionContext ctx) {
        return new CreateSubscription(
            getIdentText(ctx.name),
            visit(ctx.conninfo),
            identsToStrings(ctx.publications.ident()),
            extractGenericProperties(ctx.withProperties())
        );
    }

    @Override
    public Node visitDropSubscription(SqlBaseParser.DropSubscriptionContext ctx) {
        return new DropSubscription(getIdentText(ctx.name), ctx.EXISTS() != null);
    }

    @Override
    public Node visitAlterSubscription(SqlBaseParser.AlterSubscriptionContext ctx) {
        AlterSubscription.Mode mode = ctx.alterSubscriptionMode().ENABLE() != null
            ? AlterSubscription.Mode.ENABLE
            : AlterSubscription.Mode.DISABLE;

        return new AlterSubscription(getIdentText(ctx.name), mode);
    }

    @Override
    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext ctx) {
        return new WithQuery(
            getIdentText(ctx.ident()),
            (Query) visit(ctx.query()),
            getColumnAliases(ctx.aliasedColumns())
        );
    }

    @Override
    public Node visitWith(SqlBaseParser.WithContext ctx) {
        return new With(
            visitCollection(ctx.namedQuery(), WithQuery.class)
        );
    }

    // Column / Table definition

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context) {
        return new ColumnDefinition(
            getIdentText(context.ident()),
            visitOptionalContext(context.defaultExpr, Expression.class),
            visitOptionalContext(context.generatedExpr, Expression.class),
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
    }

    @Override
    public Node visitColumnConstraintPrimaryKey(SqlBaseParser.ColumnConstraintPrimaryKeyContext context) {
        return new PrimaryKeyColumnConstraint<>(getIdentText(context.primaryKeyContraint().name));
    }

    @Override
    public Node visitColumnConstraintNotNull(SqlBaseParser.ColumnConstraintNotNullContext context) {
        return new NotNullColumnConstraint<>();
    }

    @Override
    public Node visitColumnConstraintNull(ColumnConstraintNullContext ctx) {
        return new NullColumnConstraint<>();
    }

    @Override
    public Node visitPrimaryKeyConstraintTableLevel(SqlBaseParser.PrimaryKeyConstraintTableLevelContext ctx) {
        return new PrimaryKeyConstraint<>(
            getIdentText(ctx.primaryKeyContraint().name),
            visitCollection(ctx.columns().primaryExpression(), Expression.class));
    }

    @Override
    public Node visitTableCheckConstraint(SqlBaseParser.TableCheckConstraintContext context) {
        SqlBaseParser.CheckConstraintContext ctx = context.checkConstraint();
        String name = ctx.CONSTRAINT() != null ? getIdentText(ctx.name) : null;
        Expression expression = (Expression) visit(ctx.expression);
        String expressionStr = ExpressionFormatter.formatStandaloneExpression(expression);
        return new CheckConstraint<>(name, null, expression, expressionStr);
    }

    @Override
    public Node visitColumnCheckConstraint(SqlBaseParser.ColumnCheckConstraintContext context) {
        SqlBaseParser.CheckConstraintContext ctx = context.checkConstraint();
        String name = ctx.CONSTRAINT() != null ? getIdentText(ctx.name) : null;
        Expression expression = (Expression) visit(ctx.expression);
        String expressionStr = ExpressionFormatter.formatStandaloneExpression(expression);
        // column name is obtained during analysis
        return new CheckColumnConstraint<>(name, null, expression, expressionStr);
    }

    @Override
    public Node visitDropCheckConstraint(SqlBaseParser.DropCheckConstraintContext context) {
        Table<?> table = (Table<?>) visit(context.alterTableDefinition());
        return new DropCheckConstraint<>(table, getIdentText(context.ident()));
    }

    @Override
    public Node visitColumnIndexOff(SqlBaseParser.ColumnIndexOffContext context) {
        return IndexColumnConstraint.off();
    }

    @Override
    public Node visitColumnIndexConstraint(SqlBaseParser.ColumnIndexConstraintContext context) {
        return new IndexColumnConstraint<>(
            getIdentText(context.method),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitIndexDefinition(SqlBaseParser.IndexDefinitionContext context) {
        return new IndexDefinition<>(
            getIdentText(context.name),
            getIdentText(context.method),
            visitCollection(context.columns().primaryExpression(), Expression.class),
            extractGenericProperties(context.withProperties()));
    }

    @Override
    public Node visitColumnStorageDefinition(SqlBaseParser.ColumnStorageDefinitionContext ctx) {
        return new ColumnStorageDefinition<>(extractGenericProperties(ctx.withProperties()));
    }

    @Override
    public Node visitPartitionedBy(SqlBaseParser.PartitionedByContext context) {
        return new PartitionedBy<>(visitCollection(context.columns().primaryExpression(), Expression.class));
    }

    @Override
    public Node visitClusteredBy(SqlBaseParser.ClusteredByContext context) {
        return new ClusteredBy<>(
            visitIfPresent(context.routing, Expression.class),
            visitIfPresent(context.numShards, Expression.class));
    }

    @Override
    public Node visitBlobClusteredInto(SqlBaseParser.BlobClusteredIntoContext ctx) {
        return new ClusteredBy<>(Optional.empty(), visitIfPresent(ctx.numShards, Expression.class));
    }

    @Override
    public Node visitFunctionArgument(SqlBaseParser.FunctionArgumentContext context) {
        return new FunctionArgument(getIdentText(context.ident()), (ColumnType<?>) visit(context.dataType()));
    }

    @Override
    public Node visitRerouteMoveShard(SqlBaseParser.RerouteMoveShardContext context) {
        return new RerouteMoveShard<>(
            visit(context.shardId),
            visit(context.fromNodeId),
            visit(context.toNodeId));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitReroutePromoteReplica(SqlBaseParser.ReroutePromoteReplicaContext ctx) {
        return new PromoteReplica(
            visit(ctx.nodeId),
            visit(ctx.shardId),
            extractGenericProperties(ctx.withProperties()));
    }

    @Override
    public Node visitRerouteAllocateReplicaShard(SqlBaseParser.RerouteAllocateReplicaShardContext context) {
        return new RerouteAllocateReplicaShard<>(
            visit(context.shardId),
            visit(context.nodeId));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitRerouteCancelShard(SqlBaseParser.RerouteCancelShardContext context) {
        return new RerouteCancelShard(
            visit(context.shardId),
            visit(context.nodeId),
            extractGenericProperties(context.withProperties()));
    }

    // Properties

    @SuppressWarnings({"unchecked"})
    private GenericProperties<Expression> extractGenericProperties(ParserRuleContext context) {
        return visitIfPresent(context, GenericProperties.class).orElse(GenericProperties.empty());
    }

    @Override
    public Node visitWithGenericProperties(SqlBaseParser.WithGenericPropertiesContext context) {
        return visitGenericProperties(context.genericProperties());
    }

    @Override
    public Node visitGenericProperties(SqlBaseParser.GenericPropertiesContext context) {
        Map<String, Expression> properties = new HashMap<>();
        for (var property : context.genericProperty()) {
            String key = getIdentText(property.ident());
            Expression value = (Expression) visit(property.expr());
            properties.put(key, value);
        }
        return new GenericProperties<>(properties);
    }

    @Override
    public Node visitGenericProperty(SqlBaseParser.GenericPropertyContext context) {
        return new GenericProperty<>(getIdentText(context.ident()), (Expression) visit(context.expr()));
    }

    // Amending tables

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitAlterTableProperties(SqlBaseParser.AlterTablePropertiesContext context) {
        Table<?> name = (Table<?>) visit(context.alterTableDefinition());
        if (context.SET() != null) {
            return new AlterTable(name, extractGenericProperties(context.genericProperties()));
        }
        return new AlterTable(name, identsToStrings(context.ident()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitAlterBlobTableProperties(SqlBaseParser.AlterBlobTablePropertiesContext context) {
        Table<?> name = (Table<?>) visit(context.alterTableDefinition());
        if (context.SET() != null) {
            return new AlterBlobTable(name, extractGenericProperties(context.genericProperties()));
        }
        return new AlterBlobTable(name, identsToStrings(context.ident()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context) {
        var columnDefinitions = Lists2.map(context.addColumnDefinition(), x -> (TableElement<Expression>) visit(x));
        return new AlterTableAddColumn((Table<?>) visit(context.alterTableDefinition()), columnDefinitions);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitAddColumnDefinition(SqlBaseParser.AddColumnDefinitionContext context) {
        return new AddColumnDefinition(
            visit(context.subscriptSafe()),
            visitOptionalContext(context.expr(), Expression.class),
            visitOptionalContext(context.dataType(), ColumnType.class),
            visitCollection(context.columnConstraint(), ColumnConstraint.class));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Node visitDropColumn(SqlBaseParser.DropColumnContext ctx) {
        var columnDefinitions = Lists2.map(ctx.dropColumnDefinition(), x -> (TableElement<Expression>) visit(x));
        return new AlterTableDropColumn((Table<?>) visit(ctx.alterTableDefinition()), columnDefinitions);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitDropColumnDefinition(SqlBaseParser.DropColumnDefinitionContext ctx) {
        return new DropColumnDefinition(visit(ctx.subscriptSafe()), ctx.EXISTS() != null);
    }

    @Override
    public Node visitAlterTableOpenClose(SqlBaseParser.AlterTableOpenCloseContext context) {
        return new AlterTableOpenClose<>(
            (Table<?>) visit(context.alterTableDefinition()),
            context.BLOB() != null,
            context.OPEN() != null
        );
    }

    @Override
    public Node visitAlterTableRename(SqlBaseParser.AlterTableRenameContext context) {
        return new AlterTableRename<>(
            (Table<?>) visit(context.alterTableDefinition()),
            context.BLOB() != null,
            getQualifiedName(context.qname())
        );
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitAlterTableReroute(SqlBaseParser.AlterTableRerouteContext context) {
        return new AlterTableReroute<>(
            (Table<?>) visit(context.alterTableDefinition()),
            context.BLOB() != null,
            (RerouteOption) visit(context.rerouteOption()));
    }

    @Override
    public Node visitAlterClusterRerouteRetryFailed(SqlBaseParser.AlterClusterRerouteRetryFailedContext context) {
        return new AlterClusterRerouteRetryFailed();
    }

    @Override
    public Node visitAlterUser(SqlBaseParser.AlterUserContext context) {
        return new AlterUser<>(
            getIdentText(context.name),
            extractGenericProperties(context.genericProperties())
        );
    }

    @Override
    public Node visitSetGlobalAssignment(SqlBaseParser.SetGlobalAssignmentContext context) {
        return new Assignment<>(visit(context.primaryExpression()), visit(context.expr()));
    }

    @Override
    public Node visitAssignment(SqlBaseParser.AssignmentContext context) {
        Expression column = (Expression) visit(context.primaryExpression());
        // such as it is currently hard to restrict a left side of an assignment to subscript and
        // qname in the grammar, because of our current grammar structure which causes the
        // indirect left-side recursion when attempting to do so. We restrict it before initializing
        // an Assignment.
        if (column instanceof SubscriptExpression || column instanceof QualifiedNameReference) {
            return new Assignment<>(column, (Expression) visit(context.expr()));
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
            body.getOffset()
        );
    }

    @Override
    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context) {
        QueryBody term = (QueryBody) visit(context.queryTerm());
        if (term instanceof QuerySpecification query) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)

            return new Query(
                Optional.empty(),
                new QuerySpecification(
                    query.getSelect(),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    query.getWindows(),
                    visitCollection(context.sortItem(), SortItem.class),
                    visitIfPresent(context.limitClause(), Expression.class),
                    visitIfPresent(context.offsetClause(), Expression.class)),
                List.of(),
                Optional.empty(),
                Optional.empty());
        }
        return new Query(
            Optional.empty(),
            term,
            visitCollection(context.sortItem(), SortItem.class),
            visitIfPresent(context.limitClause(), Expression.class),
            visitIfPresent(context.offsetClause(), Expression.class));
    }

    @Override
    public Node visitLimitClause(SqlBaseParser.LimitClauseContext ctx) {
        return ctx.limit != null ? visit(ctx.limit) : null;
    }

    @Override
    public Node visitOffsetClause(SqlBaseParser.OffsetClauseContext ctx) {
        return ctx.offset != null ? visit(ctx.offset) : null;
    }

    @Override
    public Node visitIntegerParamOrLiteralDoubleColonCast(SqlBaseParser.IntegerParamOrLiteralDoubleColonCastContext ctx) {
        return new Cast((Expression) visit(ctx.parameterOrLiteral()), (ColumnType<?>) visit(ctx.dataType()), true);
    }

    @Override
    public Node visitIntegerParamOrLiteralCast(SqlBaseParser.IntegerParamOrLiteralCastContext ctx) {
        return generateCast(ctx.TRY_CAST() != null, ctx.expr(), ctx.dataType(), true);
    }

    @Override
    public Node visitDefaultQuerySpec(SqlBaseParser.DefaultQuerySpecContext context) {
        List<SelectItem> selectItems = visitCollection(context.selectItem(), SelectItem.class);
        return new QuerySpecification(
            new Select(isDistinct(context.setQuant()), selectItems),
            visitCollection(context.relation(), Relation.class),
            visitIfPresent(context.where(), Expression.class),
            visitCollection(context.expr(), Expression.class),
            visitIfPresent(context.having, Expression.class),
            getWindowDefinitions(context.windows),
            List.of(),
            Optional.empty(),
            Optional.empty()
        );
    }

    @Override
    public Node visitValuesRelation(SqlBaseParser.ValuesRelationContext ctx) {
        return new Values(visitCollection(ctx.values(), ValuesList.class));
    }

    private Map<String, Window> getWindowDefinitions(List<SqlBaseParser.NamedWindowContext> windowContexts) {
        HashMap<String, Window> windows = new HashMap<>(windowContexts.size());
        for (var windowContext : windowContexts) {
            var name = getIdentText(windowContext.name);
            if (windows.containsKey(name)) {
                throw new IllegalArgumentException("Window " + name + " is already defined");
            }
            Window window = (Window) visit(windowContext.windowDefinition());

            // It prevents circular references in the window definitions
            // by failing if the window was referenced before it was defined
            // E.g. WINDOW w AS (ww), ww AS (w)
            if (window.windowRef() != null && !windows.containsKey(window.windowRef())) {
                throw new IllegalArgumentException("Window " + window.windowRef() + " does not exist");
            }
            windows.put(name, window);
        }
        return windows;
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
                var firstIntersect = (QuerySpecification) visit(context.first);
                var secondIntersect = (QuerySpecification) visit(context.second);
                return new Intersect(firstIntersect, secondIntersect);
            case SqlBaseLexer.EXCEPT:
                var firstExcept = (QuerySpecification) visit(context.first);
                var secondExcept = (QuerySpecification) visit(context.second);
                return new Except(firstExcept, secondExcept);
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

    @Override
    public Node visitArraySubquery(SqlBaseParser.ArraySubqueryContext ctx) {
        return new ArraySubQueryExpression((SubqueryExpression) visit(ctx.subqueryExpression()));
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
        return new StringLiteral(context.getText().toLowerCase(Locale.ENGLISH));
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context) {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
            .replace("\"\"", "\"");
        return new StringLiteral(identifier);
    }

    @Nullable
    private String getIdentText(@Nullable SqlBaseParser.IdentContext ident) {
        if (ident != null) {
            StringLiteral literal = (StringLiteral) ident.accept(this);
            return literal.getValue();
        }
        return null;
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext ctx) {
        return new Table<>(getQualifiedName(ctx.qname()), false);
    }

    @Override
    public Node visitTableFunction(SqlBaseParser.TableFunctionContext ctx) {
        QualifiedName qualifiedName = getQualifiedName(ctx.qname());
        List<Expression> arguments = visitCollection(ctx.valueExpression(), Expression.class);
        return new TableFunction(new FunctionCall(qualifiedName, arguments));
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
            return new Join(JoinType.CROSS, left, right, Optional.empty());
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

    private static JoinType getJoinType(SqlBaseParser.JoinTypeContext joinTypeContext) {
        JoinType joinType;
        if (joinTypeContext.LEFT() != null) {
            joinType = JoinType.LEFT;
        } else if (joinTypeContext.RIGHT() != null) {
            joinType = JoinType.RIGHT;
        } else if (joinTypeContext.FULL() != null) {
            joinType = JoinType.FULL;
        } else {
            joinType = JoinType.INNER;
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

        boolean ignoreCase = context.LIKE() == null && context.ILIKE() != null;

        Expression result = new LikePredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.pattern),
            escape,
            ignoreCase);

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }
        return result;
    }

    @Override
    public Node visitArrayLike(SqlBaseParser.ArrayLikeContext context) {
        boolean inverse = context.NOT() != null;
        boolean ignoreCase = context.LIKE() == null && context.ILIKE() != null;
        return new ArrayLikePredicate(
            getComparisonQuantifier(((TerminalNode) context.setCmpQuantifier().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.v),
            visitOptionalContext(context.escape, Expression.class),
            inverse,
            ignoreCase);
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
            (Expression) visit(context.primaryExpression()));
    }

    @Override
    public Node visitMatch(SqlBaseParser.MatchContext context) {
        SqlBaseParser.MatchPredicateIdentsContext predicateIdents = context.matchPredicateIdents();
        List<MatchPredicateColumnIdent> idents;

        if (predicateIdents.matchPred != null) {
            idents = List.of((MatchPredicateColumnIdent) visit(predicateIdents.matchPred));
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
        return switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS -> new NegativeExpression((Expression) visit(context.valueExpression()));
            case SqlBaseLexer.PLUS -> visit(context.valueExpression());
            default -> throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        };
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
        return new ArithmeticExpression(
            getArithmeticBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitBitwiseBinary(SqlBaseParser.BitwiseBinaryContext context) {
        return new BitwiseExpression(
            getBitwiseOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
        return new FunctionCall(
            QualifiedName.of("concat"), List.of(
            (Expression) visit(context.left),
            (Expression) visit(context.right)));
    }

    @Override
    public Node visitOver(SqlBaseParser.OverContext context) {
        return visit(context.windowDefinition());
    }

    @Override
    public Node visitWindowDefinition(SqlBaseParser.WindowDefinitionContext context) {
        return new Window(
            getIdentText(context.windowRef),
            visitCollection(context.partition, Expression.class),
            visitCollection(context.sortItem(), SortItem.class),
            visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext ctx) {
        return new WindowFrame(
            getFrameType(ctx.frameType),
            (FrameBound) visit(ctx.start),
            visitIfPresent(ctx.end, FrameBound.class));
    }

    @Override
    public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context) {
        return new FrameBound(getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context) {
        return new FrameBound(getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expr()));
    }

    @Override
    public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context) {
        return new FrameBound(FrameBound.Type.CURRENT_ROW);
    }

    @Override
    public Node visitDoubleColonCast(SqlBaseParser.DoubleColonCastContext context) {
        return new Cast((Expression) visit(context.primaryExpression()), (ColumnType<?>) visit(context.dataType()));
    }

    @Override
    public Node visitFromStringLiteralCast(SqlBaseParser.FromStringLiteralCastContext context) {
        ColumnType<?> targetType = (ColumnType<?>) visit(context.dataType());

        if (targetType instanceof CollectionColumnType || targetType instanceof ObjectColumnType) {
            throw new UnsupportedOperationException("type 'string' cast notation only supports primitive types. " +
                                                    "Use '::' or cast() operator instead.");
        }
        return new Cast((Expression) visit(context.stringLiteral()), targetType);
    }

    // Primary expressions

    @Override
    public Node visitCast(SqlBaseParser.CastContext context) {
        return generateCast(context.TRY_CAST() != null, context.expr(), context.dataType(), false);
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
        return new FunctionCall(QualifiedName.of("substring"), visitCollection(context.expr(), Expression.class));
    }

    @Override
    public Node visitAtTimezone(SqlBaseParser.AtTimezoneContext context) {
        Expression zone = (Expression) visit(context.zone);
        Expression timestamp = (Expression) visit(context.timestamp);
        return new FunctionCall(QualifiedName.of("timezone"), List.of(zone, timestamp));
    }

    @Override
    public Node visitLeft(SqlBaseParser.LeftContext context) {
        Expression strOrColName = (Expression) visit(context.strOrColName);
        Expression len = (Expression) visit(context.len);
        return new FunctionCall(QualifiedName.of("left"), List.of(strOrColName, len));
    }

    @Override
    public Node visitRight(SqlBaseParser.RightContext context) {
        Expression strOrColName = (Expression) visit(context.strOrColName);
        Expression len = (Expression) visit(context.len);
        return new FunctionCall(QualifiedName.of("right"), List.of(strOrColName, len));
    }

    @Override
    public Node visitTrim(SqlBaseParser.TrimContext ctx) {
        Expression target = (Expression) visit(ctx.target);

        if (ctx.charsToTrim == null && ctx.trimMode == null) {
            return new FunctionCall(QualifiedName.of("trim"), List.of(target));
        }

        Expression charsToTrim = visitIfPresent(ctx.charsToTrim, Expression.class)
            .orElse(new StringLiteral(" "));
        StringLiteral trimMode = new StringLiteral(getTrimMode(ctx.trimMode).value());

        return new FunctionCall(
            QualifiedName.of("trim"),
            List.of(target, charsToTrim, trimMode)
        );
    }

    @Override
    public Node visitCurrentSchema(SqlBaseParser.CurrentSchemaContext context) {
        return new FunctionCall(QualifiedName.of("current_schema"), List.of());
    }

    @Override
    public Node visitCurrentUser(SqlBaseParser.CurrentUserContext ctx) {
        return new FunctionCall(QualifiedName.of("current_user"), List.of());
    }

    @Override
    public Node visitSessionUser(SqlBaseParser.SessionUserContext ctx) {
        return new FunctionCall(QualifiedName.of("session_user"), List.of());
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
    public Node visitArraySlice(SqlBaseParser.ArraySliceContext ctx) {
        return new ArraySliceExpression((Expression) visit(ctx.base),
                                        visitIfPresent(ctx.from, Expression.class),
                                        visitIfPresent(ctx.to, Expression.class));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context) {
        return new QualifiedNameReference(
            QualifiedName.of(identsToStrings(context.ident()))
        );
    }

    @Override
    public Node visitRecordSubscript(SqlBaseParser.RecordSubscriptContext ctx) {
        return new RecordSubscript(
            (Expression) visit(ctx.base),
            getIdentText(ctx.fieldName)
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
            (Expression) visit(context.operand),
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
    public Node visitFilter(SqlBaseParser.FilterContext context) {
        return visit(context.where());
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
        return new FunctionCall(
            getQualifiedName(context.qname()),
            isDistinct(context.setQuant()),
            visitCollection(context.expr(), Expression.class),
            visitIfPresent(context.over(), Window.class),
            visitIfPresent(context.filter(), Expression.class),
            context.IGNORE() == null && context.RESPECT() == null ? null : context.IGNORE() != null
        );
    }

    // Literals

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
        return NullLiteral.INSTANCE;
    }

    @Override
    public Node visitBitString(BitStringContext ctx) {
        String text = ctx.BIT_STRING().getText();
        return BitString.ofBitString(text);
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context) {
        if (context.STRING() != null) {
            var text = unquote(context.STRING().getText());
            if (parseStringLiteral != null) {
                try {
                    return parseStringLiteral.apply(text);
                } catch (Exception e) {
                    return new StringLiteral(text);
                }
            }
            return new StringLiteral(text);
        }
        return visitDollarQuotedStringLiteral(context.dollarQuotedStringLiteral());
    }

    @Override
    public Node visitDollarQuotedStringLiteral(SqlBaseParser.DollarQuotedStringLiteralContext ctx) {
        return new StringLiteral(ctx.DOLLAR_QUOTED_STRING_BODY().stream().map(ParseTree::getText).collect(Collectors.joining()));
    }

    @Override
    public Node visitEscapedCharsStringLiteral(SqlBaseParser.EscapedCharsStringLiteralContext ctx) {
        return new EscapedCharStringLiteral(unquoteEscaped(ctx.ESCAPED_STRING().getText()));
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
        String text = context.getText().replace("_", "");
        long value = Long.parseLong(text);
        if (value < Integer.MAX_VALUE + 1L) {
            return new IntegerLiteral((int) value);
        }
        return new LongLiteral(value);
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
        String text = context.getText().replace("_", "");
        return new DoubleLiteral(text);
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
    public Node visitEmptyArray(SqlBaseParser.EmptyArrayContext ctx) {
        return new ArrayLiteral(List.of());
    }

    @Override
    public Node visitObjectLiteral(SqlBaseParser.ObjectLiteralContext context) {
        LinkedHashMap<String, Expression> expressions = new LinkedHashMap<>();
        context.objectKeyValue().forEach(attr -> {
            var key = getIdentText(attr.key);
            var prevEntry = expressions.put(key, (Expression) visit(attr.value));
            if (prevEntry != null) {
                throw new IllegalArgumentException("Object literal cannot contain duplicate keys (`" + key + "`)");
            }
        });
        return new ObjectLiteral(expressions);
    }

    @Override
    public Node visitParameterPlaceholder(SqlBaseParser.ParameterPlaceholderContext context) {
        return new ParameterExpression(parameterPosition++);
    }

    @Override
    public Node visitPositionalParameter(SqlBaseParser.PositionalParameterContext context) {
        return new ParameterExpression(Integer.parseInt(context.integerLiteral().getText()));
    }

    @Override
    public Node visitOn(SqlBaseParser.OnContext context) {
        return BooleanLiteral.TRUE_LITERAL;
    }

    @Override
    public Node visitArrayDataType(SqlBaseParser.ArrayDataTypeContext ctx) {
        return new CollectionColumnType<>((ColumnType<?>) visit(ctx.dataType()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Node visitObjectTypeDefinition(SqlBaseParser.ObjectTypeDefinitionContext context) {
        return new ObjectColumnType(
            getObjectType(context.type),
            visitCollection(context.columnDefinition(), ColumnDefinition.class));
    }

    @Override
    public Node visitMaybeParametrizedDataType(SqlBaseParser.MaybeParametrizedDataTypeContext context) {
        StringLiteral name = (StringLiteral) visit(context.baseDataType());
        var parameters = new ArrayList<Integer>(context.integerLiteral().size());
        for (var param : context.integerLiteral()) {
            var literal = visit(param);
            int val;
            if (literal instanceof LongLiteral l) {
                val = Math.toIntExact(l.getValue());
            } else {
                val = ((IntegerLiteral) literal).getValue();
            }
            parameters.add(val);
        }
        return new ColumnType<>(name.getValue(), parameters);
    }

    @Override
    public Node visitIdentDataType(SqlBaseParser.IdentDataTypeContext context) {
        return Literal.fromObject(getIdentText(context.ident()));
    }

    @Override
    public Node visitDefinedDataType(SqlBaseParser.DefinedDataTypeContext context) {
        return Literal.fromObject(
            context.children.stream()
                .map(c -> c.getText().toLowerCase(Locale.ENGLISH))
                .collect(Collectors.joining(" "))
        );
    }

    @Nullable
    private static ColumnPolicy getObjectType(Token type) {
        if (type == null) {
            return null;
        }
        return switch (type.getType()) {
            case SqlBaseLexer.DYNAMIC -> ColumnPolicy.DYNAMIC;
            case SqlBaseLexer.STRICT -> ColumnPolicy.STRICT;
            case SqlBaseLexer.IGNORED -> ColumnPolicy.IGNORED;
            default -> throw new UnsupportedOperationException("Unsupported object type: " + type.getText());
        };
    }

    // Helpers

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
        if (context == null) {
            return Optional.empty();
        }
        Node node = context.accept(this);
        if (node == null) {
            return Optional.empty();
        }
        return Optional.of(clazz.cast(node));
    }

    private <T> List<T> visitCollection(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        ArrayList<T> result = new ArrayList<>(contexts.size());
        assert contexts instanceof RandomAccess : "Index access must be fast";
        for (int i = 0; i < contexts.size(); i++) {
            ParserRuleContext parserRuleContext = contexts.get(i);
            T item = clazz.cast(parserRuleContext.accept(this));
            result.add(item);
        }
        return result;
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
            .replace("''", "'");
    }

    private static String unquoteEscaped(String value) {
        // start from index: 2 to account for the 'E' literal
        // single quote escaping is handled at later stage
        // as we require more context on the surrounding characters
        return value.substring(2, value.length() - 1);
    }

    private List<SelectItem> getReturningItems(@Nullable SqlBaseParser.ReturningContext context) {
        return context == null ? List.of() : visitCollection(context.selectItem(),
                                                             SelectItem.class);
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QnameContext context) {
        return QualifiedName.of(identsToStrings(context.ident()));
    }

    private List<QualifiedName> getQualifiedNames(SqlBaseParser.QnamesContext context) {
        ArrayList<QualifiedName> names = new ArrayList<>(context.qname().size());
        for (SqlBaseParser.QnameContext qnameContext : context.qname()) {
            names.add(getQualifiedName(qnameContext));
        }
        return names;
    }

    private List<String> identsToStrings(List<SqlBaseParser.IdentContext> idents) {
        return Lists2.map(idents, this::getIdentText);
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
            return List.of();
        }
        return identsToStrings(columnAliasesContext.ident());
    }

    private static ArithmeticExpression.Type getArithmeticBinaryOperator(Token operator) {
        return switch (operator.getType()) {
            case SqlBaseLexer.PLUS -> ArithmeticExpression.Type.ADD;
            case SqlBaseLexer.MINUS -> ArithmeticExpression.Type.SUBTRACT;
            case SqlBaseLexer.ASTERISK -> ArithmeticExpression.Type.MULTIPLY;
            case SqlBaseLexer.SLASH -> ArithmeticExpression.Type.DIVIDE;
            case SqlBaseLexer.PERCENT -> ArithmeticExpression.Type.MODULUS;
            default -> throw new UnsupportedOperationException(UNSUPPORTED_OP_STR + operator.getText());
        };
    }

    private TrimMode getTrimMode(Token type) {
        if (type == null) {
            return TrimMode.BOTH;
        }
        return switch (type.getType()) {
            case SqlBaseLexer.BOTH -> TrimMode.BOTH;
            case SqlBaseLexer.LEADING -> TrimMode.LEADING;
            case SqlBaseLexer.TRAILING -> TrimMode.TRAILING;
            default -> throw new UnsupportedOperationException("Unsupported trim mode: " + type.getText());
        };
    }

    private static ComparisonExpression.Type getComparisonOperator(Token symbol) {
        return switch (symbol.getType()) {
            case SqlBaseLexer.EQ -> ComparisonExpression.Type.EQUAL;
            case SqlBaseLexer.NEQ -> ComparisonExpression.Type.NOT_EQUAL;
            case SqlBaseLexer.LT -> ComparisonExpression.Type.LESS_THAN;
            case SqlBaseLexer.LTE -> ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT -> ComparisonExpression.Type.GREATER_THAN;
            case SqlBaseLexer.GTE -> ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
            case SqlBaseLexer.LLT -> ComparisonExpression.Type.CONTAINED_WITHIN;
            case SqlBaseLexer.REGEX_MATCH -> ComparisonExpression.Type.REGEX_MATCH;
            case SqlBaseLexer.REGEX_NO_MATCH -> ComparisonExpression.Type.REGEX_NO_MATCH;
            case SqlBaseLexer.REGEX_MATCH_CI -> ComparisonExpression.Type.REGEX_MATCH_CI;
            case SqlBaseLexer.REGEX_NO_MATCH_CI -> ComparisonExpression.Type.REGEX_NO_MATCH_CI;
            default -> throw new UnsupportedOperationException(UNSUPPORTED_OP_STR + symbol.getText());
        };
    }

    private static CurrentTime.Type getDateTimeFunctionType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE -> CurrentTime.Type.DATE;
            case SqlBaseLexer.CURRENT_TIME -> CurrentTime.Type.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP -> CurrentTime.Type.TIMESTAMP;
            default -> throw new UnsupportedOperationException("Unsupported special function: " + token.getText());
        };
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.AND -> LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR -> LogicalBinaryExpression.Type.OR;
            default -> throw new IllegalArgumentException(UNSUPPORTED_OP_STR + token.getText());
        };
    }

    private static BitwiseExpression.Type getBitwiseOperator(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.BITWISE_AND -> BitwiseExpression.Type.AND;
            case SqlBaseLexer.BITWISE_OR -> BitwiseExpression.Type.OR;
            case SqlBaseLexer.BITWISE_XOR -> BitwiseExpression.Type.XOR;
            default -> throw new IllegalArgumentException(UNSUPPORTED_OP_STR + token.getText());
        };
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.FIRST -> SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST -> SortItem.NullOrdering.LAST;
            default -> throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
        };
    }

    private static SortItem.Ordering getOrderingType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.ASC -> SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC -> SortItem.Ordering.DESCENDING;
            default -> throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
        };
    }

    private static String getClazz(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.SCHEMA -> SCHEMA;
            case SqlBaseLexer.TABLE -> TABLE;
            case SqlBaseLexer.VIEW -> VIEW;
            default -> throw new IllegalArgumentException("Unsupported privilege class: " + token.getText());
        };
    }

    private static Quantifier getComparisonQuantifier(Token symbol) {
        return switch (symbol.getType()) {
            case SqlBaseLexer.ALL -> Quantifier.ALL;
            case SqlBaseLexer.ANY -> Quantifier.ANY;
            case SqlBaseLexer.SOME -> Quantifier.ANY;
            default -> throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
        };
    }

    private List<QualifiedName> getIdents(List<SqlBaseParser.QnameContext> qnames) {
        return qnames.stream().map(this::getQualifiedName).toList();
    }

    private static void validateFunctionName(QualifiedName functionName) {
        if (functionName.getParts().size() > 2) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "The function name is not correct! " +
                                                             "name [%s] does not conform the [[schema_name .] function_name] format.",
                                                             functionName));
        }
    }

    private ClassAndIdent getClassAndIdentsForPrivileges(boolean onCluster,
                                                         SqlBaseParser.ClazzContext clazz,
                                                         SqlBaseParser.QnamesContext qnamesContext) {
        if (onCluster) {
            return new ClassAndIdent(CLUSTER, emptyList());
        } else {
            return new ClassAndIdent(getClazz(clazz.getStart()), getIdents(qnamesContext.qname()));
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

    private static WindowFrame.Mode getFrameType(Token type) {
        return switch (type.getType()) {
            case SqlBaseLexer.RANGE -> WindowFrame.Mode.RANGE;
            case SqlBaseLexer.ROWS -> WindowFrame.Mode.ROWS;
            default -> throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
        };
    }

    private static FrameBound.Type getBoundedFrameBoundType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.PRECEDING -> FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING -> FrameBound.Type.FOLLOWING;
            default -> throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
        };
    }

    private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
        return switch (token.getType()) {
            case SqlBaseLexer.PRECEDING -> FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING -> FrameBound.Type.UNBOUNDED_FOLLOWING;
            default -> throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
        };
    }

    private Expression generateCast(boolean isTryCast,
                                    SqlBaseParser.ExprContext expr,
                                    SqlBaseParser.DataTypeContext datatype,
                                    boolean isIntegerOnly) {
        if (isTryCast) {
            return new TryCast((Expression) visit(expr), (ColumnType<?>) visit(datatype), isIntegerOnly);
        } else {
            return new Cast((Expression) visit(expr), (ColumnType<?>) visit(datatype), isIntegerOnly);
        }
    }
}
