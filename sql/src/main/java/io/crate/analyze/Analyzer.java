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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.auth.user.UserManager;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationsUnknown;
import io.crate.execution.ddl.RepositoryService;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.AlterClusterRerouteRetryFailed;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.CommitStatement;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreateRepository;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.DecommissionNodeStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Update;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.util.ArrayList;
import java.util.Locale;

@Singleton
public class Analyzer {

    private final AnalyzerDispatcher dispatcher = new AnalyzerDispatcher();

    private final RelationAnalyzer relationAnalyzer;
    private final DropTableAnalyzer dropTableAnalyzer;
    private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
    private final ShowCreateTableAnalyzer showCreateTableAnalyzer;
    private final ExplainStatementAnalyzer explainStatementAnalyzer;
    private final ShowStatementAnalyzer showStatementAnalyzer;
    private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
    private final CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer;
    private final DropAnalyzerStatementAnalyzer dropAnalyzerStatementAnalyzer;
    private final UserManager userManager;
    private final RefreshTableAnalyzer refreshTableAnalyzer;
    private final OptimizeTableAnalyzer optimizeTableAnalyzer;
    private final AlterTableAnalyzer alterTableAnalyzer;
    private final AlterBlobTableAnalyzer alterBlobTableAnalyzer;
    private final AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer;
    private final AlterTableOpenCloseAnalyzer alterTableOpenCloseAnalyzer;
    private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
    private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
    private final CopyAnalyzer copyAnalyzer;
    private final UpdateAnalyzer updateAnalyzer;
    private final DeleteAnalyzer deleteAnalyzer;
    private final DropRepositoryAnalyzer dropRepositoryAnalyzer;
    private final CreateRepositoryAnalyzer createRepositoryAnalyzer;
    private final DropSnapshotAnalyzer dropSnapshotAnalyzer;
    private final CreateSnapshotAnalyzer createSnapshotAnalyzer;
    private final RestoreSnapshotAnalyzer restoreSnapshotAnalyzer;
    private final UnboundAnalyzer unboundAnalyzer;
    private final CreateFunctionAnalyzer createFunctionAnalyzer;
    private final DropFunctionAnalyzer dropFunctionAnalyzer;
    private final PrivilegesAnalyzer privilegesAnalyzer;
    private final AlterTableRerouteAnalyzer alterTableRerouteAnalyzer;
    private final CreateUserAnalyzer createUserAnalyzer;
    private final AlterUserAnalyzer alterUserAnalyzer;
    private final CreateViewAnalyzer createViewAnalyzer;
    private final Schemas schemas;
    private final SwapTableAnalyzer swapTableAnalyzer;
    private final DecommissionNodeAnalyzer decommissionNodeAnalyzer;

    /**
     * @param relationAnalyzer is injected because we also need to inject it in
     *                         {@link io.crate.metadata.view.InternalViewInfoFactory} and we want to keep only a single
     *                         instance of the class
     */
    @Inject
    public Analyzer(Schemas schemas,
                    Functions functions,
                    RelationAnalyzer relationAnalyzer,
                    ClusterService clusterService,
                    AnalysisRegistry analysisRegistry,
                    RepositoryService repositoryService,
                    RepositoryParamValidator repositoryParamValidator,
                    UserManager userManager) {
        this.relationAnalyzer = relationAnalyzer;
        this.schemas = schemas;
        this.dropTableAnalyzer = new DropTableAnalyzer(schemas);
        this.userManager = userManager;
        this.createTableStatementAnalyzer = new CreateTableStatementAnalyzer(functions);
        this.swapTableAnalyzer = new SwapTableAnalyzer(functions, schemas);
        this.createViewAnalyzer = new CreateViewAnalyzer(relationAnalyzer);
        this.showCreateTableAnalyzer = new ShowCreateTableAnalyzer(schemas);
        this.explainStatementAnalyzer = new ExplainStatementAnalyzer(this);
        this.showStatementAnalyzer = new ShowStatementAnalyzer(this);
        this.updateAnalyzer = new UpdateAnalyzer(functions, relationAnalyzer);
        this.deleteAnalyzer = new DeleteAnalyzer(functions, relationAnalyzer);
        this.insertFromValuesAnalyzer = new InsertFromValuesAnalyzer(functions, schemas);
        this.insertFromSubQueryAnalyzer = new InsertFromSubQueryAnalyzer(functions, schemas, relationAnalyzer);
        this.unboundAnalyzer = new UnboundAnalyzer(
            relationAnalyzer,
            showCreateTableAnalyzer,
            showStatementAnalyzer,
            deleteAnalyzer,
            updateAnalyzer,
            insertFromValuesAnalyzer,
            insertFromSubQueryAnalyzer,
            explainStatementAnalyzer,
            createTableStatementAnalyzer
        );
        FulltextAnalyzerResolver fulltextAnalyzerResolver =
            new FulltextAnalyzerResolver(clusterService, analysisRegistry);
        NumberOfShards numberOfShards = new NumberOfShards(clusterService);
        this.createBlobTableAnalyzer = new CreateBlobTableAnalyzer(schemas, numberOfShards);
        this.createAnalyzerStatementAnalyzer = new CreateAnalyzerStatementAnalyzer(fulltextAnalyzerResolver);
        this.dropAnalyzerStatementAnalyzer = new DropAnalyzerStatementAnalyzer(fulltextAnalyzerResolver);
        this.refreshTableAnalyzer = new RefreshTableAnalyzer(schemas);
        this.optimizeTableAnalyzer = new OptimizeTableAnalyzer(schemas);
        this.alterTableAnalyzer = new AlterTableAnalyzer(schemas);
        this.alterBlobTableAnalyzer = new AlterBlobTableAnalyzer(schemas);
        this.alterTableAddColumnAnalyzer = new AlterTableAddColumnAnalyzer(schemas, functions);
        this.alterTableOpenCloseAnalyzer = new AlterTableOpenCloseAnalyzer(schemas);
        this.alterTableRerouteAnalyzer = new AlterTableRerouteAnalyzer(functions, schemas);
        this.copyAnalyzer = new CopyAnalyzer(schemas, functions);
        this.dropRepositoryAnalyzer = new DropRepositoryAnalyzer(repositoryService);
        this.createRepositoryAnalyzer = new CreateRepositoryAnalyzer(repositoryService, repositoryParamValidator);
        this.dropSnapshotAnalyzer = new DropSnapshotAnalyzer(repositoryService);
        this.createSnapshotAnalyzer = new CreateSnapshotAnalyzer(repositoryService, schemas);
        this.restoreSnapshotAnalyzer = new RestoreSnapshotAnalyzer(repositoryService, schemas);
        this.createFunctionAnalyzer = new CreateFunctionAnalyzer();
        this.dropFunctionAnalyzer = new DropFunctionAnalyzer();
        this.privilegesAnalyzer = new PrivilegesAnalyzer(userManager.isEnabled());
        this.createUserAnalyzer = new CreateUserAnalyzer(functions);
        this.alterUserAnalyzer = new AlterUserAnalyzer(functions);
        this.decommissionNodeAnalyzer = new DecommissionNodeAnalyzer(functions);
    }

    public Analysis boundAnalyze(Statement statement, CoordinatorTxnCtx coordinatorTxnCtx, ParameterContext parameterContext) {
        Analysis analysis = new Analysis(coordinatorTxnCtx, parameterContext, ParamTypeHints.EMPTY);
        AnalyzedStatement analyzedStatement = analyzedStatement(statement, analysis);
        SessionContext sessionContext = coordinatorTxnCtx.sessionContext();
        userManager.getAccessControl(sessionContext).ensureMayExecute(analyzedStatement);
        analysis.analyzedStatement(analyzedStatement);
        return analysis;
    }

    public AnalyzedStatement unboundAnalyze(Statement statement, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        return unboundAnalyzer.analyze(statement, sessionContext, paramTypeHints);
    }

    AnalyzedStatement analyzedStatement(Statement statement, Analysis analysis) {
        AnalyzedStatement analyzedStatement = statement.accept(dispatcher, analysis);
        assert analyzedStatement != null : "analyzed statement must not be null";
        return analyzedStatement;
    }

    private class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, Analysis> {

        @Override
        protected AnalyzedStatement visitQuery(Query node, Analysis analysis) {
            AnalyzedRelation relation = relationAnalyzer.analyze(
                node,
                analysis.transactionContext(),
                analysis.parameterContext());
            analysis.rootRelation(relation);
            return relation;
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, Analysis context) {
            return deleteAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues node, Analysis context) {
            return insertFromValuesAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, Analysis context) {
            return insertFromSubQueryAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, Analysis context) {
            return updateAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCopyFrom(CopyFrom node, Analysis context) {
            return copyAnalyzer.convertCopyFrom(node, context);
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo node, Analysis context) {
            return copyAnalyzer.convertCopyTo(node, context);
        }

        @Override
        public AnalyzedStatement visitDropTable(DropTable node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable<?> node, Analysis analysis) {
            return createTableStatementAnalyzer.analyze((CreateTable<Expression>) node, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitShowCreateTable(ShowCreateTable node, Analysis analysis) {
            ShowCreateTableAnalyzedStatement showCreateTableStatement =
                showCreateTableAnalyzer.analyze(node.table(), analysis.sessionContext());
            analysis.rootRelation(showCreateTableStatement);
            return showCreateTableStatement;
        }

        @Override
        public AnalyzedStatement visitShowSchemas(ShowSchemas node, Analysis analysis) {
            return showStatementAnalyzer.analyze(node, analysis);
        }

        @Override
        public AnalyzedStatement visitShowTransaction(ShowTransaction showTransaction, Analysis context) {
            return showStatementAnalyzer.analyzeShowTransaction(context);
        }

        @Override
        public AnalyzedStatement visitShowTables(ShowTables node, Analysis analysis) {
            return showStatementAnalyzer.analyze(node, analysis);
        }

        @Override
        protected AnalyzedStatement visitShowColumns(ShowColumns node, Analysis context) {
            return showStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitShowSessionParameter(ShowSessionParameter node, Analysis context) {
            return showStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer node, Analysis context) {
            return createAnalyzerStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitDropAnalyzer(DropAnalyzer node, Analysis context) {
            return dropAnalyzerStatementAnalyzer.analyze(node.name());
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable node, Analysis context) {
            return createBlobTableAnalyzer.analyze(node, context.parameterContext());
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable node, Analysis context) {
            return alterBlobTableAnalyzer.analyze(node, context.parameterContext().parameters());
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement node, Analysis context) {
            return refreshTableAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitOptimizeStatement(OptimizeStatement node, Analysis context) {
            return optimizeTableAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable node, Analysis context) {
            return alterTableAnalyzer.analyze(
                node, context.parameterContext().parameters(), context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterClusterRerouteRetryFailed(AlterClusterRerouteRetryFailed node, Analysis context) {
            return new RerouteRetryFailedAnalyzedStatement();
        }

        @Override
        public AnalyzedStatement visitAlterTableRename(AlterTableRename node, Analysis context) {
            return alterTableAnalyzer.analyzeRename(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn node, Analysis analysis) {
            return alterTableAddColumnAnalyzer.analyze(
                (AlterTableAddColumn<Expression>) node,
                analysis.sessionContext().searchPath(),
                analysis.parameterContext(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableOpenClose(AlterTableOpenClose node, Analysis context) {
            return alterTableOpenCloseAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableReroute(AlterTableReroute node, Analysis context) {
            return alterTableRerouteAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement node, Analysis context) {
            return SetStatementAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitResetStatement(ResetStatement node, Analysis context) {
            return SetStatementAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitKillStatement(KillStatement node, Analysis context) {
            return KillAnalyzer.analyze(node, context.parameterContext());
        }

        @Override
        public AnalyzedStatement visitDeallocateStatement(DeallocateStatement node, Analysis context) {
            return DeallocateAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitDropRepository(DropRepository node, Analysis context) {
            return dropRepositoryAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitCreateRepository(CreateRepository node, Analysis context) {
            return createRepositoryAnalyzer.analyze(node, context.parameterContext());
        }

        @Override
        public AnalyzedStatement visitCreateFunction(CreateFunction node, Analysis context) {
            return createFunctionAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitDropFunction(DropFunction node, Analysis context) {
            return dropFunctionAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCreateUser(CreateUser node, Analysis context) {
            return createUserAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropUser(DropUser node, Analysis context) {
            return new DropUserAnalyzedStatement(
                node.name(),
                node.ifExists()
            );
        }

        @Override
        public AnalyzedStatement visitAlterUser(AlterUser node, Analysis context) {
            return alterUserAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitGrantPrivilege(GrantPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeGrant(node,
                context.sessionContext().user(),
                context.sessionContext().searchPath(),
                schemas);
        }

        @Override
        public AnalyzedStatement visitDenyPrivilege(DenyPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeDeny(node,
                context.sessionContext().user(),
                context.sessionContext().searchPath(),
                schemas);
        }

        @Override
        public AnalyzedStatement visitRevokePrivilege(RevokePrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeRevoke(node,
                context.sessionContext().user(),
                context.sessionContext().searchPath(),
                schemas);
        }

        @Override
        public AnalyzedStatement visitDropSnapshot(DropSnapshot node, Analysis context) {
            return dropSnapshotAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitCreateSnapshot(CreateSnapshot node, Analysis context) {
            return createSnapshotAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitRestoreSnapshot(RestoreSnapshot node, Analysis context) {
            return restoreSnapshotAnalyzer.analyze(node, context);
        }

        @Override
        protected AnalyzedStatement visitExplain(Explain node, Analysis context) {
            return explainStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitBegin(BeginStatement node, Analysis context) {
            return new AnalyzedBegin();
        }

        @Override
        public AnalyzedStatement visitCommit(CommitStatement node, Analysis context) {
            return new AnalyzedCommit();
        }

        @Override
        protected AnalyzedStatement visitNode(Node node, Analysis context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot analyze statement: '%s'", node));
        }

        @Override
        public AnalyzedStatement visitCreateView(CreateView createView, Analysis analysis) {
            return createViewAnalyzer.analyze(createView, analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitSwapTable(SwapTable swapTable, Analysis analysis) {
            return swapTableAnalyzer.analyze(
                swapTable,
                analysis.transactionContext(),
                analysis.paramTypeHints(),
                analysis.sessionContext().searchPath()
            );
        }

        @Override
        public AnalyzedStatement visitGCDanglingArtifacts(GCDanglingArtifacts gcDanglingArtifacts, Analysis context) {
            return AnalyzedGCDanglingArtifacts.INSTANCE;
        }

        @Override
        public AnalyzedStatement visitAlterClusterDecommissionNode(DecommissionNodeStatement decommissionNodeStatement,
                                                                   Analysis analysis) {
            return decommissionNodeAnalyzer.analyze(decommissionNodeStatement,
                analysis.transactionContext(),
                analysis.paramTypeHints()
            );
        }

        @Override
        public AnalyzedStatement visitDropView(DropView dropView, Analysis analysis) {
            // No exists check to avoid stale clusterState race conditions
            ArrayList<RelationName> views = new ArrayList<>(dropView.names().size());
            ArrayList<RelationName> missing = new ArrayList<>();
            for (QualifiedName qualifiedName : dropView.names()) {
                try {
                    views.add(schemas.resolveView(qualifiedName, analysis.sessionContext().searchPath()).v2());
                } catch (RelationUnknown e) {
                    if (!dropView.ifExists()) {
                        missing.add(RelationName.of(qualifiedName, analysis.sessionContext().searchPath().currentSchema()));
                    }
                }
            }

            if (!missing.isEmpty()) {
                throw new RelationsUnknown(missing);
            }

            return new DropViewStmt(views, dropView.ifExists());
        }
    }
}
