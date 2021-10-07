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

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.execution.ddl.RepositoryService;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.analyze.LogicalReplicationAnalyzer;
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.AlterClusterRerouteRetryFailed;
import io.crate.sql.tree.AlterPublication;
import io.crate.sql.tree.AlterSubscription;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.AnalyzeStatement;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.CommitStatement;
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
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.DecommissionNodeStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DiscardStatement;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropCheckConstraint;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropPublication;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropSubscription;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.SetSessionAuthorizationStatement;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.SetTransactionStatement;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Update;
import io.crate.user.UserManager;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.analysis.AnalysisRegistry;

@Singleton
public class Analyzer {

    private final AnalyzerDispatcher dispatcher = new AnalyzerDispatcher();

    private final RelationAnalyzer relationAnalyzer;
    private final DropTableAnalyzer dropTableAnalyzer;
    private final DropCheckConstraintAnalyzer dropCheckConstraintAnalyzer;
    private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
    private final CreateTableAsAnalyzer createTableAsAnalyzer;
    private final ExplainStatementAnalyzer explainStatementAnalyzer;
    private final ShowStatementAnalyzer showStatementAnalyzer;
    private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
    private final CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer;
    private final DropAnalyzerStatementAnalyzer dropAnalyzerStatementAnalyzer;
    private final UserManager userManager;
    private final RefreshTableAnalyzer refreshTableAnalyzer;
    private final OptimizeTableAnalyzer optimizeTableAnalyzer;
    private final AlterTableAnalyzer alterTableAnalyzer;
    private final AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer;
    private final InsertAnalyzer insertAnalyzer;
    private final CopyAnalyzer copyAnalyzer;
    private final UpdateAnalyzer updateAnalyzer;
    private final DeleteAnalyzer deleteAnalyzer;
    private final DropRepositoryAnalyzer dropRepositoryAnalyzer;
    private final CreateRepositoryAnalyzer createRepositoryAnalyzer;
    private final DropSnapshotAnalyzer dropSnapshotAnalyzer;
    private final CreateSnapshotAnalyzer createSnapshotAnalyzer;
    private final RestoreSnapshotAnalyzer restoreSnapshotAnalyzer;
    private final CreateFunctionAnalyzer createFunctionAnalyzer;
    private final DropFunctionAnalyzer dropFunctionAnalyzer;
    private final PrivilegesAnalyzer privilegesAnalyzer;
    private final AlterTableRerouteAnalyzer alterTableRerouteAnalyzer;
    private final UserAnalyzer userAnalyzer;
    private final ViewAnalyzer viewAnalyzer;
    private final SwapTableAnalyzer swapTableAnalyzer;
    private final DecommissionNodeAnalyzer decommissionNodeAnalyzer;
    private final KillAnalyzer killAnalyzer;
    private final SetStatementAnalyzer setStatementAnalyzer;
    private final ResetStatementAnalyzer resetStatementAnalyzer;
    private final LogicalReplicationAnalyzer logicalReplicationAnalyzer;

    /**
     * @param relationAnalyzer is injected because we also need to inject it in
     *                         {@link io.crate.metadata.view.InternalViewInfoFactory} and we want to keep only a single
     *                         instance of the class
     */
    @Inject
    public Analyzer(Schemas schemas,
                    NodeContext nodeCtx,
                    RelationAnalyzer relationAnalyzer,
                    ClusterService clusterService,
                    AnalysisRegistry analysisRegistry,
                    RepositoryService repositoryService,
                    UserManager userManager,
                    SessionSettingRegistry sessionSettingRegistry,
                    LogicalReplicationService logicalReplicationService
    ) {
        this.relationAnalyzer = relationAnalyzer;
        this.dropTableAnalyzer = new DropTableAnalyzer(clusterService, schemas);
        this.dropCheckConstraintAnalyzer = new DropCheckConstraintAnalyzer(schemas);
        this.userManager = userManager;
        this.createTableStatementAnalyzer = new CreateTableStatementAnalyzer(nodeCtx);
        this.alterTableAnalyzer = new AlterTableAnalyzer(schemas, nodeCtx);
        this.alterTableAddColumnAnalyzer = new AlterTableAddColumnAnalyzer(schemas, nodeCtx);
        this.swapTableAnalyzer = new SwapTableAnalyzer(nodeCtx, schemas);
        this.viewAnalyzer = new ViewAnalyzer(relationAnalyzer, schemas);
        this.explainStatementAnalyzer = new ExplainStatementAnalyzer(this);
        this.showStatementAnalyzer = new ShowStatementAnalyzer(this, schemas, sessionSettingRegistry);
        this.updateAnalyzer = new UpdateAnalyzer(nodeCtx, relationAnalyzer);
        this.deleteAnalyzer = new DeleteAnalyzer(nodeCtx, relationAnalyzer);
        this.insertAnalyzer = new InsertAnalyzer(nodeCtx, schemas, relationAnalyzer);
        this.createTableAsAnalyzer = new CreateTableAsAnalyzer(createTableStatementAnalyzer, insertAnalyzer, relationAnalyzer);
        this.optimizeTableAnalyzer = new OptimizeTableAnalyzer(schemas, nodeCtx);
        this.createRepositoryAnalyzer = new CreateRepositoryAnalyzer(repositoryService, nodeCtx);
        this.dropRepositoryAnalyzer = new DropRepositoryAnalyzer(repositoryService);
        this.createSnapshotAnalyzer = new CreateSnapshotAnalyzer(repositoryService, nodeCtx);
        this.dropSnapshotAnalyzer = new DropSnapshotAnalyzer(repositoryService);
        this.userAnalyzer = new UserAnalyzer(nodeCtx);
        this.createBlobTableAnalyzer = new CreateBlobTableAnalyzer(schemas, nodeCtx);
        this.createFunctionAnalyzer = new CreateFunctionAnalyzer(nodeCtx);
        this.dropFunctionAnalyzer = new DropFunctionAnalyzer();
        this.refreshTableAnalyzer = new RefreshTableAnalyzer(nodeCtx, schemas);
        this.restoreSnapshotAnalyzer = new RestoreSnapshotAnalyzer(repositoryService, nodeCtx);
        FulltextAnalyzerResolver fulltextAnalyzerResolver =
            new FulltextAnalyzerResolver(clusterService, analysisRegistry);
        this.createAnalyzerStatementAnalyzer = new CreateAnalyzerStatementAnalyzer(fulltextAnalyzerResolver, nodeCtx);
        this.dropAnalyzerStatementAnalyzer = new DropAnalyzerStatementAnalyzer(fulltextAnalyzerResolver);
        this.decommissionNodeAnalyzer = new DecommissionNodeAnalyzer(nodeCtx);
        this.killAnalyzer = new KillAnalyzer(nodeCtx);
        this.alterTableRerouteAnalyzer = new AlterTableRerouteAnalyzer(nodeCtx, schemas);
        this.privilegesAnalyzer = new PrivilegesAnalyzer(userManager.isEnabled(), schemas);
        this.copyAnalyzer = new CopyAnalyzer(schemas, nodeCtx);
        this.setStatementAnalyzer = new SetStatementAnalyzer(nodeCtx);
        this.resetStatementAnalyzer = new ResetStatementAnalyzer(nodeCtx);
        this.logicalReplicationAnalyzer = new LogicalReplicationAnalyzer(
            schemas,
            logicalReplicationService,
            nodeCtx
        );
    }

    public AnalyzedStatement analyze(Statement statement,
                                     SessionContext sessionContext,
                                     ParamTypeHints paramTypeHints) {
        var analyzedStatement = statement.accept(
            dispatcher,
            new Analysis(
                new CoordinatorTxnCtx(sessionContext),
                paramTypeHints));
        userManager.getAccessControl(sessionContext).ensureMayExecute(analyzedStatement);
        return analyzedStatement;
    }

    AnalyzedStatement analyzedStatement(Statement statement, Analysis analysis) {
        AnalyzedStatement analyzedStatement = statement.accept(dispatcher, analysis);
        assert analyzedStatement != null : "analyzed statement must not be null";
        return analyzedStatement;
    }

    @SuppressWarnings("unchecked")
    private class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, Analysis> {

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterBlobTable<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterClusterDecommissionNode(DecommissionNodeStatement<?> node,
                                                                   Analysis context) {
            return decommissionNodeAnalyzer.analyze(
                (DecommissionNodeStatement<Expression>) node,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitAlterClusterRerouteRetryFailed(AlterClusterRerouteRetryFailed node,
                                                                     Analysis context) {
            return new AnalyzedRerouteRetryFailed();
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterTable<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropCheckConstraint(DropCheckConstraint<?> node, Analysis context) {
            return dropCheckConstraintAnalyzer.analyze(
                node.table(),
                node.name(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn<?> node, Analysis context) {
            return alterTableAddColumnAnalyzer.analyze(
                (AlterTableAddColumn<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableOpenClose(AlterTableOpenClose<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterTableOpenClose<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableRename(AlterTableRename<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterTableRename<Expression>) node,
                context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableReroute(AlterTableReroute<?> node, Analysis context) {
            return alterTableRerouteAnalyzer.analyze(
                (AlterTableReroute<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterUser(AlterUser<?> node, Analysis context) {
            return userAnalyzer.analyze(
                (AlterUser<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAnalyze(AnalyzeStatement analyzeStatement, Analysis analysis) {
            return new AnalyzedAnalyze();
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
        public AnalyzedStatement visitCopyFrom(CopyFrom<?> node, Analysis context) {
            return copyAnalyzer.analyzeCopyFrom(
                (CopyFrom<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo<?> node, Analysis context) {
            return copyAnalyzer.analyzeCopyTo(
                (CopyTo<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer<?> node, Analysis context) {
            return createAnalyzerStatementAnalyzer.analyze(
                (CreateAnalyzer<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable<?> node,
                                                      Analysis context) {
            return createBlobTableAnalyzer.analyze(
                (CreateBlobTable<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateFunction(CreateFunction<?> node, Analysis context) {
            return createFunctionAnalyzer.analyze(
                (CreateFunction<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitCreateRepository(CreateRepository node, Analysis context) {
            return createRepositoryAnalyzer.analyze(
                (CreateRepository<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateSnapshot(CreateSnapshot<?> node, Analysis context) {
            return createSnapshotAnalyzer.analyze(
                (CreateSnapshot<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable node, Analysis analysis) {
            return createTableStatementAnalyzer.analyze(
                (CreateTable<Expression>) node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateTableAs(CreateTableAs node, Analysis analysis) {
            return createTableAsAnalyzer.analyze(
                node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateUser(CreateUser<?> node, Analysis context) {
            return userAnalyzer.analyze(
                (CreateUser<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateView(CreateView node, Analysis context) {
            return viewAnalyzer.analyze(node, context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDeallocateStatement(DeallocateStatement node, Analysis context) {
            return DeallocateAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitDiscard(DiscardStatement discardStatement, Analysis context) {
            return new AnalyzedDiscard(discardStatement.target());
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, Analysis analysis) {
            return deleteAnalyzer.analyze(
                node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDenyPrivilege(DenyPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeDeny(
                node,
                context.sessionContext().sessionUser(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitDropAnalyzer(DropAnalyzer node, Analysis context) {
            return dropAnalyzerStatementAnalyzer.analyze(node.name());
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable<?> node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitDropFunction(DropFunction node, Analysis context) {
            return dropFunctionAnalyzer.analyze(node, context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitDropRepository(DropRepository node, Analysis context) {
            return dropRepositoryAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitDropSnapshot(DropSnapshot node, Analysis context) {
            return dropSnapshotAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedDropTable visitDropTable(DropTable<?> node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitDropUser(DropUser node, Analysis context) {
            return new AnalyzedDropUser(node.name(), node.ifExists());
        }

        @Override
        public AnalyzedStatement visitDropView(DropView node, Analysis context) {
            return viewAnalyzer.analyze(node, context.transactionContext());
        }

        @Override
        protected AnalyzedStatement visitExplain(Explain node, Analysis context) {
            return explainStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitGCDanglingArtifacts(GCDanglingArtifacts gcDanglingArtifacts, Analysis context) {
            return AnalyzedGCDanglingArtifacts.INSTANCE;
        }

        @Override
        public AnalyzedStatement visitGrantPrivilege(GrantPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeGrant(
                node,
                context.sessionContext().sessionUser(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitInsert(Insert<?> node, Analysis analysis) {
            return insertAnalyzer.analyze(
                (Insert<Expression>) node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitKillStatement(KillStatement<?> node, Analysis context) {
            return killAnalyzer.analyze(
                (KillStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitOptimizeStatement(OptimizeStatement<?> node, Analysis context) {
            return optimizeTableAnalyzer.analyze(
                (OptimizeStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext()
            );
        }

        @Override
        protected AnalyzedStatement visitQuery(Query node, Analysis context) {
            return relationAnalyzer.analyze(
                node,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement<?> node, Analysis context) {
            return refreshTableAnalyzer.analyze(
                (RefreshStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext()
            );
        }

        public AnalyzedStatement visitResetStatement(ResetStatement<?> node, Analysis context) {
            return resetStatementAnalyzer.analyze(
                (ResetStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitRestoreSnapshot(RestoreSnapshot<?> node, Analysis context) {
            return restoreSnapshotAnalyzer.analyze(
                (RestoreSnapshot<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitRevokePrivilege(RevokePrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeRevoke(
                node,
                context.sessionContext().sessionUser(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement<?> node, Analysis context) {
            return setStatementAnalyzer.analyze(
                (SetStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitSetSessionAuthorizationStatement(SetSessionAuthorizationStatement node,
                                                                       Analysis context) {
            return new AnalyzedSetSessionAuthorizationStatement(node.user(), node.scope());
        }

        @Override
        public AnalyzedStatement visitSetTransaction(SetTransactionStatement setTransaction, Analysis analysis) {
            return new AnalyzedSetTransaction(setTransaction.transactionModes());
        }

        @Override
        protected AnalyzedStatement visitShowColumns(ShowColumns node, Analysis context) {
            var coordinatorTxnCtx = context.transactionContext();
            Query query = showStatementAnalyzer.rewriteShowColumns(
                node,
                coordinatorTxnCtx.sessionContext().searchPath().currentSchema());
            return relationAnalyzer.analyze(
                query,
                coordinatorTxnCtx,
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitShowCreateTable(ShowCreateTable<?> node, Analysis context) {
            return showStatementAnalyzer.analyzeShowCreateTable(node.table(), context);
        }

        @Override
        protected AnalyzedStatement visitShowSchemas(ShowSchemas node, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowSchemas(node);
            return relationAnalyzer.analyze(
                query,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitShowSessionParameter(ShowSessionParameter node, Analysis context) {
            showStatementAnalyzer.validateSessionSetting(node.parameter());
            Query query = showStatementAnalyzer.rewriteShowSessionParameter(node);
            return relationAnalyzer.analyze(
                query,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        protected AnalyzedStatement visitShowTables(ShowTables node, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowTables(node);
            return relationAnalyzer.analyze(
                query,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitShowTransaction(ShowTransaction showTransaction, Analysis context) {
            return showStatementAnalyzer.analyzeShowTransaction(context);
        }

        @Override
        public AnalyzedStatement visitSwapTable(SwapTable<?> node, Analysis analysis) {
            return swapTableAnalyzer.analyze(
                (SwapTable<Expression>) node,
                analysis.transactionContext(),
                analysis.paramTypeHints()
            );
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, Analysis analysis) {
            return updateAnalyzer.analyze(
                node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreatePublication(CreatePublication createPublication,
                                                        Analysis context) {
            return logicalReplicationAnalyzer.analyze(createPublication, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitDropPublication(DropPublication dropPublication,
                                                      Analysis context) {
            return logicalReplicationAnalyzer.analyze(dropPublication);
        }

        @Override
        public AnalyzedStatement visitAlterPublication(AlterPublication alterPublication,
                                                       Analysis context) {
            return logicalReplicationAnalyzer.analyze(alterPublication, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitCreateSubscription(CreateSubscription<?> createSubscription,
                                                         Analysis context) {
            return logicalReplicationAnalyzer.analyze(
                (CreateSubscription<Expression>) createSubscription,
                context.paramTypeHints(),
                context.transactionContext()
            );
        }

        @Override
        public AnalyzedStatement visitDropSubscription(DropSubscription dropSubscription,
                                                       Analysis context) {
            return logicalReplicationAnalyzer.analyze(dropSubscription);
        }

        @Override
        public AnalyzedStatement visitAlterSubscription(AlterSubscription alterSubscription,
                                                        Analysis context) {
            return logicalReplicationAnalyzer.analyze(alterSubscription);
        }
    }
}
