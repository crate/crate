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

package io.crate.planner;

import io.crate.analyze.AnalyzedAlterBlobTable;
import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedAlterTableDropCheckConstraint;
import io.crate.analyze.AnalyzedAlterTableOpenClose;
import io.crate.analyze.AnalyzedAlterTableRename;
import io.crate.analyze.AnalyzedAlterUser;
import io.crate.analyze.AnalyzedAnalyze;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedCommit;
import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.AnalyzedCopyTo;
import io.crate.analyze.AnalyzedCreateAnalyzer;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateFunction;
import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.AnalyzedCreateSnapshot;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateTableAs;
import io.crate.analyze.AnalyzedCreateUser;
import io.crate.analyze.AnalyzedDeallocate;
import io.crate.analyze.AnalyzedDecommissionNode;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedDiscard;
import io.crate.analyze.AnalyzedDropAnalyzer;
import io.crate.analyze.AnalyzedDropFunction;
import io.crate.analyze.AnalyzedDropRepository;
import io.crate.analyze.AnalyzedDropSnapshot;
import io.crate.analyze.AnalyzedDropTable;
import io.crate.analyze.AnalyzedDropUser;
import io.crate.analyze.AnalyzedDropView;
import io.crate.analyze.AnalyzedGCDanglingArtifacts;
import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedKill;
import io.crate.analyze.AnalyzedOptimizeTable;
import io.crate.analyze.AnalyzedPromoteReplica;
import io.crate.analyze.AnalyzedRefreshTable;
import io.crate.analyze.AnalyzedRerouteAllocateReplicaShard;
import io.crate.analyze.AnalyzedRerouteCancelShard;
import io.crate.analyze.AnalyzedRerouteMoveShard;
import io.crate.analyze.AnalyzedRerouteRetryFailed;
import io.crate.analyze.AnalyzedResetStatement;
import io.crate.analyze.AnalyzedRestoreSnapshot;
import io.crate.analyze.AnalyzedSetLicenseStatement;
import io.crate.analyze.AnalyzedSetSessionAuthorizationStatement;
import io.crate.analyze.AnalyzedSetStatement;
import io.crate.analyze.AnalyzedSetTransaction;
import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.AnalyzedSwapTable;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.CreateViewStmt;
import io.crate.analyze.DCLStatement;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.consumer.CreateTableAsPlanner;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.dcl.GenericDCLPlan;
import io.crate.planner.node.ddl.AlterBlobTablePlan;
import io.crate.planner.node.ddl.AlterTableAddColumnPlan;
import io.crate.planner.node.ddl.AlterTableDropCheckConstraintPlan;
import io.crate.planner.node.ddl.AlterTableOpenClosePlan;
import io.crate.planner.node.ddl.AlterTablePlan;
import io.crate.planner.node.ddl.AlterTableRenameTablePlan;
import io.crate.planner.node.ddl.AlterUserPlan;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.node.ddl.CreateFunctionPlan;
import io.crate.planner.node.ddl.CreateRepositoryPlan;
import io.crate.planner.node.ddl.CreateSnapshotPlan;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.node.ddl.CreateUserPlan;
import io.crate.planner.node.ddl.DropAnalyzerPlan;
import io.crate.planner.node.ddl.DropFunctionPlan;
import io.crate.planner.node.ddl.DropRepositoryPlan;
import io.crate.planner.node.ddl.DropSnapshotPlan;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.DropUserPlan;
import io.crate.planner.node.ddl.OptimizeTablePlan;
import io.crate.planner.node.ddl.RefreshTablePlan;
import io.crate.planner.node.ddl.ResetSettingsPlan;
import io.crate.planner.node.ddl.RestoreSnapshotPlan;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.node.management.AlterTableReroutePlan;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.node.management.RerouteRetryFailedPlan;
import io.crate.planner.node.management.ShowCreateTablePlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.statement.CopyFromPlan;
import io.crate.planner.statement.CopyToPlan;
import io.crate.planner.statement.DeletePlanner;
import io.crate.planner.statement.SetLicensePlan;
import io.crate.planner.statement.SetSessionAuthorizationPlan;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;
import io.crate.replication.logical.analyze.AnalyzedCreatePublication;
import io.crate.sql.tree.SetSessionAuthorizationStatement;
import io.crate.statistics.TableStats;
import io.crate.user.UserManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;

@Singleton
public class Planner extends AnalyzedStatementVisitor<PlannerContext, Plan> {

    private static final Logger LOGGER = LogManager.getLogger(Planner.class);

    private final ClusterService clusterService;
    private final TableStats tableStats;
    private final LogicalPlanner logicalPlanner;
    private final NumberOfShards numberOfShards;
    private final TableCreator tableCreator;
    private final Schemas schemas;
    private final UserManager userManager;
    private final SessionSettingRegistry sessionSettingRegistry;

    private List<String> awarenessAttributes;

    @Inject
    public Planner(Settings settings,
                   ClusterService clusterService,
                   NodeContext nodeCtx,
                   TableStats tableStats,
                   NumberOfShards numberOfShards,
                   TableCreator tableCreator,
                   Schemas schemas,
                   UserManager userManager,
                   SessionSettingRegistry sessionSettingRegistry) {
        this.clusterService = clusterService;
        this.tableStats = tableStats;
        this.logicalPlanner = new LogicalPlanner(nodeCtx, tableStats, () -> clusterService.state().nodes().getMinNodeVersion());
        this.numberOfShards = numberOfShards;
        this.tableCreator = tableCreator;
        this.schemas = schemas;
        this.userManager = userManager;
        this.sessionSettingRegistry = sessionSettingRegistry;
        initAwarenessAttributes(settings);
    }

    private void initAwarenessAttributes(Settings settings) {
        awarenessAttributes =
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes);
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    public ClusterState currentClusterState() {
        return clusterService.state();
    }

    /**
     * dispatch plan creation based on analyzed statement
     *
     * @param analyzedStatement analyzed statement to create plan from
     * @return plan
     */
    public Plan plan(AnalyzedStatement analyzedStatement, PlannerContext plannerContext) {
        return analyzedStatement.accept(this, plannerContext);
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                                                              "Cannot create Plan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
    }

    @Override
    public Plan visitAnalyze(AnalyzedAnalyze analyzedAnalyze, PlannerContext context) {
        return new AnalyzePlan();
    }

    @Override
    public Plan visitBegin(AnalyzedBegin analyzedBegin, PlannerContext context) {
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitCommit(AnalyzedCommit analyzedCommit, PlannerContext context) {
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitSelectStatement(AnalyzedRelation relation, PlannerContext context) {
        return logicalPlanner.plan(relation, context);
    }

    @Override
    public Plan visitSwapTable(AnalyzedSwapTable swapTable, PlannerContext context) {
        return new SwapTablePlan(swapTable);
    }

    @Override
    public Plan visitGCDanglingArtifacts(AnalyzedGCDanglingArtifacts gcDanglingArtifacts, PlannerContext context) {
        return new GCDangingArtifactsPlan();
    }

    @Override
    public Plan visitDecommissionNode(AnalyzedDecommissionNode decommissionNode, PlannerContext context) {
        return new DecommissionNodePlan(decommissionNode);
    }

    @Override
    protected Plan visitAnalyzedInsertStatement(AnalyzedInsertStatement statement, PlannerContext context) {
        return logicalPlanner.plan(statement, context);
    }

    @Override
    public Plan visitAnalyzedUpdateStatement(AnalyzedUpdateStatement update, PlannerContext context) {
        return UpdatePlanner.plan(
            update, context, new SubqueryPlanner(s -> logicalPlanner.planSubSelect(s, context)));
    }

    @Override
    protected Plan visitAnalyzedDeleteStatement(AnalyzedDeleteStatement statement, PlannerContext context) {
        return DeletePlanner.planDelete(
            statement,
            new SubqueryPlanner(s -> logicalPlanner.planSubSelect(s, context)),
            context
        );
    }

    @Override
    protected Plan visitCopyFromStatement(AnalyzedCopyFrom analysis, PlannerContext context) {
        return new CopyFromPlan(analysis);
    }

    @Override
    protected Plan visitCopyToStatement(AnalyzedCopyTo analysis, PlannerContext context) {
        return new CopyToPlan(analysis, tableStats);
    }

    @Override
    public Plan visitShowCreateTableAnalyzedStatement(AnalyzedShowCreateTable statement, PlannerContext context) {
        return new ShowCreateTablePlan(statement);
    }

    @Override
    protected Plan visitCreateRepositoryAnalyzedStatement(AnalyzedCreateRepository analysis,
                                                          PlannerContext context) {
        return new CreateRepositoryPlan(analysis);
    }

    @Override
    public Plan visitDropRepositoryAnalyzedStatement(AnalyzedDropRepository analysis,
                                                     PlannerContext context) {
        return new DropRepositoryPlan(analysis);
    }

    @Override
    public Plan visitCreateSnapshotAnalyzedStatement(AnalyzedCreateSnapshot analysis,
                                                     PlannerContext context) {
        return new CreateSnapshotPlan(analysis);
    }

    @Override
    public Plan visitDropSnapshotAnalyzedStatement(AnalyzedDropSnapshot analysis,
                                                   PlannerContext context) {
        return new DropSnapshotPlan(analysis);
    }

    @Override
    public Plan visitDCLStatement(DCLStatement statement, PlannerContext context) {
        return new GenericDCLPlan(statement);
    }

    @Override
    public Plan visitDropTable(AnalyzedDropTable<?> dropTable, PlannerContext context) {
        return new DropTablePlan(dropTable);
    }

    @Override
    public Plan visitCreateTable(AnalyzedCreateTable createTable, PlannerContext context) {
        return new CreateTablePlan(createTable, numberOfShards, tableCreator, schemas);
    }

    @Override
    public Plan visitCreateTableAs(AnalyzedCreateTableAs createTableAs, PlannerContext context) {
        return CreateTableAsPlanner.plan(
            createTableAs, numberOfShards, tableCreator, schemas, context, logicalPlanner
        );
    }

    @Override
    public Plan visitAlterTable(AnalyzedAlterTable alterTable, PlannerContext context) {
        return new AlterTablePlan(alterTable);
    }

    @Override
    public Plan visitAnalyzedAlterBlobTable(AnalyzedAlterBlobTable analysis,
                                            PlannerContext context) {
        return new AlterBlobTablePlan(analysis);
    }

    @Override
    public Plan visitAnalyzedCreateBlobTable(AnalyzedCreateBlobTable analysis,
                                             PlannerContext context) {
        return new CreateBlobTablePlan(analysis, numberOfShards);
    }

    public Plan visitRefreshTableStatement(AnalyzedRefreshTable analysis, PlannerContext context) {
        return new RefreshTablePlan(analysis);
    }

    @Override
    public Plan visitAnalyzedAlterTableRename(AnalyzedAlterTableRename analysis,
                                              PlannerContext context) {
        return new AlterTableRenameTablePlan(analysis);
    }

    @Override
    public Plan visitAnalyzedAlterTableOpenClose(AnalyzedAlterTableOpenClose analysis,
                                                 PlannerContext context) {
        return new AlterTableOpenClosePlan(analysis);
    }

    @Override
    protected Plan visitAnalyzedCreateUser(AnalyzedCreateUser analysis,
                                           PlannerContext context) {
        return new CreateUserPlan(analysis, userManager);
    }

    @Override
    public Plan visitAnalyzedAlterUser(AnalyzedAlterUser analysis, PlannerContext context) {
        return new AlterUserPlan(analysis, userManager);
    }

    @Override
    protected Plan visitDropUser(AnalyzedDropUser analysis, PlannerContext context) {
        return new DropUserPlan(analysis, userManager);
    }

    protected Plan visitCreateAnalyzerStatement(AnalyzedCreateAnalyzer analysis, PlannerContext context) {
        return new CreateAnalyzerPlan(analysis);
    }

    @Override
    public Plan visitAlterTableAddColumn(AnalyzedAlterTableAddColumn alterTableAddColumn,
                                         PlannerContext context) {
        return new AlterTableAddColumnPlan(alterTableAddColumn);
    }

    @Override
    public Plan visitAlterTableDropCheckConstraint(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint,
                                                   PlannerContext context) {
        return new AlterTableDropCheckConstraintPlan(dropCheckConstraint);
    }

    @Override
    protected Plan visitCreateFunction(AnalyzedCreateFunction analysis,
                                       PlannerContext context) {
        return new CreateFunctionPlan(analysis);
    }

    @Override
    public Plan visitDropFunction(AnalyzedDropFunction analysis, PlannerContext context) {
        return new DropFunctionPlan(analysis);
    }

    @Override
    protected Plan visitDropAnalyzerStatement(AnalyzedDropAnalyzer analysis, PlannerContext context) {
        return new DropAnalyzerPlan(analysis);
    }

    @Override
    public Plan visitResetAnalyzedStatement(AnalyzedResetStatement resetStatement, PlannerContext context) {
        if (resetStatement.settingsToRemove().isEmpty()) {
            return NoopPlan.INSTANCE;
        }
        return new ResetSettingsPlan(resetStatement);
    }

    @Override
    public Plan visitRestoreSnapshotAnalyzedStatement(AnalyzedRestoreSnapshot analysis, PlannerContext context) {
        return new RestoreSnapshotPlan(analysis);
    }

    @Override
    public Plan visitSetStatement(AnalyzedSetStatement setStatement, PlannerContext context) {
        switch (setStatement.scope()) {
            case LICENSE:
                throw new AssertionError(
                    "`AnalyzedSetStatement` with scope `LICENSE` should have been converted to `AnalyzedSetLicenseStatement` by the analyzer");
            case LOCAL:
                LOGGER.info(
                    "SET LOCAL `{}` statement will be ignored. " +
                    "CrateDB has no transactions, so any `SET LOCAL` change would be dropped in the next statement.", setStatement.settings());
                return NoopPlan.INSTANCE;
            case SESSION:
                return new SetSessionPlan(setStatement.settings(), sessionSettingRegistry);
            case GLOBAL:
            default:
                return new UpdateSettingsPlan(setStatement.settings(), setStatement.isPersistent());
        }
    }

    @Override
    public Plan visitSetSessionAuthorizationStatement(AnalyzedSetSessionAuthorizationStatement analysis,
                                                      PlannerContext context) {
        if (analysis.scope() == SetSessionAuthorizationStatement.Scope.LOCAL) {
            LOGGER.info(
                "SET LOCAL SESSION AUTHORIZATION 'username' statement will be ignored. " +
                "CrateDB has no transactions, so any `SET LOCAL` change would be dropped in the next statement.");
            return NoopPlan.INSTANCE;
        } else {
            return new SetSessionAuthorizationPlan(analysis, userManager);
        }
    }

    @Override
    public Plan visitSetTransaction(AnalyzedSetTransaction setTransaction, PlannerContext context) {
        LOGGER.info("'SET TRANSACTION' statement is ignored. CrateDB doesn't support transactions");
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitSetLicenseStatement(AnalyzedSetLicenseStatement setLicenseAnalyzedStatement, PlannerContext context) {
        return new SetLicensePlan(setLicenseAnalyzedStatement);
    }

    @Override
    public Plan visitKillAnalyzedStatement(AnalyzedKill analysis, PlannerContext context) {
        return new KillPlan(analysis.jobId());
    }

    @Override
    public Plan visitDeallocateAnalyzedStatement(AnalyzedDeallocate analysis, PlannerContext context) {
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitDiscard(AnalyzedDiscard discard, PlannerContext context) {
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, PlannerContext context) {
        ProfilingContext ctx = explainAnalyzedStatement.context();
        if (ctx == null) {
            return new ExplainPlan(explainAnalyzedStatement.statement().accept(this, context), null);
        } else {
            Timer timer = ctx.createAndStartTimer(ExplainPlan.Phase.Plan.name());
            Plan subPlan = explainAnalyzedStatement.statement().accept(this, context);
            ctx.stopTimerAndStoreDuration(timer);
            return new ExplainPlan(subPlan, ctx);
        }
    }

    @Override
    public Plan visitCreateViewStmt(CreateViewStmt createViewStmt, PlannerContext context) {
        return new CreateViewPlan(createViewStmt);
    }

    @Override
    public Plan visitDropView(AnalyzedDropView dropView, PlannerContext context) {
        return new DropViewPlan(dropView);
    }

    @Override
    public Plan visitOptimizeTableStatement(AnalyzedOptimizeTable analysis, PlannerContext context) {
        return new OptimizeTablePlan(analysis);
    }

    @Override
    protected Plan visitRerouteMoveShard(AnalyzedRerouteMoveShard analysis, PlannerContext context) {
        return new AlterTableReroutePlan(analysis);
    }

    @Override
    protected Plan visitRerouteAllocateReplicaShard(AnalyzedRerouteAllocateReplicaShard analysis,
                                                    PlannerContext context) {
        return new AlterTableReroutePlan(analysis);
    }

    @Override
    protected Plan visitRerouteCancelShard(AnalyzedRerouteCancelShard analysis, PlannerContext context) {
        return new AlterTableReroutePlan(analysis);
    }

    @Override
    public Plan visitReroutePromoteReplica(AnalyzedPromoteReplica analysis, PlannerContext context) {
        return new AlterTableReroutePlan(analysis);
    }

    @Override
    public Plan visitRerouteRetryFailedStatement(AnalyzedRerouteRetryFailed analysis, PlannerContext context) {
        return new RerouteRetryFailedPlan();
    }

    @Override
    public Plan visitCreatePublication(AnalyzedCreatePublication createPublication,
                                       PlannerContext context) {
        return super.visitCreatePublication(createPublication, context);
    }
}

