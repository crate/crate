/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.crate.analyze.AnalyzedAlterBlobTable;
import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.AnalyzedAlterTableOpenClose;
import io.crate.analyze.AnalyzedAlterTableRename;
import io.crate.analyze.AnalyzedAlterUser;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedCommit;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateFunction;
import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.AnalyzedCreateSnapshot;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateUser;
import io.crate.analyze.AnalyzedDecommissionNodeStatement;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedDropFunction;
import io.crate.analyze.AnalyzedDropRepository;
import io.crate.analyze.AnalyzedDropSnapshot;
import io.crate.analyze.AnalyzedDropUser;
import io.crate.analyze.AnalyzedGCDanglingArtifacts;
import io.crate.analyze.AnalyzedRefreshTable;
import io.crate.analyze.AnalyzedOptimizeTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.AnalyzedSwapTable;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.analyze.CreateAnalyzerAnalyzedStatement;
import io.crate.analyze.CreateViewStmt;
import io.crate.analyze.DCLStatement;
import io.crate.analyze.DDLStatement;
import io.crate.analyze.DeallocateAnalyzedStatement;
import io.crate.analyze.DropAnalyzerStatement;
import io.crate.analyze.DropTableAnalyzedStatement;
import io.crate.analyze.DropViewStmt;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.InsertFromValuesAnalyzedStatement;
import io.crate.analyze.KillAnalyzedStatement;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ResetAnalyzedStatement;
import io.crate.analyze.SetAnalyzedStatement;
import io.crate.analyze.SetLicenseAnalyzedStatement;
import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.auth.user.UserManager;
import io.crate.exceptions.LicenseViolationException;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.expression.symbol.Symbol;
import io.crate.license.LicenseService;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.dcl.GenericDCLPlan;
import io.crate.planner.node.ddl.AlterBlobTablePlan;
import io.crate.planner.node.ddl.AlterTableAddColumnPlan;
import io.crate.planner.node.ddl.AlterTableOpenClosePlan;
import io.crate.planner.node.ddl.AlterTablePlan;
import io.crate.planner.node.ddl.AlterTableRenameTablePlan;
import io.crate.planner.node.ddl.AlterUserPlan;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.node.ddl.CreateDropAnalyzerPlan;
import io.crate.planner.node.ddl.CreateFunctionPlan;
import io.crate.planner.node.ddl.CreateRepositoryPlan;
import io.crate.planner.node.ddl.CreateSnapshotPlan;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.node.ddl.CreateUserPlan;
import io.crate.planner.node.ddl.DropFunctionPlan;
import io.crate.planner.node.ddl.DropRepositoryPlan;
import io.crate.planner.node.ddl.DropSnapshotPlan;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.DropUserPlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.ddl.OptimizeTablePlan;
import io.crate.planner.node.ddl.RefreshTablePlan;
import io.crate.planner.node.ddl.UpdateSettingsPlan;
import io.crate.planner.node.dml.LegacyUpsertById;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.node.management.ShowCreateTablePlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.statement.CopyStatementPlanner;
import io.crate.planner.statement.DeletePlanner;
import io.crate.planner.statement.SetLicensePlan;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;
import io.crate.sql.tree.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

@Singleton
public class Planner extends AnalyzedStatementVisitor<PlannerContext, Plan> {

    private static final Logger LOGGER = LogManager.getLogger(Planner.class);

    private final ClusterService clusterService;
    private final Functions functions;
    private final LogicalPlanner logicalPlanner;
    private final IsStatementExecutionAllowed isStatementExecutionAllowed;
    private final NumberOfShards numberOfShards;
    private final TableCreator tableCreator;
    private final Schemas schemas;
    private final UserManager userManager;

    private List<String> awarenessAttributes;

    @Inject
    public Planner(Settings settings,
                   ClusterService clusterService,
                   Functions functions,
                   TableStats tableStats,
                   LicenseService licenseService,
                   NumberOfShards numberOfShards,
                   TableCreator tableCreator,
                   Schemas schemas,
                   UserManager userManager) {
        this(
            settings,
            clusterService,
            functions,
            tableStats,
            numberOfShards,
            tableCreator,
            schemas,
            userManager,
            () -> licenseService.getLicenseState() == LicenseService.LicenseState.VALID
        );
    }

    @VisibleForTesting
    public Planner(Settings settings,
                   ClusterService clusterService,
                   Functions functions,
                   TableStats tableStats,
                   NumberOfShards numberOfShards,
                   TableCreator tableCreator,
                   Schemas schemas,
                   UserManager userManager,
                   BooleanSupplier hasValidLicense) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.logicalPlanner = new LogicalPlanner(functions, tableStats);
        this.isStatementExecutionAllowed = new IsStatementExecutionAllowed(hasValidLicense);
        this.numberOfShards = numberOfShards;
        this.tableCreator = tableCreator;
        this.schemas = schemas;
        this.userManager = userManager;
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
        if (isStatementExecutionAllowed.test(analyzedStatement) == false) {
            throw new LicenseViolationException("Statement not allowed");
        }
        return process(analyzedStatement, plannerContext);
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                                                              "Cannot create Plan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
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
    public Plan visitDecommissionNode(AnalyzedDecommissionNodeStatement decommissionNode, PlannerContext context) {
        return new DecommissionNodePlan(decommissionNode);
    }

    @Override
    protected Plan visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement statement, PlannerContext context) {
        Preconditions.checkState(!statement.sourceMaps().isEmpty(), "no values given");
        return processInsertStatement(statement);
    }

    @Override
    protected Plan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement statement, PlannerContext context) {
        return logicalPlanner.plan(statement, context);
    }

    @Override
    public Plan visitAnalyzedUpdateStatement(AnalyzedUpdateStatement update, PlannerContext context) {
        return UpdatePlanner.plan(
            update, functions, context, new SubqueryPlanner(s -> logicalPlanner.planSubSelect(s, context)));
    }

    @Override
    protected Plan visitAnalyzedDeleteStatement(AnalyzedDeleteStatement statement, PlannerContext context) {
        return DeletePlanner.planDelete(
            functions,
            statement,
            new SubqueryPlanner(s -> logicalPlanner.planSubSelect(s, context)),
            context
        );
    }

    @Override
    protected Plan visitCopyFromStatement(CopyFromAnalyzedStatement analysis, PlannerContext context) {
        return CopyStatementPlanner.planCopyFrom(analysis);
    }

    @Override
    protected Plan visitCopyToStatement(CopyToAnalyzedStatement analysis, PlannerContext context) {
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));
        return CopyStatementPlanner.planCopyTo(analysis, logicalPlanner, subqueryPlanner);
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
    protected Plan visitDDLStatement(DDLStatement statement, PlannerContext context) {
        return new GenericDDLPlan(statement);
    }

    @Override
    public Plan visitDCLStatement(DCLStatement statement, PlannerContext context) {
        return new GenericDCLPlan(statement);
    }

    @Override
    public Plan visitDropTable(DropTableAnalyzedStatement<?> dropTable, PlannerContext context) {
        TableInfo table = dropTable.table();
        if (table == null) {
            return NoopPlan.INSTANCE;
        }
        return new DropTablePlan(table, dropTable.dropIfExists());
    }

    @Override
    public Plan visitCreateTable(AnalyzedCreateTable createTable, PlannerContext context) {
        return new CreateTablePlan(createTable, numberOfShards, tableCreator, schemas);
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

    @Override
    protected Plan visitCreateAnalyzerStatement(CreateAnalyzerAnalyzedStatement analysis, PlannerContext context) {
        return new CreateDropAnalyzerPlan(analysis.buildSettings());
    }

    @Override
    public Plan visitAlterTableAddColumn(AnalyzedAlterTableAddColumn alterTableAddColumn,
                                         PlannerContext context) {
        return new AlterTableAddColumnPlan(alterTableAddColumn);
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
    protected Plan visitDropAnalyzerStatement(DropAnalyzerStatement analysis, PlannerContext context) {
        return new CreateDropAnalyzerPlan(analysis.settingsForRemoval());
    }

    @Override
    public Plan visitResetAnalyzedStatement(ResetAnalyzedStatement resetStatement, PlannerContext context) {
        Set<String> settingsToRemove = resetStatement.settingsToRemove();
        if (settingsToRemove.isEmpty()) {
            return NoopPlan.INSTANCE;
        }

        Map<String, List<Expression>> nullSettings = new HashMap<>(settingsToRemove.size(), 1);
        for (String setting : settingsToRemove) {
            nullSettings.put(setting, null);
        }
        return new UpdateSettingsPlan(nullSettings, nullSettings);
    }

    @Override
    public Plan visitSetStatement(SetAnalyzedStatement setStatement, PlannerContext context) {
        switch (setStatement.scope()) {
            case LICENSE:
                LOGGER.warn("SET LICENSE STATEMENT WILL BE IGNORED: {}", setStatement.settings());
                return NoopPlan.INSTANCE;
            case LOCAL:
                LOGGER.warn("SET LOCAL STATEMENT WILL BE IGNORED: {}", setStatement.settings());
                return NoopPlan.INSTANCE;
            case SESSION_TRANSACTION_MODE:
                LOGGER.warn("'SET SESSION CHARACTERISTICS AS TRANSACTION' STATEMENT WILL BE IGNORED");
                return NoopPlan.INSTANCE;
            case SESSION:
                return new SetSessionPlan(setStatement.settings());
            case GLOBAL:
            default:
                if (setStatement.isPersistent()) {
                    return new UpdateSettingsPlan(setStatement.settings());
                } else {
                    return new UpdateSettingsPlan(
                        Collections.emptyMap(),
                        setStatement.settings()
                    );
                }
        }
    }

    @Override
    public Plan visitSetLicenseStatement(SetLicenseAnalyzedStatement setLicenseAnalyzedStatement, PlannerContext context) {
        return new SetLicensePlan(setLicenseAnalyzedStatement);
    }

    @Override
    public Plan visitKillAnalyzedStatement(KillAnalyzedStatement analysis, PlannerContext context) {
        return new KillPlan(analysis.jobId());
    }

    @Override
    public Plan visitDeallocateAnalyzedStatement(DeallocateAnalyzedStatement analysis, PlannerContext context) {
        return NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, PlannerContext context) {
        ProfilingContext ctx = explainAnalyzedStatement.context();
        if (ctx == null) {
            return new ExplainPlan(process(explainAnalyzedStatement.statement(), context), null);
        } else {
            Timer timer = ctx.createAndStartTimer(ExplainPlan.Phase.Plan.name());
            Plan subPlan = process(explainAnalyzedStatement.statement(), context);
            ctx.stopTimerAndStoreDuration(timer);
            return new ExplainPlan(subPlan, ctx);
        }
    }

    @Override
    public Plan visitCreateViewStmt(CreateViewStmt createViewStmt, PlannerContext context) {
        return new CreateViewPlan(createViewStmt);
    }

    @Override
    public Plan visitDropView(DropViewStmt dropViewStmt, PlannerContext context) {
        return new DropViewPlan(dropViewStmt);
    }

    @Override
    public Plan visitOptimizeTableStatement(AnalyzedOptimizeTable analysis, PlannerContext context) {
        return new OptimizeTablePlan(analysis);
    }

    private static LegacyUpsertById processInsertStatement(InsertFromValuesAnalyzedStatement analysis) {
        String[] onDuplicateKeyAssignmentsColumns = null;
        if (analysis.onDuplicateKeyAssignmentsColumns().size() > 0) {
            onDuplicateKeyAssignmentsColumns = analysis.onDuplicateKeyAssignmentsColumns().get(0);
        }
        DocTableInfo tableInfo = analysis.tableInfo();
        LegacyUpsertById legacyUpsertById = new LegacyUpsertById(
            analysis.numBulkResponses(),
            tableInfo.isPartitioned(),
            analysis.bulkIndices(),
            analysis.isIgnoreDuplicateKeys(),
            onDuplicateKeyAssignmentsColumns,
            analysis.columns().toArray(new Reference[analysis.columns().size()])
        );
        if (tableInfo.isPartitioned()) {
            List<String> partitions = analysis.generatePartitions();
            String[] indices = partitions.toArray(new String[partitions.size()]);
            for (int i = 0; i < indices.length; i++) {
                Symbol[] onDuplicateKeyAssignments = null;
                if (analysis.onDuplicateKeyAssignmentsColumns().size() > i) {
                    onDuplicateKeyAssignments = analysis.onDuplicateKeyAssignments().get(i);
                }
                legacyUpsertById.add(
                    indices[i],
                    analysis.ids().get(i),
                    analysis.routingValues().get(i),
                    onDuplicateKeyAssignments,
                    null,
                    null,
                    null,
                    analysis.sourceMaps().get(i));
            }
        } else {
            for (int i = 0; i < analysis.ids().size(); i++) {
                Symbol[] onDuplicateKeyAssignments = null;
                if (analysis.onDuplicateKeyAssignments().size() > i) {
                    onDuplicateKeyAssignments = analysis.onDuplicateKeyAssignments().get(i);
                }
                legacyUpsertById.add(
                    tableInfo.ident().indexNameOrAlias(),
                    analysis.ids().get(i),
                    analysis.routingValues().get(i),
                    onDuplicateKeyAssignments,
                    null,
                    null,
                    null,
                    analysis.sourceMaps().get(i));
            }
        }

        return legacyUpsertById;
    }

    public Functions functions() {
        return functions;
    }
}

