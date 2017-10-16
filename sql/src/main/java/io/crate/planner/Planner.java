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

import com.google.common.base.Preconditions;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.analyze.CreateAnalyzerAnalyzedStatement;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.analyze.DCLStatement;
import io.crate.analyze.DDLStatement;
import io.crate.analyze.DeleteAnalyzedStatement;
import io.crate.analyze.DropBlobTableAnalyzedStatement;
import io.crate.analyze.DropTableAnalyzedStatement;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.InsertFromValuesAnalyzedStatement;
import io.crate.analyze.KillAnalyzedStatement;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.ResetAnalyzedStatement;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.SetAnalyzedStatement;
import io.crate.analyze.ShowCreateTableAnalyzedStatement;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.dcl.GenericDCLPlan;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.node.management.ShowCreateTablePlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.statement.CopyStatementPlanner;
import io.crate.planner.statement.DeleteStatementPlanner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.sql.tree.Expression;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.crate.analyze.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    private static final Logger LOGGER = Loggers.getLogger(Planner.class);

    private final ClusterService clusterService;
    private final CopyStatementPlanner copyStatementPlanner;
    private final SelectStatementPlanner selectStatementPlanner;
    private final EvaluatingNormalizer normalizer;
    private final LogicalPlanner logicalPlanner;

    private String[] awarenessAttributes;


    public static class Context {

        private final Planner planner;
        private final UUID jobId;
        private final EvaluatingNormalizer normalizer;
        private final TransactionContext transactionContext;
        private final int softLimit;
        private final int fetchSize;
        private final RoutingBuilder routingBuilder;
        private final ClusterState clusterState;
        private final RoutingProvider routingProvider;
        private final LogicalPlanner logicalPlanner;
        private int executionPhaseId = 0;
        private String handlerNode;

        public Context(Planner planner,
                       LogicalPlanner logicalPlanner,
                       ClusterState clusterState,
                       RoutingProvider routingProvider,
                       UUID jobId,
                       EvaluatingNormalizer normalizer,
                       TransactionContext transactionContext,
                       int softLimit,
                       int fetchSize) {
            this.logicalPlanner = logicalPlanner;
            this.routingBuilder = new RoutingBuilder(clusterState, routingProvider);
            this.routingProvider = routingProvider;
            this.clusterState = clusterState;
            this.planner = planner;
            this.jobId = jobId;
            this.normalizer = normalizer;
            this.transactionContext = transactionContext;
            this.softLimit = softLimit;
            this.fetchSize = fetchSize;
            this.handlerNode = clusterState.getNodes().getLocalNodeId();
        }

        public EvaluatingNormalizer normalizer() {
            return normalizer;
        }

        @Nullable
        public Integer toInteger(@Nullable Symbol symbol) {
            if (symbol == null) {
                return null;
            }
            Input input = (Input) (normalizer.normalize(symbol, transactionContext));
            return DataTypes.INTEGER.value(input.value());
        }

        public Limits getLimits(QuerySpec querySpec) {
            Integer limit = toInteger(querySpec.limit());
            if (limit == null) {
                limit = TopN.NO_LIMIT;
            }

            Integer offset = toInteger(querySpec.offset());
            if (offset == null) {
                offset = 0;
            }
            return new Limits(limit, offset);
        }

        public int fetchSize() {
            return fetchSize;
        }

        public TransactionContext transactionContext() {
            return transactionContext;
        }

        Plan planSubselect(AnalyzedStatement statement, SelectSymbol selectSymbol) {
            UUID subJobId = UUID.randomUUID();
            final int softLimit, fetchSize;
            if (selectSymbol.getResultType() == SINGLE_COLUMN_SINGLE_VALUE) {
                softLimit = fetchSize = 2;
            } else {
                softLimit = this.softLimit;
                fetchSize = this.fetchSize;
            }
            return planner.process(
                statement,
                new Planner.Context(
                    planner,
                    logicalPlanner,
                    clusterState,
                    routingProvider,
                    subJobId,
                    normalizer,
                    transactionContext,
                    softLimit,
                    fetchSize)
            );
        }

        void applySoftLimit(QuerySpec querySpec) {
            if (softLimit != 0 && querySpec.limit() == null) {
                querySpec.limit(Literal.of((long) softLimit));
            }
        }

        public String handlerNode() {
            return handlerNode;
        }


        public Plan planSubRelation(QueriedRelation relation, FetchMode fetchMode) {
            assert logicalPlanner != null : "consumingPlanner needs to be present to plan sub relations";
            return logicalPlanner.plan(relation, this, fetchMode);
        }

        public UUID jobId() {
            return jobId;
        }

        public int nextExecutionPhaseId() {
            return executionPhaseId++;
        }

        public Routing allocateRouting(TableInfo tableInfo,
                                       WhereClause where,
                                       RoutingProvider.ShardSelection shardSelection,
                                       SessionContext sessionContext) {
            return routingBuilder.allocateRouting(tableInfo, where, shardSelection, sessionContext);
        }

        public ReaderAllocations buildReaderAllocations() {
            return routingBuilder.buildReaderAllocations();
        }
    }


    @Inject
    public Planner(Settings settings, ClusterService clusterService, Functions functions, TableStats tableStats) {
        this.clusterService = clusterService;
        this.logicalPlanner = new LogicalPlanner(functions, tableStats);
        this.copyStatementPlanner = new CopyStatementPlanner(clusterService);
        this.selectStatementPlanner = new SelectStatementPlanner(logicalPlanner);
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);

        this.awarenessAttributes =
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes);
    }

    private void setAwarenessAttributes(String[] awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis  analysis to create plan from
     * @param softLimit A soft limit will be applied if there is no explicit limit within the query.
     *                  0 for unlimited (query limit or maxRows will still apply)
     *                  If the type of query doesn't have a resultSet this has no effect.
     * @param fetchSize Limit the number of rows that should be returned to a client.
     *                  If > 0 this overrides the limit that might be part of a query.
     *                  0 for unlimited (soft limit or query limit may still apply)
     * @return plan
     */
    public Plan plan(Analysis analysis, UUID jobId, int softLimit, int fetchSize) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), awarenessAttributes);
        return process(
            analyzedStatement,
            new Context(
                this,
                logicalPlanner,
                clusterService.state(),
                routingProvider,
                jobId,
                normalizer,
                analysis.transactionContext(),
                softLimit,
                fetchSize
            )
        );
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
            "Cannot create Plan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
    }

    @Override
    public Plan visitBegin(AnalyzedBegin analyzedBegin, Context context) {
        return new NoopPlan(context.jobId);
    }

    @Override
    protected Plan visitSelectStatement(SelectAnalyzedStatement statement, Context context) {
        return selectStatementPlanner.plan(statement, context);
    }

    @Override
    protected Plan visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement statement, Context context) {
        Preconditions.checkState(!statement.sourceMaps().isEmpty(), "no values given");
        return processInsertStatement(statement, context);
    }

    @Override
    protected Plan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement statement, Context context) {
        return InsertFromSubQueryPlanner.plan(statement, context);
    }

    @Override
    protected Plan visitUpdateStatement(UpdateAnalyzedStatement statement, Context context) {
        Plan plan = UpdatePlanner.plan(statement, context);
        if (plan == null) {
            throw new IllegalArgumentException("Couldn't plan Update statement");
        }
        return plan;
    }

    @Override
    protected Plan visitDeleteStatement(DeleteAnalyzedStatement analyzedStatement, Context context) {
        return DeleteStatementPlanner.planDelete(analyzedStatement, context);
    }

    @Override
    protected Plan visitCopyFromStatement(CopyFromAnalyzedStatement analysis, Context context) {
        return copyStatementPlanner.planCopyFrom(analysis, context);
    }

    @Override
    protected Plan visitCopyToStatement(CopyToAnalyzedStatement analysis, Context context) {
        return copyStatementPlanner.planCopyTo(analysis, context);
    }

    @Override
    public Plan visitShowCreateTableAnalyzedStatement(ShowCreateTableAnalyzedStatement statement, Context context) {
        return new ShowCreateTablePlan(context.jobId(), statement);
    }

    @Override
    protected Plan visitDDLStatement(DDLStatement statement, Context context) {
        return new GenericDDLPlan(context.jobId(), statement);
    }

    @Override
    public Plan visitDCLStatement(DCLStatement statement, Context context) {
        return new GenericDCLPlan(context.jobId(), statement);
    }

    @Override
    public Plan visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, Context context) {
        if (analysis.noop()) {
            return new NoopPlan(context.jobId());
        }
        return visitDDLStatement(analysis, context);
    }

    @Override
    protected Plan visitDropTableStatement(DropTableAnalyzedStatement analysis, Context context) {
        if (analysis.noop()) {
            return new NoopPlan(context.jobId());
        }
        return new DropTablePlan(context.jobId(), analysis.table(), analysis.dropIfExists());
    }

    @Override
    protected Plan visitCreateTableStatement(CreateTableAnalyzedStatement analysis, Context context) {
        if (analysis.noOp()) {
            return new NoopPlan(context.jobId());
        }
        return new GenericDDLPlan(context.jobId(), analysis);
    }

    @Override
    protected Plan visitCreateAnalyzerStatement(CreateAnalyzerAnalyzedStatement analysis, Context context) {
        Settings analyzerSettings;
        try {
            analyzerSettings = analysis.buildSettings();
        } catch (IOException ioe) {
            throw new UnhandledServerException("Could not build analyzer Settings", ioe);
        }
        return new CreateAnalyzerPlan(context.jobId(), analyzerSettings);
    }

    @Override
    public Plan visitResetAnalyzedStatement(ResetAnalyzedStatement resetStatement, Context context) {
        Set<String> settingsToRemove = resetStatement.settingsToRemove();
        if (settingsToRemove.isEmpty()) {
            return new NoopPlan(context.jobId());
        }

        Map<String, List<Expression>> nullSettings = new HashMap<>(settingsToRemove.size(), 1);
        for (String setting : settingsToRemove) {
            nullSettings.put(setting, null);
        }
        return new ESClusterUpdateSettingsPlan(context.jobId(), nullSettings, nullSettings);
    }

    @Override
    public Plan visitSetStatement(SetAnalyzedStatement setStatement, Context context) {
        switch (setStatement.scope()) {
            case LOCAL:
                LOGGER.warn("SET LOCAL STATEMENT  WILL BE IGNORED: {}", setStatement.settings());
                return new NoopPlan(context.jobId());
            case SESSION_TRANSACTION_MODE:
                LOGGER.warn("'SET SESSION CHARACTERISTICS AS TRANSACTION' STATEMENT WILL BE IGNORED");
                return new NoopPlan(context.jobId());
            case SESSION:
                return new SetSessionPlan(
                    context.jobId(),
                    setStatement.settings(),
                    context.transactionContext().sessionContext()
                );
            case GLOBAL:
            default:
                if (setStatement.isPersistent()) {
                    return new ESClusterUpdateSettingsPlan(context.jobId(), setStatement.settings());
                } else {
                    return new ESClusterUpdateSettingsPlan(
                        context.jobId(),
                        Collections.<String, List<Expression>>emptyMap(),
                        setStatement.settings()
                    );
                }
        }
    }

    @Override
    public Plan visitKillAnalyzedStatement(KillAnalyzedStatement analysis, Context context) {
        return analysis.jobId().isPresent() ?
            new KillPlan(context.jobId(), analysis.jobId().get()) :
            new KillPlan(context.jobId());
    }

    @Override
    public Plan visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, Context context) {
        return new ExplainPlan(process(explainAnalyzedStatement.statement(), context));
    }

    private UpsertById processInsertStatement(InsertFromValuesAnalyzedStatement analysis, Context context) {
        String[] onDuplicateKeyAssignmentsColumns = null;
        if (analysis.onDuplicateKeyAssignmentsColumns().size() > 0) {
            onDuplicateKeyAssignmentsColumns = analysis.onDuplicateKeyAssignmentsColumns().get(0);
        }
        DocTableInfo tableInfo = analysis.tableInfo();
        UpsertById upsertById = new UpsertById(
            context.jobId(),
            analysis.numBulkResponses(),
            tableInfo.isPartitioned(),
            analysis.bulkIndices(),
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
                upsertById.add(
                    indices[i],
                    analysis.ids().get(i),
                    analysis.routingValues().get(i),
                    onDuplicateKeyAssignments,
                    null,
                    analysis.sourceMaps().get(i));
            }
        } else {
            for (int i = 0; i < analysis.ids().size(); i++) {
                Symbol[] onDuplicateKeyAssignments = null;
                if (analysis.onDuplicateKeyAssignments().size() > i) {
                    onDuplicateKeyAssignments = analysis.onDuplicateKeyAssignments().get(i);
                }
                upsertById.add(
                    tableInfo.ident().indexName(),
                    analysis.ids().get(i),
                    analysis.routingValues().get(i),
                    onDuplicateKeyAssignments,
                    null,
                    analysis.sourceMaps().get(i));
            }
        }

        return upsertById;
    }

    /**
     * return the ES index names the query should go to
     */
    public static String[] indices(DocTableInfo tableInfo, WhereClause whereClause) {
        String[] indices;

        if (whereClause.noMatch()) {
            indices = org.elasticsearch.common.Strings.EMPTY_ARRAY;
        } else if (!tableInfo.isPartitioned()) {
            // table name for non-partitioned tables
            indices = new String[]{tableInfo.ident().indexName()};
        } else if (whereClause.partitions().isEmpty()) {
            if (whereClause.noMatch()) {
                return new String[0];
            }

            // all partitions
            indices = new String[tableInfo.partitions().size()];
            int i = 0;
            for (PartitionName partitionName : tableInfo.partitions()) {
                indices[i] = partitionName.asIndexName();
                i++;
            }
        } else {
            indices = whereClause.partitions().toArray(new String[whereClause.partitions().size()]);
        }
        return indices;
    }
}

