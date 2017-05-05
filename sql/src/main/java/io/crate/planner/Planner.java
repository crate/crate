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
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsPlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.node.management.GenericShowPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.statement.CopyStatementPlanner;
import io.crate.planner.statement.DeleteStatementPlanner;
import io.crate.planner.statement.SetSessionPlan;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SetStatement;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    private static final Logger LOGGER = Loggers.getLogger(Planner.class);

    private final ConsumingPlanner consumingPlanner;
    private final ClusterService clusterService;
    private final CopyStatementPlanner copyStatementPlanner;
    private final SelectStatementPlanner selectStatementPlanner;
    private final EvaluatingNormalizer normalizer;


    public static class Context {

        private final RoutingBuilder routingBuilder = new RoutingBuilder();
        private final Planner planner;
        private final ClusterService clusterService;
        private final UUID jobId;
        private final ConsumingPlanner consumingPlanner;
        private final EvaluatingNormalizer normalizer;
        private final TransactionContext transactionContext;
        private final int softLimit;
        private final int fetchSize;
        private int executionPhaseId = 0;
        private String handlerNode;

        public Context(Planner planner,
                       ClusterService clusterService,
                       UUID jobId,
                       ConsumingPlanner consumingPlanner,
                       EvaluatingNormalizer normalizer,
                       TransactionContext transactionContext,
                       int softLimit,
                       int fetchSize) {
            this.planner = planner;
            this.clusterService = clusterService;
            this.jobId = jobId;
            this.consumingPlanner = consumingPlanner;
            this.normalizer = normalizer;
            this.transactionContext = transactionContext;
            this.softLimit = softLimit;
            this.fetchSize = fetchSize;
            this.handlerNode = clusterService.localNode().getId();
        }

        public EvaluatingNormalizer normalizer() {
            return normalizer;
        }

        @Nullable
        private Integer toInteger(@Nullable Symbol symbol) {
            if (symbol == null) {
                return null;
            }
            Input input = (Input) (normalizer.normalize(symbol, transactionContext));
            return DataTypes.INTEGER.value(input.value());
        }

        public Limits getLimits(QuerySpec querySpec) {
            Optional<Symbol> optLimit = querySpec.limit();
            Integer limit = toInteger(optLimit.orElse(null));
            if (limit == null) {
                limit = TopN.NO_LIMIT;
            }

            Integer offset = toInteger(querySpec.offset().orElse(Literal.ZERO));
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

        public Plan planSingleRowSubselect(AnalyzedStatement statement) {
            UUID subJobId = UUID.randomUUID();
            return planner.process(statement, new Planner.Context(
                planner, clusterService, subJobId, consumingPlanner, normalizer, transactionContext, 2, 2));
        }

        void applySoftLimit(QuerySpec querySpec) {
            if (softLimit != 0 && !querySpec.limit().isPresent()) {
                querySpec.limit(Optional.of(Literal.of((long) softLimit)));
            }
        }

        public String handlerNode() {
            return handlerNode;
        }


        public Plan planSubRelation(AnalyzedRelation relation, ConsumerContext consumerContext) {
            assert consumingPlanner != null : "consumingPlanner needs to be present to plan sub relations";
            return consumingPlanner.plan(relation, consumerContext);
        }

        public UUID jobId() {
            return jobId;
        }

        public int nextExecutionPhaseId() {
            return executionPhaseId++;
        }

        public Routing allocateRouting(TableInfo tableInfo, WhereClause where, String preference) {
            return routingBuilder.allocateRouting(tableInfo, where, preference);
        }

        public ReaderAllocations buildReaderAllocations() {
            return routingBuilder.buildReaderAllocations();
        }
    }


    @Inject
    public Planner(ClusterService clusterService, Functions functions, TableStats tableStats) {
        this.clusterService = clusterService;
        this.consumingPlanner = new ConsumingPlanner(clusterService, functions, tableStats);
        this.copyStatementPlanner = new CopyStatementPlanner(clusterService);
        this.selectStatementPlanner = new SelectStatementPlanner(consumingPlanner, clusterService);
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
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
        return process(analyzedStatement, new Context(this,
            clusterService, jobId, consumingPlanner, normalizer, analysis.transactionContext(), softLimit, fetchSize));
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
        return InsertFromSubQueryPlanner.plan(statement, new ConsumerContext(context));
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
    protected Plan visitShowAnalyzedStatement(AbstractShowAnalyzedStatement statement, Context context) {
        return new GenericShowPlan(context.jobId(), statement);
    }

    @Override
    protected Plan visitDDLStatement(DDLStatement statement, Context context) {
        return new GenericDDLPlan(context.jobId(), statement);
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
        if (SetStatement.Scope.LOCAL.equals(setStatement.scope())) {
            LOGGER.warn("SET LOCAL STATEMENT  WILL BE IGNORED: {}", setStatement.settings());
            return new NoopPlan(context.jobId());
        } else if (SetStatement.Scope.SESSION.equals(setStatement.scope())) {
            return new SetSessionPlan(
                context.jobId(),
                setStatement.settings(),
                context.transactionContext().sessionContext()
            );
        } else if (setStatement.isPersistent()) {
            return new ESClusterUpdateSettingsPlan(context.jobId(), setStatement.settings());
        } else {
            return new ESClusterUpdateSettingsPlan(
                context.jobId(),
                Collections.<String, List<Expression>>emptyMap(),
                setStatement.settings()
            );
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
        UpsertById upsertById = new UpsertById(
            context.jobId(),
            context.nextExecutionPhaseId(),
            analysis.tableInfo().isPartitioned(),
            analysis.numBulkResponses(),
            analysis.bulkIndices(),
            onDuplicateKeyAssignmentsColumns,
            analysis.columns().toArray(new Reference[analysis.columns().size()])
        );
        if (analysis.tableInfo().isPartitioned()) {
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
                    analysis.tableInfo().ident().indexName(),
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

