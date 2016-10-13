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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.operation.projectors.TopN;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.UpdateConsumer;
import io.crate.planner.fetch.IndexBaseVisitor;
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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    private static final ESLogger LOGGER = Loggers.getLogger(Planner.class);

    private final ConsumingPlanner consumingPlanner;
    private final ClusterService clusterService;
    private final UpdateConsumer updateConsumer;
    private final CopyStatementPlanner copyStatementPlanner;
    private final SelectStatementPlanner selectStatementPlanner;
    private final EvaluatingNormalizer normalizer;


    public static class Context {

        //index, shardId, node
        private Map<String, Map<Integer, String>> shardNodes;

        private final Planner planner;
        private final ClusterService clusterService;
        private final UUID jobId;
        private final ConsumingPlanner consumingPlanner;
        private final EvaluatingNormalizer normalizer;
        private final TransactionContext transactionContext;
        private final int softLimit;
        private final int fetchSize;
        private int executionPhaseId = 0;
        private final Multimap<TableIdent, TableRouting> tableRoutings = HashMultimap.create();
        private ReaderAllocations readerAllocations;
        private HashMultimap<TableIdent, String> tableIndices;
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
            this.handlerNode = clusterService.localNode().id();
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
            Integer limit = toInteger(optLimit.orNull());
            if (limit == null) {
                limit = TopN.NO_LIMIT;
            }
            Integer offset = toInteger(querySpec.offset().or(Literal.ZERO));
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
                querySpec.limit(Optional.<Symbol>of(Literal.of((long) softLimit)));
            }
        }

        public String handlerNode() {
            return handlerNode;
        }

        public static class ReaderAllocations {

            private final TreeMap<Integer, String> readerIndices = new TreeMap<>();
            private final Map<String, IntSet> nodeReaders = new HashMap<>();
            private final TreeMap<String, Integer> bases;
            private final Multimap<TableIdent, String> tableIndices;
            private final Map<String, TableIdent> indicesToIdents;


            ReaderAllocations(TreeMap<String, Integer> bases,
                              Map<String, Map<Integer, String>> shardNodes,
                              Multimap<TableIdent, String> tableIndices) {
                this.bases = bases;
                this.tableIndices = tableIndices;
                this.indicesToIdents = new HashMap<>(tableIndices.values().size());
                for (Map.Entry<TableIdent, String> entry : tableIndices.entries()) {
                    indicesToIdents.put(entry.getValue(), entry.getKey());
                }
                for (Map.Entry<String, Integer> entry : bases.entrySet()) {
                    readerIndices.put(entry.getValue(), entry.getKey());
                }
                for (Map.Entry<String, Map<Integer, String>> entry : shardNodes.entrySet()) {
                    Integer base = bases.get(entry.getKey());
                    if (base == null) {
                        continue;
                    }
                    for (Map.Entry<Integer, String> nodeEntries : entry.getValue().entrySet()) {
                        int readerId = base + nodeEntries.getKey();
                        IntSet readerIds = nodeReaders.get(nodeEntries.getValue());
                        if (readerIds == null) {
                            readerIds = new IntHashSet();
                            nodeReaders.put(nodeEntries.getValue(), readerIds);
                        }
                        readerIds.add(readerId);
                    }
                }
            }

            public Multimap<TableIdent, String> tableIndices() {
                return tableIndices;
            }

            public TreeMap<Integer, String> indices() {
                return readerIndices;
            }

            public Map<String, IntSet> nodeReaders() {
                return nodeReaders;
            }

            public TreeMap<String, Integer> bases() {
                return bases;
            }

            public Map<String, TableIdent> indicesToIdents() {
                return indicesToIdents;
            }
        }

        public ReaderAllocations buildReaderAllocations() {
            if (readerAllocations != null) {
                return readerAllocations;
            }

            IndexBaseVisitor visitor = new IndexBaseVisitor();

            // tableIdent -> indexName
            final Multimap<TableIdent, String> usedTableIndices = HashMultimap.create();
            for (final Map.Entry<TableIdent, Collection<TableRouting>> tableRoutingEntry : tableRoutings.asMap().entrySet()) {
                for (TableRouting tr : tableRoutingEntry.getValue()) {
                    if (!tr.nodesAllocated) {
                        allocateRoutingNodes(tableRoutingEntry.getKey(), tr.routing.locations());
                        tr.nodesAllocated = true;
                    }
                    tr.routing.walkLocations(visitor);
                    tr.routing.walkLocations(new Routing.RoutingLocationVisitor() {
                        @Override
                        public boolean visitNode(String nodeId, Map<String, List<Integer>> nodeRouting) {
                            usedTableIndices.putAll(tableRoutingEntry.getKey(), nodeRouting.keySet());
                            return super.visitNode(nodeId, nodeRouting);
                        }
                    });

                }
            }
            readerAllocations = new ReaderAllocations(visitor.build(), shardNodes, usedTableIndices);
            return readerAllocations;
        }

        public ClusterService clusterService() {
            return clusterService;
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

        private boolean allocateRoutingNodes(TableIdent tableIdent, Map<String, Map<String, List<Integer>>> locations) {
            boolean success = true;
            if (tableIndices == null) {
                tableIndices = HashMultimap.create();
            }
            if (shardNodes == null) {
                shardNodes = new HashMap<>();
            }
            for (Map.Entry<String, Map<String, List<Integer>>> location : locations.entrySet()) {
                for (Map.Entry<String, List<Integer>> indexEntry : location.getValue().entrySet()) {
                    Map<Integer, String> shardsOnIndex = shardNodes.get(indexEntry.getKey());
                    tableIndices.put(tableIdent, indexEntry.getKey());
                    List<Integer> shards = indexEntry.getValue();
                    if (shardsOnIndex == null) {
                        shardsOnIndex = new HashMap<>(shards.size());
                        shardNodes.put(indexEntry.getKey(), shardsOnIndex);
                        for (Integer id : shards) {
                            shardsOnIndex.put(id, location.getKey());
                        }
                    } else {
                        for (Integer id : shards) {
                            String allocatedNodeId = shardsOnIndex.get(id);
                            if (allocatedNodeId != null) {
                                if (!allocatedNodeId.equals(location.getKey())) {
                                    success = false;
                                }
                            } else {
                                shardsOnIndex.put(id, location.getKey());
                            }
                        }
                    }
                }
            }
            return success;
        }

        public Routing allocateRouting(TableInfo tableInfo, WhereClause where, @Nullable String preference) {
            Collection<TableRouting> existingRoutings = tableRoutings.get(tableInfo.ident());
            // allocate routing nodes only if we have more than one table routings
            Routing routing;
            if (existingRoutings.isEmpty()) {
                routing = tableInfo.getRouting(where, preference);
                assert routing != null : tableInfo + " returned empty routing. Routing must not be null";
            } else {
                for (TableRouting existing : existingRoutings) {
                    assert preference == null || preference.equals(existing.preference);
                    if (Objects.equals(existing.where, where)) {
                        return existing.routing;
                    }
                }

                routing = tableInfo.getRouting(where, preference);
                // ensure all routings of this table are allocated
                // and update new routing by merging with existing ones
                for (TableRouting existingRouting : existingRoutings) {
                    if (!existingRouting.nodesAllocated) {
                        allocateRoutingNodes(tableInfo.ident(), existingRouting.routing.locations());
                        existingRouting.nodesAllocated = true;
                    }
                    // Merge locations with existing routing
                    routing.mergeLocations(existingRouting.routing.locations());
                }
                if (!allocateRoutingNodes(tableInfo.ident(), routing.locations())) {
                    throw new UnsupportedOperationException(
                        "Nodes of existing routing are not allocated, routing rebuild needed");
                }
            }
            tableRoutings.put(tableInfo.ident(), new TableRouting(where, preference, routing));
            return routing;
        }
    }

    @VisibleForTesting
    static class TableRouting {
        final WhereClause where;
        final String preference;
        final Routing routing;
        boolean nodesAllocated = false;

        TableRouting(WhereClause where, String preference, Routing routing) {
            this.where = where;
            this.preference = preference;
            this.routing = routing;
        }
    }

    @Inject
    public Planner(ClusterService clusterService,
                   Functions functions,
                   TableStatsService tableStatsService) {
        this.clusterService = clusterService;
        this.updateConsumer = new UpdateConsumer();
        this.consumingPlanner = new ConsumingPlanner(clusterService, functions, tableStatsService);
        this.copyStatementPlanner = new CopyStatementPlanner(clusterService);
        this.selectStatementPlanner = new SelectStatementPlanner(consumingPlanner);
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
        return consumingPlanner.plan(statement, context);
    }

    @Override
    protected Plan visitUpdateStatement(UpdateAnalyzedStatement statement, Context context) {
        ConsumerContext consumerContext = new ConsumerContext(statement, context);
        Plan plan = updateConsumer.consume(statement, consumerContext);
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
    protected Plan visitDDLAnalyzedStatement(AbstractDDLAnalyzedStatement statement, Context context) {
        return new GenericDDLPlan(context.jobId(), statement);
    }

    @Override
    public Plan visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, Context context) {
        if (analysis.noop()) {
            return new NoopPlan(context.jobId());
        }
        return visitDDLAnalyzedStatement(analysis, context);
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
        if (resetStatement.settingsToRemove().isEmpty()) {
            return new NoopPlan(context.jobId());
        }
        return new ESClusterUpdateSettingsPlan(context.jobId(),
            resetStatement.settingsToRemove(), resetStatement.settingsToRemove());
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

