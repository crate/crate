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

package io.crate.planner;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.UpdateConsumer;
import io.crate.planner.fetch.IndexBaseVisitor;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertByIdNode;
import io.crate.planner.node.management.GenericShowPlan;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.statement.CopyStatementPlanner;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    private final ConsumingPlanner consumingPlanner;
    private final ClusterService clusterService;
    private final UpdateConsumer updateConsumer;
    private final CopyStatementPlanner copyStatementPlanner;

    public static class Context {

        //index, shardId, node
        private Map<String, Map<Integer, String>> shardNodes;

        private final ClusterService clusterService;
        private final UUID jobId;
        private final ConsumingPlanner consumingPlanner;
        private int executionPhaseId = 0;
        private final Multimap<TableIdent, TableRouting> tableRoutings = HashMultimap.create();
        private ReaderAllocations readerAllocations;
        private HashMultimap<TableIdent, String> tableIndices;

        public Context(ClusterService clusterService, UUID jobId, ConsumingPlanner consumingPlanner) {
            this.clusterService = clusterService;
            this.jobId = jobId;
            this.consumingPlanner = consumingPlanner;
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
                    assert base != null;
                    for (Map.Entry<Integer, String> nodeEntries : entry.getValue().entrySet()) {
                        int readerId = base + nodeEntries.getKey();
                        IntSet readerIds = nodeReaders.get(nodeEntries.getValue());
                        if (readerIds == null){
                            readerIds = new IntOpenHashSet();
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

        public PlannedAnalyzedRelation planSubRelation(AnalyzedRelation relation, ConsumerContext consumerContext) {
            assert consumingPlanner != null;
            boolean isRoot = consumerContext.isRoot();
            consumerContext.isRoot(false);
            PlannedAnalyzedRelation subPlan = consumingPlanner.plan(relation, consumerContext);
            consumerContext.isRoot(isRoot);
            return subPlan;
        }

        public UUID jobId() {
            return jobId;
        }

        public int nextExecutionPhaseId() {
            return executionPhaseId++;
        }

        private boolean allocateRoutingNodes(TableIdent tableIdent, Map<String, Map<String, List<Integer>>> locations) {
            boolean success = true;
            if (tableIndices == null){
                tableIndices = HashMultimap.create();
            }
            if (shardNodes == null) {
                shardNodes = new HashMap<>();
            }
            for (Map.Entry<String, Map<String, List<Integer>>> location : locations.entrySet()) {
                for (Map.Entry<String, List<Integer>> nodeRouting : location.getValue().entrySet()) {
                    Map<Integer, String> shardsOnIndex = shardNodes.get(nodeRouting.getKey());
                    tableIndices.put(tableIdent, nodeRouting.getKey());
                    if (shardsOnIndex == null) {
                        shardsOnIndex = new HashMap<>(nodeRouting.getValue().size());
                        shardNodes.put(nodeRouting.getKey(), shardsOnIndex);
                        for (Integer id : nodeRouting.getValue()) {
                            shardsOnIndex.put(id, location.getKey());
                        }
                    } else {
                        for (Integer id : nodeRouting.getValue()) {
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
            } else {
                for (TableRouting existing : existingRoutings) {
                    assert preference == null || preference.equals(existing.preference);
                    if (Objects.equals(existing.where, where)) {
                        return existing.routing;
                    }
                }
                // ensure all routings of this table are allocated
                for (TableRouting existingRouting : existingRoutings) {
                    if (!existingRouting.nodesAllocated) {
                        allocateRoutingNodes(tableInfo.ident(), existingRouting.routing.locations());
                        existingRouting.nodesAllocated = true;
                    }
                }
                routing = tableInfo.getRouting(where, preference);
                if (!allocateRoutingNodes(tableInfo.ident(), routing.locations())) {
                    throw new UnsupportedOperationException(
                            "Nodes of existing routing are not allocated, routing rebuild needed");
                }
            }
            tableRoutings.put(tableInfo.ident(), new TableRouting(where, preference, routing));
            return routing;
        }
    }

    private static class TableRouting {
        final WhereClause where;
        final String preference;
        final Routing routing;
        boolean nodesAllocated = false;

        public TableRouting(WhereClause where, String preference, Routing routing) {
            this.where = where;
            this.preference = preference;
            this.routing = routing;
        }
    }

    @Inject
    public Planner(ClusterService clusterService,
                   ConsumingPlanner consumingPlanner,
                   UpdateConsumer updateConsumer,
                   CopyStatementPlanner copyStatementPlanner) {
        this.clusterService = clusterService;
        this.updateConsumer = updateConsumer;
        this.consumingPlanner = consumingPlanner;
        this.copyStatementPlanner = copyStatementPlanner;
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis, UUID jobId) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        return process(analyzedStatement, new Context(clusterService, jobId, consumingPlanner));
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
        throw new UnsupportedOperationException(String.format("AnalyzedStatement \"%s\" not supported.", analyzedStatement));
    }

    @Override
    protected Plan visitSelectStatement(SelectAnalyzedStatement statement, Context context) {
        return consumingPlanner.plan(statement.relation(), context);
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
        PlannedAnalyzedRelation plannedAnalyzedRelation = updateConsumer.consume(statement, consumerContext);
        if (plannedAnalyzedRelation == null) {
            throw new IllegalArgumentException("Couldn't plan Update statement");
        }
        return plannedAnalyzedRelation.plan();
    }

    @Override
    protected Plan visitDeleteStatement(DeleteAnalyzedStatement analyzedStatement, Context context) {
        IterablePlan plan = new IterablePlan(context.jobId());
        DocTableRelation tableRelation = analyzedStatement.analyzedRelation();
        List<WhereClause> whereClauses = new ArrayList<>(analyzedStatement.whereClauses().size());
        List<DocKeys.DocKey> docKeys = new ArrayList<>(analyzedStatement.whereClauses().size());
        for (WhereClause whereClause : analyzedStatement.whereClauses()) {
            if (whereClause.noMatch()) {
                continue;
            }
            if (whereClause.docKeys().isPresent() && whereClause.docKeys().get().size() == 1) {
                docKeys.add(whereClause.docKeys().get().getOnlyKey());
            } else if (!whereClause.noMatch()) {
                whereClauses.add(whereClause);
            }
        }
        if (!docKeys.isEmpty()) {
            plan.add(new ESDeleteNode(context.nextExecutionPhaseId(), tableRelation.tableInfo(), docKeys));
        } else if (!whereClauses.isEmpty()) {
            createESDeleteByQueryNode(tableRelation.tableInfo(), whereClauses, plan, context);
        }

        if (plan.isEmpty()) {
            return new NoopPlan(context.jobId());
        }
        return plan;
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
    protected Plan visitDDLAnalyzedStatement(AbstractDDLAnalyzedStatement statement, Context context) {
        return new IterablePlan(context.jobId(), new GenericDDLNode(statement));
    }

    @Override
    protected Plan visitShowAnalyzedStatement(AbstractShowAnalyzedStatement statement, Context context) {
        return new GenericShowPlan(context.jobId(), statement);
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
        return new IterablePlan(context.jobId(), new DropTableNode(analysis.table(), analysis.dropIfExists()));
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

        ESClusterUpdateSettingsNode node = new ESClusterUpdateSettingsNode(analyzerSettings);
        return new IterablePlan(context.jobId(), node);
    }

    @Override
    public Plan visitResetAnalyzedStatement(ResetAnalyzedStatement resetStatement, Context context) {
        if (resetStatement.settingsToRemove().isEmpty()) {
            return new NoopPlan(context.jobId());
        }
        return new IterablePlan(context.jobId(), new ESClusterUpdateSettingsNode(
                resetStatement.settingsToRemove(), resetStatement.settingsToRemove()));
    }

    @Override
    public Plan visitSetStatement(SetAnalyzedStatement analysis, Context context) {
        ESClusterUpdateSettingsNode node = null;
        if (analysis.settings() != null) {
            if (analysis.isPersistent()) {
                node = new ESClusterUpdateSettingsNode(analysis.settings());
            } else {
                node = new ESClusterUpdateSettingsNode(ImmutableSettings.EMPTY, analysis.settings());
            }
        }
        return node != null ? new IterablePlan(context.jobId(), node) : new NoopPlan(context.jobId());
    }

    @Override
    public Plan visitKillAnalyzedStatement(KillAnalyzedStatement analysis, Context context) {
        return analysis.jobId().isPresent() ?
                new KillPlan(context.jobId(), analysis.jobId().get()) :
                new KillPlan(context.jobId());
    }

    private void createESDeleteByQueryNode(DocTableInfo tableInfo,
                                           List<WhereClause> whereClauses,
                                           IterablePlan plan,
                                           Context context) {

        List<String[]> indicesList = new ArrayList<>(whereClauses.size());
        for (WhereClause whereClause : whereClauses) {
            String[] indices = indices(tableInfo, whereClause);
            if (indices.length > 0) {
                if (!whereClause.hasQuery() && tableInfo.isPartitioned()) {
                    plan.add(new ESDeletePartitionNode(indices));
                } else {
                    indicesList.add(indices);
                }
            }
        }
        // TODO: if we allow queries like 'partitionColumn=X or column=Y' which is currently
        // forbidden through analysis, we must issue deleteByQuery request in addition
        // to above deleteIndex request(s)
        if (!indicesList.isEmpty()) {
            plan.add(new ESDeleteByQueryNode(context.nextExecutionPhaseId(), indicesList, whereClauses));
        }
    }

    private Upsert processInsertStatement(InsertFromValuesAnalyzedStatement analysis, Context context) {
        String[] onDuplicateKeyAssignmentsColumns = null;
        if (analysis.onDuplicateKeyAssignmentsColumns().size() > 0) {
            onDuplicateKeyAssignmentsColumns = analysis.onDuplicateKeyAssignmentsColumns().get(0);
        }
        UpsertByIdNode upsertByIdNode = new UpsertByIdNode(
                context.nextExecutionPhaseId(),
                analysis.tableInfo().isPartitioned(),
                analysis.isBulkRequest(),
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
                upsertByIdNode.add(
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
                upsertByIdNode.add(
                        analysis.tableInfo().ident().indexName(),
                        analysis.ids().get(i),
                        analysis.routingValues().get(i),
                        onDuplicateKeyAssignments,
                        null,
                        analysis.sourceMaps().get(i));
            }
        }

        return new Upsert(ImmutableList.<Plan>of(new IterablePlan(context.jobId(), upsertByIdNode)), context.jobId());
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

