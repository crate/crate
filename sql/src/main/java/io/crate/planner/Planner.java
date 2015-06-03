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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.*;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.UpdateConsumer;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SourceIndexWriterProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.firstNonNull;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    private final ConsumingPlanner consumingPlanner;
    private final ClusterService clusterService;
    private UpdateConsumer updateConsumer;

    public static class Context {

        private final IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard = new IntObjectOpenHashMap<>();
        private final IntObjectOpenHashMap<String> jobSearchContextIdToNode = new IntObjectOpenHashMap<>();
        private final IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId = new IntObjectOpenHashMap<>();
        private final ClusterService clusterService;
        private int jobSearchContextIdBaseSeq = 0;
        private int executionNodeId = 0;

        public Context(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        public ClusterService clusterService() {
            return clusterService;
        }

        /**
         * Increase current {@link #jobSearchContextIdBaseSeq} by number of shards affected by given
         * <code>routing</code> parameter and register a {@link org.elasticsearch.index.shard.ShardId}
         * under each incremented jobSearchContextId.
         * The current {@link #jobSearchContextIdBaseSeq} is set on the {@link io.crate.metadata.Routing} instance,
         * in order to be able to re-generate jobSearchContextId's for every shard in a deterministic way.
         *
         * Skip generating jobSearchContextId's if {@link io.crate.metadata.Routing#jobSearchContextIdBase} is already
         * set on the given <code>routing</code>.
         */
        public void allocateJobSearchContextIds(Routing routing) {
            if (routing.jobSearchContextIdBase() > -1 || routing.hasLocations() == false
                    || routing.numShards() == 0) {
                return;
            }
            int jobSearchContextId = jobSearchContextIdBaseSeq;
            jobSearchContextIdBaseSeq += routing.numShards();
            routing.jobSearchContextIdBase(jobSearchContextId);
            for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : routing.locations().entrySet()) {
                String nodeId = nodeEntry.getKey();
                Map<String, List<Integer>> nodeRouting = nodeEntry.getValue();
                if (nodeRouting != null) {
                    for (Map.Entry<String, List<Integer>> entry : nodeRouting.entrySet()) {
                        for (Integer shardId : entry.getValue()) {
                            jobSearchContextIdToShard.put(jobSearchContextId, new ShardId(entry.getKey(), shardId));
                            jobSearchContextIdToNode.put(jobSearchContextId, nodeId);
                            jobSearchContextIdToExecutionNodeId.put(jobSearchContextId, executionNodeId);
                            jobSearchContextId++;
                        }
                    }
                }
            }
        }

        @Nullable
        public ShardId shardId(int jobSearchContextId) {
            return jobSearchContextIdToShard.get(jobSearchContextId);
        }

        public IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId() {
            return jobSearchContextIdToExecutionNodeId;
        }

        public IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard() {
            return jobSearchContextIdToShard;
        }

        @Nullable
        public String nodeId(int jobSearchContextId) {
            return jobSearchContextIdToNode.get(jobSearchContextId);
        }

        public IntObjectOpenHashMap<String> jobSearchContextIdToNode() {
            return jobSearchContextIdToNode;
        }

        public int nextExecutionNodeId() {
            return executionNodeId++;
        }
    }

    @Inject
    public Planner(ClusterService clusterService, ConsumingPlanner consumingPlanner, UpdateConsumer updateConsumer) {
        this.clusterService = clusterService;
        this.updateConsumer = updateConsumer;
        this.consumingPlanner = consumingPlanner;
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        return process(analyzedStatement, new Context(clusterService));
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
        if (updateConsumer.consume(statement, consumerContext)) {
            return ((PlannedAnalyzedRelation) consumerContext.rootRelation()).plan();
        }
        throw new IllegalArgumentException("Couldn't plan Update statement");
    }

    @Override
    protected Plan visitDeleteStatement(DeleteAnalyzedStatement analyzedStatement, Context context) {
        IterablePlan plan = new IterablePlan();
        TableRelation tableRelation = analyzedStatement.analyzedRelation();
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
            plan.add(new ESDeleteNode(context.nextExecutionNodeId(), tableRelation.tableInfo(), docKeys));
        } else if (!whereClauses.isEmpty()) {
            createESDeleteByQueryNode(tableRelation.tableInfo(), whereClauses, plan, context);
        }

        if (plan.isEmpty()) {
            return NoopPlan.INSTANCE;
        }
        return plan;
    }

    @Override
    protected Plan visitCopyStatement(final CopyAnalyzedStatement analysis, Context context) {
        switch (analysis.mode()) {
            case FROM:
                return copyFromPlan(analysis, context);
            case TO:
                return copyToPlan(analysis, context);
            default:
                throw new UnsupportedOperationException("mode not supported: " + analysis.mode());
        }
    }

    private Plan copyToPlan(CopyAnalyzedStatement analysis, Context context) {
        TableInfo tableInfo = analysis.table();
        WriterProjection projection = new WriterProjection();
        projection.uri(analysis.uri());
        projection.isDirectoryUri(analysis.directoryUri());
        projection.settings(analysis.settings());

        List<Symbol> outputs;
        if (analysis.selectedColumns() != null && !analysis.selectedColumns().isEmpty()) {
            outputs = new ArrayList<>(analysis.selectedColumns().size());
            List<Symbol> columnSymbols = new ArrayList<>(analysis.selectedColumns().size());
            for (int i = 0; i < analysis.selectedColumns().size(); i++) {
                outputs.add(DocReferenceConverter.convertIfPossible(analysis.selectedColumns().get(i), analysis.table()));
                columnSymbols.add(new InputColumn(i, null));
            }
            projection.inputs(columnSymbols);
        } else {
            Reference sourceRef;
            if (analysis.table().isPartitioned() && analysis.partitionIdent() == null) {
                // table is partitioned, insert partitioned columns into the output
                sourceRef = new Reference(analysis.table().getReferenceInfo(DocSysColumns.DOC));
                Map<ColumnIdent, Symbol> overwrites = new HashMap<>();
                for (ReferenceInfo referenceInfo : analysis.table().partitionedByColumns()) {
                    overwrites.put(referenceInfo.ident().columnIdent(), new Reference(referenceInfo));
                }
                projection.overwrites(overwrites);
            } else {
                sourceRef = new Reference(analysis.table().getReferenceInfo(DocSysColumns.RAW));
            }
            outputs = ImmutableList.<Symbol>of(sourceRef);
        }
        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                context,
                WhereClause.MATCH_ALL,
                outputs,
                ImmutableList.<Projection>of(projection),
                analysis.partitionIdent()
        );

        MergeNode mergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION), collectNode, context);
        return new CollectAndMerge(collectNode, mergeNode);
    }

    private Plan copyFromPlan(CopyAnalyzedStatement analysis, Context context) {
        /**
         * copy from has two "modes":
         *
         * 1: non-partitioned tables or partitioned tables with partition ident --> import into single es index
         *    -> collect raw source and import as is
         *
         * 2: partitioned table without partition ident
         *    -> collect document and partition by values
         *    -> exclude partitioned by columns from document
         *    -> insert into es index (partition determined by partition by value)
         */

        TableInfo table = analysis.table();
        int clusteredByPrimaryKeyIdx = table.primaryKey().indexOf(analysis.table().clusteredBy());
        List<String> partitionedByNames;
        String partitionIdent = null;

        List<BytesRef> partitionValues;
        if (analysis.partitionIdent() == null) {

            if (table.isPartitioned()) {
                partitionedByNames = Lists.newArrayList(
                        Lists.transform(table.partitionedBy(), ColumnIdent.GET_FQN_NAME_FUNCTION));
            } else {
                partitionedByNames = Collections.emptyList();
            }
            partitionValues = ImmutableList.of();
        } else {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index
            PartitionName partitionName = PartitionName.fromPartitionIdent(table.ident().schema(), table.ident().name(), analysis.partitionIdent());
            partitionValues = partitionName.values();

            partitionIdent = partitionName.ident();
            partitionedByNames = Collections.emptyList();
        }

        SourceIndexWriterProjection sourceIndexWriterProjection = new SourceIndexWriterProjection(
                table.ident(),
                partitionIdent,
                new Reference(table.getReferenceInfo(DocSysColumns.RAW)),
                table.primaryKey(),
                table.partitionedBy(),
                partitionValues,
                table.clusteredBy(),
                clusteredByPrimaryKeyIdx,
                analysis.settings(),
                null,
                partitionedByNames.size() > 0 ? partitionedByNames.toArray(new String[partitionedByNames.size()]) : null,
                table.isPartitioned() // autoCreateIndices
        );
        List<Projection> projections = Arrays.<Projection>asList(sourceIndexWriterProjection);
        partitionedByNames.removeAll(Lists.transform(table.primaryKey(), ColumnIdent.GET_FQN_NAME_FUNCTION));
        int referencesSize = table.primaryKey().size() + partitionedByNames.size() + 1;
        referencesSize = clusteredByPrimaryKeyIdx == -1 ? referencesSize + 1 : referencesSize;

        List<Symbol> toCollect = new ArrayList<>(referencesSize);
        // add primaryKey columns
        for (ColumnIdent primaryKey : table.primaryKey()) {
            toCollect.add(new Reference(table.getReferenceInfo(primaryKey)));
        }

        // add partitioned columns (if not part of primaryKey)
        for (String partitionedColumn : partitionedByNames) {
            toCollect.add(
                    new Reference(table.getReferenceInfo(ColumnIdent.fromPath(partitionedColumn)))
            );
        }
        // add clusteredBy column (if not part of primaryKey)
        if (clusteredByPrimaryKeyIdx == -1) {
            toCollect.add(
                    new Reference(table.getReferenceInfo(table.clusteredBy())));
        }
        // finally add _raw or _doc
        if (table.isPartitioned() && analysis.partitionIdent() == null) {
            toCollect.add(new Reference(table.getReferenceInfo(DocSysColumns.DOC)));
        } else {
            toCollect.add(new Reference(table.getReferenceInfo(DocSysColumns.RAW)));
        }

        DiscoveryNodes allNodes = clusterService.state().nodes();
        FileUriCollectNode collectNode = new FileUriCollectNode(
                context.nextExecutionNodeId(),
                "copyFrom",
                generateRouting(allNodes, analysis.settings().getAsInt("num_readers", allNodes.getSize())),
                analysis.uri(),
                toCollect,
                projections,
                analysis.settings().get("compression", null),
                analysis.settings().getAsBoolean("shared", null)
        );
        PlanNodeBuilder.setOutputTypes(collectNode);

        return new CollectAndMerge(collectNode, PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION), collectNode, context));
    }

    private Routing generateRouting(DiscoveryNodes allNodes, int maxNodes) {
        final AtomicInteger counter = new AtomicInteger(maxNodes);
        final Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        allNodes.dataNodes().keys().forEach(new ObjectProcedure<String>() {
            @Override
            public void apply(String value) {
                if (counter.getAndDecrement() > 0) {
                    locations.put(value, TreeMapBuilder.<String, List<Integer>>newMapBuilder().map());
                }
            }
        });
        return new Routing(locations);
    }

    @Override
    protected Plan visitDDLAnalyzedStatement(AbstractDDLAnalyzedStatement statement, Context context) {
        return new IterablePlan(new GenericDDLNode(statement));
    }

    @Override
    public Plan visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, Context context) {
        if (analysis.noop()) {
            return NoopPlan.INSTANCE;
        }
        return visitDDLAnalyzedStatement(analysis, context);
    }

    @Override
    protected Plan visitDropTableStatement(DropTableAnalyzedStatement analysis, Context context) {
        if (analysis.noop()) {
            return NoopPlan.INSTANCE;
        }
        return new IterablePlan(new DropTableNode(analysis.table()));
    }

    @Override
    protected Plan visitCreateTableStatement(CreateTableAnalyzedStatement analysis, Context context) {
        if (analysis.noOp()) {
            return NoopPlan.INSTANCE;
        }
        TableIdent tableIdent = analysis.tableIdent();

        CreateTableNode createTableNode;
        if (analysis.isPartitioned()) {
            createTableNode = CreateTableNode.createPartitionedTableNode(
                    tableIdent,
                    analysis.ifNotExists(),
                    analysis.tableParameter().settings().getByPrefix("index."),
                    analysis.mapping(),
                    analysis.templateName(),
                    analysis.templatePrefix()
            );
        } else {
            createTableNode = CreateTableNode.createTableNode(
                    tableIdent,
                    analysis.ifNotExists(),
                    analysis.tableParameter().settings(),
                    analysis.mapping()
            );
        }
        return new IterablePlan(createTableNode);
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
        return new IterablePlan(node);
    }

    @Override
    public Plan visitSetStatement(SetAnalyzedStatement analysis, Context context) {
        ESClusterUpdateSettingsNode node = null;
        if (analysis.isReset()) {
            // always reset persistent AND transient settings
            if (analysis.settingsToRemove() != null) {
                node = new ESClusterUpdateSettingsNode(analysis.settingsToRemove(), analysis.settingsToRemove());
            }
        } else {
            if (analysis.settings() != null) {
                if (analysis.isPersistent()) {
                    node = new ESClusterUpdateSettingsNode(analysis.settings());
                } else {
                    node = new ESClusterUpdateSettingsNode(ImmutableSettings.EMPTY, analysis.settings());
                }
            }
        }
        return node != null ? new IterablePlan(node) : NoopPlan.INSTANCE;
    }

    @Override
    public Plan visitKillAnalyzedStatement(KillAnalyzedStatement analysis, Context context) {
        return KillPlan.INSTANCE;
    }

    private void createESDeleteByQueryNode(TableInfo tableInfo,
                                           List<WhereClause> whereClauses,
                                           IterablePlan plan,
                                           Context context) {

        List<String[]> indicesList = new ArrayList<>(whereClauses.size());
        for (WhereClause whereClause : whereClauses) {
            String[] indices = indices(tableInfo, whereClauses.get(0));
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
            plan.add(new ESDeleteByQueryNode(context.nextExecutionNodeId(), indicesList, whereClauses));
        }
    }

    private Upsert processInsertStatement(InsertFromValuesAnalyzedStatement analysis, Context context) {
        String[] onDuplicateKeyAssignmentsColumns = null;
        if (analysis.onDuplicateKeyAssignmentsColumns().size() > 0) {
            onDuplicateKeyAssignmentsColumns = analysis.onDuplicateKeyAssignmentsColumns().get(0);
        }
        SymbolBasedUpsertByIdNode upsertByIdNode = new SymbolBasedUpsertByIdNode(
                context.nextExecutionNodeId(),
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
                        analysis.tableInfo().ident().esName(),
                        analysis.ids().get(i),
                        analysis.routingValues().get(i),
                        onDuplicateKeyAssignments,
                        null,
                        analysis.sourceMaps().get(i));
            }
        }

        return new Upsert(ImmutableList.<Plan>of(new IterablePlan(upsertByIdNode)));
    }

    static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
        if (projections.size() == 0) {
            return inputTypes;
        }
        int projectionIdx = projections.size() - 1;
        Projection lastProjection = projections.get(projectionIdx);
        List<DataType> types = new ArrayList<>(lastProjection.outputs().size());
        List<DataType> dataTypes = firstNonNull(inputTypes, ImmutableList.<DataType>of());

        for (int c = 0; c < lastProjection.outputs().size(); c++) {
            types.add(resolveType(projections, projectionIdx, c, dataTypes));
        }
        return types;
    }

    private static DataType resolveType(List<Projection> projections, int projectionIdx, int columnIdx, List<DataType> inputTypes) {
        Projection projection = projections.get(projectionIdx);
        Symbol symbol = projection.outputs().get(columnIdx);
        DataType type = symbol.valueType();
        if (type == null || (type.equals(DataTypes.UNDEFINED) && symbol instanceof InputColumn)) {
            if (projectionIdx > 0) {
                if (symbol instanceof InputColumn) {
                    columnIdx = ((InputColumn) symbol).index();
                }
                return resolveType(projections, projectionIdx - 1, columnIdx, inputTypes);
            } else {
                assert symbol instanceof InputColumn; // otherwise type shouldn't be null
                return inputTypes.get(((InputColumn) symbol).index());
            }
        }

        return type;
    }


    /**
     * return the ES index names the query should go to
     */
    public static String[] indices(TableInfo tableInfo, WhereClause whereClause) {
        String[] indices;

        if (whereClause.noMatch()) {
            indices = org.elasticsearch.common.Strings.EMPTY_ARRAY;
        } else if (!tableInfo.isPartitioned()) {
            // table name for non-partitioned tables
            indices = new String[]{tableInfo.ident().esName()};
        } else if (whereClause.partitions().isEmpty()) {
            if (whereClause.noMatch()) {
                return new String[0];
            }

            // all partitions
            indices = new String[tableInfo.partitions().size()];
            int i = 0;
            for (PartitionName partitionName: tableInfo.partitions()) {
                indices[i] = partitionName.stringValue();
                i++;
            }
        } else {
            indices = whereClause.partitions().toArray(new String[whereClause.partitions().size()]);
        }
        return indices;
    }
}

