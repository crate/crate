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

import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.analyze.*;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.SourceIndexWriterProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.firstNonNull;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();

    private final ConsumingPlanner consumingPlanner;
    private final ClusterService clusterService;
    private Functions functions;
    private AnalysisMetaData analysisMetaData;
    private AggregationProjection localMergeProjection;

    protected static class Context {

        private final Analysis analysis;

        Context(Analysis analysis) {
            this.analysis = analysis;
        }

        public Analysis analysis() {
            return analysis;
        }
    }

    @Inject
    public Planner(ClusterService clusterService, AnalysisMetaData analysisMetaData) {
        this.clusterService = clusterService;
        this.functions = analysisMetaData.functions();
        this.analysisMetaData = analysisMetaData;
        this.consumingPlanner = new ConsumingPlanner(analysisMetaData);
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        return process(analyzedStatement, new Context(analysis));
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
        throw new UnsupportedOperationException(String.format("AnalyzedStatement \"%s\" not supported.", analyzedStatement));
    }

    @Override
    protected Plan visitSelectStatement(SelectAnalyzedStatement statement, Context context) {
        return consumingPlanner.plan(statement);
    }

    @Override
    protected Plan visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement analysis, Context context) {
        Preconditions.checkState(!analysis.sourceMaps().isEmpty(), "no values given");
        return new IterablePlan(createESIndexNode(analysis));
    }

    @Override
    protected Plan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement analysis, Context context) {
        return consumingPlanner.plan(analysis);
    }

    @Override
    protected Plan visitUpdateStatement(UpdateAnalyzedStatement statement, Context context) {
        return consumingPlanner.plan(statement);
    }

    @Override
    protected Plan visitDeleteStatement(DeleteAnalyzedStatement analyzedStatement, Context context) {
        IterablePlan plan = new IterablePlan();
        TableRelation tableRelation = (TableRelation) analyzedStatement.analyzedRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
        for (WhereClause whereClause : analyzedStatement.whereClauses()) {
            if (whereClause.noMatch()){
                continue;
            }
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(whereClause);
            if (whereClauseContext.ids().size() == 1 && whereClauseContext.routingValues().size() == 1) {
                createESDeleteNode(tableRelation.tableInfo(), whereClauseContext, plan);
            } else {
                createESDeleteByQueryNode(tableRelation.tableInfo(), whereClauseContext, plan);
            }
        }
        if (plan.isEmpty()) {
            return NoopPlan.INSTANCE;
        }
        return plan;
    }

    @Override
    protected Plan visitCopyStatement(final CopyAnalyzedStatement analysis, Context context) {
        IterablePlan plan = new IterablePlan();
        if (analysis.mode() == CopyAnalyzedStatement.Mode.FROM) {
            copyFromPlan(analysis, plan);
        } else if (analysis.mode() == CopyAnalyzedStatement.Mode.TO) {
            copyToPlan(analysis, plan);
        }

        return plan;
    }

    private void copyToPlan(CopyAnalyzedStatement analysis, IterablePlan plan) {
        TableInfo tableInfo = analysis.table();
        WriterProjection projection = new WriterProjection();
        projection.uri(analysis.uri());
        projection.isDirectoryUri(analysis.directoryUri());
        projection.settings(analysis.settings());

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder();
        if (analysis.selectedColumns() != null && !analysis.selectedColumns().isEmpty()) {
            List<Symbol> columns = new ArrayList<>(analysis.selectedColumns().size());
            for (Symbol symbol : analysis.selectedColumns()) {
                columns.add(DocReferenceConverter.convertIfPossible(symbol, analysis.table()));
            }
            contextBuilder = contextBuilder.output(columns);
            projection.inputs(contextBuilder.outputs());
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
            contextBuilder = contextBuilder.output(ImmutableList.<Symbol>of(sourceRef));
        }
        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                WhereClause.MATCH_ALL,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(projection),
                analysis.partitionIdent()
        );
        plan.add(collectNode);
        MergeNode mergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(localMergeProjection()), collectNode);
        plan.add(mergeNode);
    }

    private void copyFromPlan(CopyAnalyzedStatement analysis, IterablePlan plan) {
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
        List<ColumnIdent> partitionByColumns;
        String tableName;

        if (analysis.partitionIdent() == null) {
            tableName = table.ident().esName();
            if (table.isPartitioned()) {
                partitionedByNames = Lists.newArrayList(
                        Lists.transform(table.partitionedBy(), ColumnIdent.GET_FQN_NAME_FUNCTION));
                partitionByColumns = table.partitionedBy();
            } else {
                partitionedByNames = Collections.emptyList();
                partitionByColumns = Collections.emptyList();
            }
        } else {
            assert table.isPartitioned() : "table must be partitioned if partitionIdent is set";
            // partitionIdent is present -> possible to index raw source into concrete es index
            tableName = PartitionName.fromPartitionIdent(table.ident().schema(), table.ident().name(), analysis.partitionIdent()).stringValue();
            partitionedByNames = Collections.emptyList();
            partitionByColumns = Collections.emptyList();
        }

        SourceIndexWriterProjection sourceIndexWriterProjection = new SourceIndexWriterProjection(
                tableName,
                table.primaryKey(),
                partitionByColumns,
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
                "copyFrom",
                generateRouting(allNodes, analysis.settings().getAsInt("num_readers", allNodes.getSize())),
                analysis.uri(),
                toCollect,
                projections,
                analysis.settings().get("compression", null),
                analysis.settings().getAsBoolean("shared", null)
        );
        PlanNodeBuilder.setOutputTypes(collectNode);
        plan.add(collectNode);
        plan.add(PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(localMergeProjection()), collectNode));
    }

    private Routing generateRouting(DiscoveryNodes allNodes, int maxNodes) {
        final AtomicInteger counter = new AtomicInteger(maxNodes);
        final Map<String, Map<String, Set<Integer>>> locations = new HashMap<>();
        allNodes.dataNodes().keys().forEach(new ObjectProcedure<String>() {
            @Override
            public void apply(String value) {
                if (counter.getAndDecrement() > 0) {
                    locations.put(value, ImmutableMap.<String, Set<Integer>>of());
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
    protected Plan visitDropTableStatement(DropTableAnalyzedStatement analysis, Context context) {
        return new IterablePlan(new DropTableNode(analysis.table()));
    }

    @Override
    protected Plan visitCreateTableStatement(CreateTableAnalyzedStatement analysis, Context context) {
        TableIdent tableIdent = analysis.tableIdent();

        CreateTableNode createTableNode;
        if (analysis.isPartitioned()) {
            createTableNode = CreateTableNode.createPartitionedTableNode(
                    tableIdent,
                    analysis.tableParameter().settings().getByPrefix("index."),
                    analysis.mapping(),
                    analysis.templateName(),
                    analysis.templatePrefix()
            );
        } else {
            createTableNode = CreateTableNode.createTableNode(
                    tableIdent,
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

    private void createESDeleteNode(TableInfo tableInfo, WhereClauseContext whereClauseContext, IterablePlan plan) {
        assert whereClauseContext.ids().size() == 1 && whereClauseContext.routingValues().size() == 1;
        plan.add(new ESDeleteNode(
                indices(tableInfo, whereClauseContext.whereClause())[0],
                whereClauseContext.ids().get(0),
                whereClauseContext.routingValues().get(0),
                whereClauseContext.whereClause().version()));
    }

    @Nullable
    private void createESDeleteByQueryNode(TableInfo tableInfo, WhereClauseContext whereClauseContext, IterablePlan plan) {
        WhereClause whereClause = whereClauseContext.whereClause();

        String[] indices = indices(tableInfo, whereClause);
        if (indices.length > 0 && !whereClause.noMatch()) {
            if (!whereClause.hasQuery() && tableInfo.isPartitioned()) {
                for (String index : indices) {
                    plan.add(new ESDeleteIndexNode(index, true));
                }
            } else {
                // TODO: if we allow queries like 'partitionColumn=X or column=Y' which is currently
                // forbidden through analysis, we must issue deleteByQuery request in addition
                // to above deleteIndex request(s)
                plan.add(new ESDeleteByQueryNode(indices, whereClause));
            }
        }
    }

    private ESIndexNode createESIndexNode(InsertFromValuesAnalyzedStatement analysis) {
        String[] indices;
        if (analysis.tableInfo().isPartitioned()) {
            List<String> partitions = analysis.generatePartitions();
            indices = partitions.toArray(new String[partitions.size()]);
        } else {
            indices = new String[]{analysis.tableInfo().ident().esName()};
        }

        ESIndexNode indexNode = new ESIndexNode(
                indices,
                analysis.sourceMaps(),
                analysis.ids(),
                analysis.routingValues(),
                analysis.tableInfo().isPartitioned(),
                analysis.isBulkRequest()
        );
        return indexNode;
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
            indices = new String[]{tableInfo.ident().name()};
        } else if (whereClause.partitions().isEmpty()) {
            if (whereClause.noMatch()) {
                return new String[0];
            }

            // all partitions
            indices = new String[tableInfo.partitions().size()];
            for (int i = 0; i < tableInfo.partitions().size(); i++) {
                indices[i] = tableInfo.partitions().get(i).stringValue();
            }
        } else {
            indices = whereClause.partitions().toArray(new String[whereClause.partitions().size()]);
        }
        return indices;
    }

    private AggregationProjection localMergeProjection() {
        if (localMergeProjection == null) {
            localMergeProjection = new AggregationProjection(
                    Arrays.asList(new Aggregation(
                                    functions.getSafe(
                                            new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                                    ).info(),
                                    Arrays.<Symbol>asList(new InputColumn(0, DataTypes.LONG)),
                                    Aggregation.Step.ITER,
                                    Aggregation.Step.FINAL
                            )
                    )
            );
        }
        return localMergeProjection;
    }
}

