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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.validator.SortSymbolValidator;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.ESCountNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.planner.v2.ConsumingPlanner;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.crate.planner.symbol.Field.unwrap;

@Singleton
public class Planner extends AnalyzedStatementVisitor<Planner.Context, Plan> {

    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final RelationPlanner relationPlanner = new RelationPlanner();
    private final ClusterService clusterService;
    private Functions functions;
    private AnalysisMetaData analysisMetaData;
    private AggregationProjection localMergeProjection;

    protected static class Context {
        public final Optional<ColumnIndexWriterProjection> indexWriterProjection;

        Context() {
            this(null);
        }

        Context(@Nullable ColumnIndexWriterProjection indexWriterProjection) {
            this.indexWriterProjection = Optional.fromNullable(indexWriterProjection);
        }
    }

    private static final Context EMPTY_CONTEXT = new Context();

    @Inject
    public Planner(ClusterService clusterService, AnalysisMetaData analysisMetaData) {
        this.clusterService = clusterService;
        this.functions = analysisMetaData.functions();
        this.analysisMetaData = analysisMetaData;
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        assert !analyzedStatement.hasNoResult() : "analysis has no result. we're wrong here";
        return process(analyzedStatement, EMPTY_CONTEXT);
    }

    @Override
    protected Plan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
        throw new UnsupportedOperationException(String.format("AnalyzedStatement \"%s\" not supported.", analyzedStatement));
    }

    @Override
    protected Plan visitSelectStatement(SelectAnalyzedStatement statement, Context context) {
        return relationPlanner.process(statement, context);
    }

    @Override
    protected Plan visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement analysis, Context context) {
        Preconditions.checkState(!analysis.sourceMaps().isEmpty(), "no values given");
        Plan plan = new Plan();
        ESIndex(analysis, plan);
        return plan;
    }

    @Override
    protected Plan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement analysis, Context context) {
        return relationPlanner.process(analysis, context);
    }

    @Override
    protected Plan visitUpdateStatement(UpdateAnalyzedStatement statement, Context context) {
        Plan plan = new Plan();
        assert statement.sourceRelation() instanceof TableRelation : "sourceRelation of update statement must be a TableRelation";
        TableRelation tableRelation = (TableRelation) statement.sourceRelation();
        TableInfo tableInfo = tableRelation.tableInfo();

        for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(nestedAnalysis.whereClause());
            WhereClause whereClause = whereClauseContext.whereClause();

            if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
                ESUpdateNode node = new ESUpdateNode(
                        indices(tableInfo, whereClause),
                        nestedAnalysis.assignments(),
                        whereClause,
                        whereClauseContext.ids(),
                        whereClauseContext.routingValues()
                );
                plan.add(node);
            }
        }
        return plan;
    }

    @Override
    protected Plan visitDeleteStatement(DeleteAnalyzedStatement analyzedStatement, Context context) {
        Plan plan = new Plan();
        TableRelation tableRelation = (TableRelation) analyzedStatement.analyzedRelation();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
        for (WhereClause whereClause : analyzedStatement.whereClauses()) {
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(whereClause);
            if (whereClauseContext.ids().size() == 1 && whereClauseContext.routingValues().size() == 1) {
                ESDelete(tableRelation.tableInfo(), whereClauseContext, plan);
            } else {
                ESDeleteByQuery(tableRelation.tableInfo(), whereClauseContext, plan);
            }
        }
        return plan;
    }

    @Override
    protected Plan visitCopyStatement(final CopyAnalyzedStatement analysis, Context context) {
        Plan plan = new Plan();
        if (analysis.mode() == CopyAnalyzedStatement.Mode.FROM) {
            copyFromPlan(analysis, plan);
        } else if (analysis.mode() == CopyAnalyzedStatement.Mode.TO) {
            copyToPlan(analysis, plan);
        }

        return plan;
    }

    private void copyToPlan(CopyAnalyzedStatement analysis, Plan plan) {
        TableInfo tableInfo = analysis.table();
        WriterProjection projection = new WriterProjection();
        projection.uri(analysis.uri());
        projection.isDirectoryUri(analysis.directoryUri());
        projection.settings(analysis.settings());

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder();
        if (analysis.outputSymbols() != null && !analysis.outputSymbols().isEmpty()) {
            List<Symbol> columns = new ArrayList<>(analysis.outputSymbols().size());
            for (Symbol symbol : analysis.outputSymbols()) {
                columns.add(DocReferenceConverter.convertIfPossible(unwrap(symbol), analysis.table()));
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
                analysis.whereClause(),
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(projection),
                analysis.partitionIdent()
        );
        plan.add(collectNode);
        MergeNode mergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(localMergeProjection()), collectNode);
        plan.add(mergeNode);
    }

    private void copyFromPlan(CopyAnalyzedStatement analysis, Plan plan) {
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
        Plan plan = new Plan();
        plan.add(new GenericDDLNode(statement));
        return plan;
    }

    @Override
    protected Plan visitDropTableStatement(DropTableAnalyzedStatement analysis, Context context) {
        Plan plan = new Plan();
        plan.add(new DropTableNode(analysis.table()));
        return plan;
    }

    @Override
    protected Plan visitCreateTableStatement(CreateTableAnalyzedStatement analysis, Context context) {
        Plan plan = new Plan();
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
        plan.add(createTableNode);
        return plan;
    }

    @Override
    protected Plan visitCreateAnalyzerStatement(CreateAnalyzerAnalyzedStatement analysis, Context context) {
        Plan plan = new Plan();

        Settings analyzerSettings;
        try {
            analyzerSettings = analysis.buildSettings();
        } catch (IOException ioe) {
            throw new UnhandledServerException("Could not build analyzer Settings", ioe);
        }

        ESClusterUpdateSettingsNode node = new ESClusterUpdateSettingsNode(analyzerSettings);
        plan.add(node);
        return plan;
    }

    @Override
    public Plan visitSetStatement(SetAnalyzedStatement analysis, Context context) {
        Plan plan = new Plan();
        ESClusterUpdateSettingsNode node;
        if (analysis.isReset()) {
            // always reset persistent AND transient settings
            node = new ESClusterUpdateSettingsNode(analysis.settingsToRemove(), analysis.settingsToRemove());
        } else {
            if (analysis.isPersistent()) {
                node = new ESClusterUpdateSettingsNode(analysis.settings());
            } else {
                node = new ESClusterUpdateSettingsNode(ImmutableSettings.EMPTY, analysis.settings());
            }
        }
        plan.add(node);
        return plan;
    }

    private void ESDelete(TableInfo tableInfo, WhereClauseContext whereClauseContext, Plan plan) {
        assert whereClauseContext.ids().size() == 1 && whereClauseContext.routingValues().size() == 1;
        plan.add(new ESDeleteNode(
                indices(tableInfo, whereClauseContext.whereClause())[0],
                whereClauseContext.ids().get(0),
                whereClauseContext.routingValues().get(0),
                whereClauseContext.whereClause().version()));
    }

    private void ESDeleteByQuery(TableInfo tableInfo, WhereClauseContext whereClauseContext, Plan plan) {
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
                ESDeleteByQueryNode node = new ESDeleteByQueryNode(indices, whereClause);
                plan.add(node);
            }
        }
    }

    private void normalSelect(SelectAnalyzedStatement analysis,
                              TableInfo tableInfo,
                              WhereClauseContext whereClauseContext,
                              Plan plan,
                              Context context) {
        // node or shard level normal select

        // TODO: without locations the localMerge node can be removed and the topN projection
        // added to the collectNode.

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(unwrap(analysis.outputSymbols()))
                .orderBy(unwrap(analysis.orderBy().orderBySymbols()));

        WhereClause whereClause = whereClauseContext.whereClause();

        ImmutableList<Projection> projections;
        if (analysis.isLimited()) {
            // if we have an offset we have to get as much docs from every node as we have offset+limit
            // otherwise results will be wrong
            TopNProjection tnp = new TopNProjection(
                    analysis.offset() + analysis.limit(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.orderBy().reverseFlags(),
                    analysis.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projections = ImmutableList.<Projection>of(tnp);
        } else if(context.indexWriterProjection.isPresent()) {
            // no limit, projection (index writer) will run on shard/CollectNode
            projections = ImmutableList.<Projection>of(context.indexWriterProjection.get());
        } else {
            projections = ImmutableList.of();
        }

        List<Symbol> toCollect;
        if (tableInfo.schemaInfo().systemSchema()) {
            toCollect = contextBuilder.toCollect();
        } else {
            toCollect = new ArrayList<>();
            for (Symbol symbol : contextBuilder.toCollect()) {
                toCollect.add(DocReferenceConverter.convertIfPossible(symbol, tableInfo));
            }
        }

        CollectNode collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, toCollect, projections);
        plan.add(collectNode);
        ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.builder();

        if (!context.indexWriterProjection.isPresent() || analysis.isLimited()) {
            // limit set, apply topN projection
            TopNProjection tnp = new TopNProjection(
                    firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    analysis.offset(),
                    contextBuilder.orderBy(),
                    analysis.orderBy().reverseFlags(),
                    analysis.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projectionBuilder.add(tnp);
        }
        if (context.indexWriterProjection.isPresent() && analysis.isLimited()) {
            // limit set, context projection (index writer) will run on handler
            projectionBuilder.add(context.indexWriterProjection.get());
        } else if (context.indexWriterProjection.isPresent() && !analysis.isLimited()) {
            // no limit -> no topN projection, use aggregation projection to merge node results
            projectionBuilder.add(localMergeProjection());
        }
        plan.add(PlanNodeBuilder.localMerge(projectionBuilder.build(), collectNode));
    }

    private void globalAggregates(SelectAnalyzedStatement analysis,
                                  TableInfo tableInfo,
                                  WhereClauseContext whereClauseContext,
                                  Plan plan,
                                  Context context) {
        String schema = tableInfo.ident().schema();
        WhereClause whereClause = whereClauseContext.whereClause();

        if ((schema == null || !tableInfo.schemaInfo().systemSchema())
                && hasOnlyGlobalCount(analysis.outputSymbols())
                && !analysis.hasSysExpressions()
                && !context.indexWriterProjection.isPresent()) {
            plan.add(new ESCountNode(indices(tableInfo, whereClauseContext.whereClause()), analysis.whereClause()));
            return;
        }
        // global aggregate: collect and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2).output(unwrap(analysis.outputSymbols()));

        // havingClause could be a Literal or Function.
        // if its a Literal and value is false, we'll never reach this point (no match),
        // otherwise (true value) having can be ignored
        Symbol havingClause = unwrap(analysis.havingClause());
        if (havingClause != null && havingClause.symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(havingClause);
        }

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                whereClause,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(ap)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        //// the handler stuff
        List<Projection> projections = new ArrayList<>();

        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }

        if (contextBuilder.aggregationsWrappedInScalar || havingClause != null) {
            // will filter out optional having symbols which are not selected
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        if (context.indexWriterProjection.isPresent()) {
            projections.add(context.indexWriterProjection.get());
        }
        plan.add(PlanNodeBuilder.localMerge(projections, collectNode));
    }

    private boolean hasOnlyGlobalCount(List<Symbol> symbols) {
        if (symbols.size() != 1) {
            return false;
        }

        Symbol symbol = symbols.get(0);
        if (symbol.symbolType() != SymbolType.FUNCTION) {
            return false;
        }

        Function function = (Function)symbol;
        return (function.info().type() == FunctionInfo.Type.AGGREGATE
                && function.arguments().size() == 0
                && function.info().ident().name().equalsIgnoreCase(CountAggregation.NAME));
    }

    private void ESIndex(InsertFromValuesAnalyzedStatement analysis, Plan plan) {
        String[] indices;
        if (analysis.tableInfo().isPartitioned()) {
            List<String> partitions = analysis.generatePartitions();
            indices = partitions.toArray(new String[partitions.size()]);
        } else {
            indices = new String[]{ analysis.tableInfo().ident().esName() };
        }

        ESIndexNode indexNode = new ESIndexNode(
                indices,
                analysis.sourceMaps(),
                analysis.ids(),
                analysis.routingValues(),
                analysis.tableInfo().isPartitioned(),
                analysis.isBulkRequest()
        );
        plan.add(indexNode);
    }

    static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
        if (projections.size() == 0){
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
            indices = new String[]{ tableInfo.ident().name() };
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

    private class RelationPlanner extends RelationVisitor<Context, Plan> {

        @Override
        public Plan process(AnalyzedRelation relation, @Nullable Context context) {
            /**
             * in case of insert from query the indexWriterProjection is set.
             * New consumingPlanner will handle Insert-From-Query differently and therefore can't handle the
             * indexWriterProjection so if it is set fallback to old planner logic
             */
            if (!context.indexWriterProjection.isPresent()) {
                ConsumingPlanner consumingPlanner = new ConsumingPlanner(analysisMetaData);
                Plan plan = consumingPlanner.plan(relation);
                if (plan != null) {
                    return plan;
                }
            }
            return relation.accept(this, context);
        }

        @Override
        public Plan visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            assert statement.sources().size() == 1 : "more then 1 source is not supported";
            AnalyzedRelation sourceRelation = Iterables.getOnlyElement(statement.sources().entrySet()).getValue();
            assert sourceRelation instanceof TableRelation : "source must be a TableRelation";

            TableRelation tableRelation = (TableRelation) sourceRelation;
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(unwrap(statement.whereClause()));
            TableInfo tableInfo = tableRelation.tableInfo();

            for (Symbol symbol : statement.orderBy().orderBySymbols()) {
                SortSymbolValidator.validate(symbol, tableInfo.partitionedBy());
            }

            if (tableInfo.schemaInfo().systemSchema() && whereClauseContext.whereClause().hasQuery()) {
                ensureNoLuceneOnlyPredicates(whereClauseContext.whereClause().query());
            }
            Plan plan = new Plan();
            if (statement.hasAggregates()) {
                globalAggregates(statement, tableInfo, whereClauseContext, plan, context);
            } else {
                assert !(!context.indexWriterProjection.isPresent()
                        && tableInfo.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                        tableInfo.getRouting(whereClauseContext.whereClause()).hasLocations() &&
                        tableInfo.schemaInfo().name().equals(ReferenceInfos.DEFAULT_SCHEMA_NAME)) : "ConsumingPlanner should have produced a plan for QTF";

                normalSelect(statement, tableInfo, whereClauseContext, plan, context);
            }
            return plan;
        }

        @Override
        public Plan visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            throw new UnsupportedOperationException(String.format("relation \"%s\" can't be planned", relation));
        }
    }

    private void ensureNoLuceneOnlyPredicates(Symbol query) {
        NoPredicateVisitor noPredicateVisitor = new NoPredicateVisitor();
        noPredicateVisitor.process(query, null);
    }

    private static class NoPredicateVisitor extends SymbolVisitor<Void, Void> {
        @Override
        public Void visitFunction(Function symbol, Void context) {
            if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return null;
        }
    }
}

