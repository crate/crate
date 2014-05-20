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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.*;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.LongType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class Planner extends AnalysisVisitor<Void, Plan> {

    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();
    static final PlannerReferenceExtractor referenceExtractor = new PlannerReferenceExtractor();
    private final ClusterService clusterService;

    @Inject
    public Planner(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        return process(analysis, null);
    }

    @Override
    protected Plan visitSelectAnalysis(SelectAnalysis analysis, Void context) {
        Plan plan = new Plan();
        plan.expectsAffectedRows(false);

        if (analysis.hasGroupBy()) {
            groupBy(analysis, plan);
        } else if (analysis.hasAggregates()) {
            globalAggregates(analysis, plan);
        } else {
            WhereClause whereClause = analysis.whereClause();
            if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                    analysis.table().getRouting(whereClause).hasLocations() &&
                    (analysis.table().ident().schema() == null || analysis.table().ident().schema().equals(DocSchemaInfo.NAME))) {

                    if (analysis.ids().size() > 0
                            && analysis.routingValues().size() > 0
                            && !analysis.table().isAlias()) {
                        ESGet(analysis, plan);
                    } else {
                        ESSearch(analysis, plan);
                    }
            } else {
                normalSelect(analysis, plan);
            }
        }
        return plan;
    }

    @Override
    protected Plan visitInsertAnalysis(InsertAnalysis analysis, Void context) {
        Preconditions.checkState(!analysis.sourceMaps().isEmpty(), "no values given");
        Plan plan = new Plan();
        ESIndex(analysis, plan);
        return plan;
    }

    @Override
    protected Plan visitUpdateAnalysis(UpdateAnalysis analysis, Void context) {
        Plan plan = new Plan();
        ESUpdateNode node = new ESUpdateNode(
                indices(analysis),
                analysis.assignments(),
                analysis.whereClause(),
                analysis.ids(),
                analysis.routingValues()
        );
        plan.add(node);
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitDeleteAnalysis(DeleteAnalysis analysis, Void context) {
        Plan plan = new Plan();
        if (analysis.ids().size() == 1 && analysis.routingValues().size() == 1) {
            ESDelete(analysis, plan);
        } else {
            ESDeleteByQuery(analysis, plan);
        }
        return plan;
    }

    @Override
    protected Plan visitCopyAnalysis(final CopyAnalysis analysis, Void context) {
        Plan plan = new Plan();
        if (analysis.mode() == CopyAnalysis.Mode.FROM) {
            copyFromPlan(analysis, plan);
        } else if (analysis.mode() == CopyAnalysis.Mode.TO) {
            copyToPlan(analysis, plan);
        }

        return plan;
    }

    private void copyToPlan(CopyAnalysis analysis, Plan plan) {
        WriterProjection projection = new WriterProjection();
        projection.uri(analysis.uri());
        projection.isDirectoryUri(analysis.directoryUri());
        projection.settings(analysis.settings());

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder();
        if (analysis.outputSymbols() != null && !analysis.outputSymbols().isEmpty()) {
            List<Symbol> columns = new ArrayList<>(analysis.outputSymbols().size());
            for (Symbol symbol : analysis.outputSymbols()) {
                columns.add(DocReferenceBuildingVisitor.convert(symbol));
            }
            contextBuilder = contextBuilder.output(columns);
            projection.inputs(contextBuilder.outputs());
        } else {
            Reference sourceRef;
            if (analysis.table().isPartitioned() && analysis.partitionIdent() == null) {
                // table is partitioned, insert partitioned columns into the output
                sourceRef = new Reference(analysis.table().getColumnInfo(DocSysColumns.DOC));
                Map<ColumnIdent, Symbol> overwrites = new HashMap<>();
                for (ReferenceInfo referenceInfo : analysis.table().partitionedByColumns()) {
                    overwrites.put(referenceInfo.ident().columnIdent(), new Reference(referenceInfo));
                }
                projection.overwrites(overwrites);
            } else {
                sourceRef = new Reference(analysis.table().getColumnInfo(DocSysColumns.RAW));
            }
            contextBuilder = contextBuilder.output(ImmutableList.<Symbol>of(sourceRef));
        }
        CollectNode collectNode = PlanNodeBuilder.collect(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(projection),
                analysis.partitionIdent()
        );
        plan.add(collectNode);
        AggregationProjection aggregationProjection = new AggregationProjection(
                Arrays.asList(new Aggregation(
                                analysis.getFunctionInfo(
                                        new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                                ),
                                Arrays.<Symbol>asList(new InputColumn(0)),
                                Aggregation.Step.ITER,
                                Aggregation.Step.FINAL
                        )
                ));
        MergeNode mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(aggregationProjection), collectNode);
        plan.add(mergeNode);
        plan.expectsAffectedRows(true);
    }

    private void copyFromPlan(CopyAnalysis analysis, Plan plan) {
        int clusteredByPrimaryKeyIdx = analysis.table().primaryKey().indexOf(analysis.table().clusteredBy());

        List<String> partitionedBy;
        String tableName = analysis.table().ident().name();
        if (analysis.partitionIdent() == null) {
            partitionedBy = new ArrayList<>(analysis.table().partitionedBy());
        } else {
            /*
             * if there is a partitionIdent in the analysis this means that the file doesn't include
             * the partition ident in the rows.
             *
             * Therefore there is no need to exclude the partition columns from the source and
             * it is possible to import into the partitioned index directly.
             */
            partitionedBy = Arrays.asList();
            tableName = PartitionName.fromPartitionIdent(tableName, analysis.partitionIdent()).stringValue();
        }
        List<Projection> projections = Arrays.<Projection>asList(new IndexWriterProjection(
                tableName,
                analysis.table().primaryKey(),
                partitionedBy,
                clusteredByPrimaryKeyIdx,
                analysis.settings(),
                null,
                partitionedBy.size() > 0 ? partitionedBy.toArray(new String[partitionedBy.size()]) : null
        ));

        partitionedBy.removeAll(analysis.table().primaryKey());

        int referencesSize = analysis.table().primaryKey().size() + partitionedBy.size() + 1;
        referencesSize = clusteredByPrimaryKeyIdx == -1 ? referencesSize + 1 : referencesSize;
        List<Symbol> toCollect = new ArrayList<>(referencesSize);
        // add primaryKey columns
        for (String primaryKey : analysis.table().primaryKey()) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(new ColumnIdent(primaryKey)))
            );
        }

        // add partitioned columns (if not part of primaryKey)
        for (String partitionedColumn : partitionedBy) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(new ColumnIdent(partitionedColumn)))
            );
        }

        // add clusteredBy column (if not part of primaryKey)
        if (clusteredByPrimaryKeyIdx == -1) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(new ColumnIdent(analysis.table().clusteredBy())))
            );
        }

        // finally add _raw or _doc
        if (analysis.table().isPartitioned() && analysis.partitionIdent() == null) {
            toCollect.add(new Reference(analysis.table().getColumnInfo(DocSysColumns.DOC)));
        } else {
            toCollect.add(new Reference(analysis.table().getColumnInfo(DocSysColumns.RAW)));
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
        AggregationProjection aggregationProjection = new AggregationProjection(
                Arrays.asList(new Aggregation(
                        analysis.getFunctionInfo(
                            new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                        ),
                        Arrays.<Symbol>asList(new InputColumn(0)),
                        Aggregation.Step.ITER,
                        Aggregation.Step.FINAL
                    )
                ));
        plan.add(PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(aggregationProjection), collectNode));
        plan.expectsAffectedRows(true);
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
    protected Plan visitDropTableAnalysis(DropTableAnalysis analysis, Void context) {
        Plan plan = new Plan();

        if (!analysis.table().isPartitioned() || analysis.table().partitions().size() > 0) {
            // delete index always for normal tables
            // and for partitioned tables only if partitions exist
            ESDeleteIndexNode node = new ESDeleteIndexNode(analysis.index());
            plan.add(node);
        }

        if (analysis.table().isPartitioned()) {
            String templateName = PartitionName.templateName(analysis.index());
            ESDeleteTemplateNode templateNode = new ESDeleteTemplateNode(templateName);
            plan.add(templateNode);
        }

        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitCreateTableAnalysis(CreateTableAnalysis analysis, Void context) {
        Plan plan = new Plan();
        TableIdent tableIdent = analysis.tableIdent();
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "a SCHEMA name other than null isn't allowed.");
        if (analysis.isPartitioned()) {
            ESCreateTemplateNode node = new ESCreateTemplateNode(
                    analysis.templateName(),
                    analysis.templatePrefix(),
                    analysis.indexSettings().getByPrefix("index."), // strip 'index' prefix for template api
                    analysis.mapping(),
                    tableIdent.name()
            );
            plan.add(node);
        } else {
            ESCreateIndexNode node = new ESCreateIndexNode(
                    tableIdent.name(),
                    analysis.indexSettings(),
                    analysis.mapping()
            );
            plan.add(node);
        }
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitCreateAnalyzerAnalysis(CreateAnalyzerAnalysis analysis, Void context) {
        Plan plan = new Plan();

        Settings analyzerSettings;
        try {
            analyzerSettings = analysis.buildSettings();
        } catch (IOException ioe) {
            throw new UnhandledServerException("Could not build analyzer Settings", ioe);
        }

        ESClusterUpdateSettingsNode node = new ESClusterUpdateSettingsNode(analyzerSettings);
        plan.add(node);
        plan.expectsAffectedRows(true);
        return plan;
    }

    private void ESDelete(DeleteAnalysis analysis, Plan plan) {
        WhereClause whereClause = analysis.whereClause();
        if (analysis.ids().size() == 1 && analysis.routingValues().size() == 1) {
            plan.add(new ESDeleteNode(
                    indices(analysis)[0],
                    analysis.ids().get(0),
                    analysis.routingValues().get(0),
                    whereClause.version()));
            plan.expectsAffectedRows(true);
        } else {
            // TODO: implement bulk delete task / node
            ESDeleteByQuery(analysis, plan);
        }
    }

    private void ESDeleteByQuery(DeleteAnalysis analysis, Plan plan) {
        String[] indices = indices(analysis);

        if (!analysis.whereClause().hasQuery() && analysis.table().isPartitioned()) {
            if (indices.length == 0) {
                // collect all partitions to drop
                indices = new String[analysis.table().partitions().size()];
                for (int i=0; i < analysis.table().partitions().size(); i++) {
                    indices[i] = analysis.table().partitions().get(i).stringValue();
                }
            }

            // drop indices/tables
            for (int i=0; i < indices.length; i++) {
                ESDeleteIndexNode dropNode = new ESDeleteIndexNode(indices[i], true);
                plan.add(dropNode);
            }
        } else {
            // TODO: if we allow queries like 'partitionColumn=X or column=Y' which is currently
            // forbidden through analysis, we must issue deleteByQuery request in addition
            // to above deleteIndex request(s)
            ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                    indices,
                    analysis.whereClause());
            plan.add(node);
        }
        plan.expectsAffectedRows(true);
    }

    private void ESGet(SelectAnalysis analysis, Plan plan) {
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        String indexName;
        if (analysis.table().isPartitioned()) {
            assert analysis.whereClause().partitions().size() == 1 : "ambiguous partitions for ESGet";
            indexName = analysis.whereClause().partitions().get(0);
        } else {
            indexName = analysis.table().ident().name();
        }

        ESGetNode getNode = new ESGetNode(
                indexName,
                analysis.ids(),
                analysis.routingValues(),
                analysis.table().partitionedByColumns());
        getNode.outputs(contextBuilder.toCollect());
        getNode.outputTypes(extractDataTypes(analysis.outputSymbols()));
        plan.add(getNode);

        // handle sorting, limit and offset
        if (analysis.isSorted() || analysis.limit() != null || analysis.offset() > 0 ) {
            TopNProjection tnp = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    analysis.offset(),
                    contextBuilder.orderBy(),
                    analysis.reverseFlags()
            );
            tnp.outputs(contextBuilder.outputs());
            plan.add(PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), getNode));
        }
    }

    private void normalSelect(SelectAnalysis analysis, Plan plan) {
        // node or shard level normal select

        // TODO: without locations the localMerge node can be removed and the topN projection
        // added to the collectNode.

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        ImmutableList<Projection> projections;
        if (analysis.limit() != null) {
            // if we have an offset we have to get as much docs from every node as we have offset+limit
            // otherwise results will be wrong
            TopNProjection tnp = new TopNProjection(analysis.offset() + analysis.limit(), 0,
                    contextBuilder.orderBy(), analysis.reverseFlags());
            tnp.outputs(contextBuilder.outputs());
            projections = ImmutableList.<Projection>of(tnp);
        } else {
            projections = ImmutableList.of();
        }

        CollectNode collectNode = PlanNodeBuilder.collect(analysis, contextBuilder.toCollect(), projections);
        plan.add(collectNode);


        TopNProjection tnp = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                contextBuilder.orderBy(),
                analysis.reverseFlags()
        );
        tnp.outputs(contextBuilder.outputs());
        plan.add(PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), collectNode));
    }

    private void ESSearch(SelectAnalysis analysis, Plan plan) {
        // this is an es query
        // this only supports INFOS as order by
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .searchOutput(analysis.outputSymbols());
        final Predicate<Symbol> symbolIsReference = new Predicate<Symbol>() {
            @Override
            public boolean apply(@Nullable Symbol input) {
                return input instanceof Reference;
            }
        };

        boolean needsProjection = !Iterables.all(analysis.outputSymbols(), symbolIsReference);
        List<Symbol> searchSymbols;
        if (needsProjection) {
            searchSymbols = contextBuilder.toCollect();
        } else {
            searchSymbols = analysis.outputSymbols();
        }

        List<Reference> orderBy = null;
        if (analysis.isSorted()) {
            orderBy = Lists.transform(analysis.sortSymbols(), new com.google.common.base.Function<Symbol, Reference>() {
                @Nullable
                @Override
                public Reference apply(@Nullable Symbol symbol) {
                    if (!symbolIsReference.apply(symbol)) {
                        throw new IllegalArgumentException(
                                SymbolFormatter.format(
                                        "Unsupported order symbol for ESPlan: %s", symbol));
                    }
                    return (Reference)symbol;
                }
            });
        }
        ESSearchNode node = new ESSearchNode(
                indices(analysis),
                searchSymbols,
                orderBy,
                analysis.reverseFlags(),
                analysis.limit(),
                analysis.offset(),
                analysis.whereClause(),
                analysis.table().partitionedByColumns()
        );
        node.outputTypes(extractDataTypes(searchSymbols));
        plan.add(node);
        // only add projection if we have scalar functions
        if (needsProjection) {
            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    TopN.NO_OFFSET
            );
            topN.outputs(contextBuilder.outputs());
            plan.add(PlanNodeBuilder.localMerge(Arrays.<Projection>asList(topN), node));
        }
    }

    private void globalAggregates(SelectAnalysis analysis, Plan plan) {
        String schema = analysis.table().ident().schema();
        if ((schema == null || schema.equalsIgnoreCase(DocSchemaInfo.NAME))
                && hasOnlyGlobalCount(analysis.outputSymbols())
                && !analysis.hasSysExpressions()
                && !analysis.table().isPartitioned()) {
            plan.add(new ESCountNode(analysis.table().ident().name(), analysis.whereClause()));
            return;
        }

        // global aggregate: collect and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2)
                .output(analysis.outputSymbols());

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(ap)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        //// the hander stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (contextBuilder.aggregationsWrappedInScalar) {
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
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
        return (function.info().isAggregate()
                && function.arguments().size() == 0
                && function.info().ident().name().equalsIgnoreCase(CountAggregation.NAME));
    }

    private void groupBy(SelectAnalysis analysis, Plan plan) {
        if (analysis.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()
                || !analysis.table().getRouting(analysis.whereClause()).hasLocations()) {
            nonDistributedGroupBy(analysis, plan);
        } else {
            distributedGroupBy(analysis, plan);
        }
    }

    private void nonDistributedGroupBy(SelectAnalysis analysis, Plan plan) {
        // TODO:  if the routing is HandlerSideRouting or has no locations
        // the localMergeNode isn't needed but instead the topN projection could be added to the
        // collectNode
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, analysis.groupBy())
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        GroupProjection groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        // handler
        groupProjection =
            new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                contextBuilder.orderBy(),
                analysis.reverseFlags()
        );
        topN.outputs(contextBuilder.outputs());
        plan.add(PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(groupProjection, topN), collectNode));
    }


    /**
     * distributed collect on mapper nodes
     * with merge on reducer to final (they have row authority)
     * <p/>
     * final merge on handler
     */
    private void distributedGroupBy(SelectAnalysis analysis, Plan plan) {
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, analysis.groupBy())
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        // collector
        GroupProjection groupProjection = new GroupProjection(
                contextBuilder.groupBy(), contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                analysis,
                contextBuilder.toCollect(),
                nodesFromTable(analysis),
                ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        // mergeNode for reducer
        ImmutableList.Builder<Projection> projectionsBuilder = ImmutableList.<Projection>builder();
        projectionsBuilder.add(new GroupProjection(
                contextBuilder.groupBy(),
                contextBuilder.aggregations()));

        boolean topNDone = false;
        if (analysis.limit() != null
                || analysis.offset() > 0
                || contextBuilder.aggregationsWrappedInScalar) {
            topNDone = true;

            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.reverseFlags()
            );
            topN.outputs(contextBuilder.outputs());
            projectionsBuilder.add(topN);
        }
        MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, projectionsBuilder.build());
        plan.add(mergeNode);


        List<Symbol> outputs;
        List<Symbol> orderBy;
        if (topNDone) {
            orderBy = contextBuilder.passThroughOrderBy();
            outputs = contextBuilder.passThroughOutputs();
        } else {
            orderBy = contextBuilder.orderBy();
            outputs = contextBuilder.outputs();
        }
        // mergeNode handler
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                orderBy,
                analysis.reverseFlags()
        );
        topN.outputs(outputs);
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);
        plan.add(localMergeNode);
    }

    private List<String> nodesFromTable(SelectAnalysis analysis) {
        return Lists.newArrayList(analysis.table().getRouting(analysis.whereClause()).nodes());
    }

    private void ESIndex(InsertAnalysis analysis, Plan plan) {
        String[] indices = new String[]{analysis.table().ident().name()};
        if (analysis.table().isPartitioned()) {
            indices = analysis.partitions().toArray(new String[analysis.partitions().size()]);
        }
        ESIndexNode indexNode = new ESIndexNode(
                indices,
                analysis.sourceMaps(),
                analysis.ids(),
                analysis.routingValues());
        plan.add(indexNode);
        plan.expectsAffectedRows(true);
    }



    static List<DataType> extractDataTypes(List<Symbol> symbols) {
        List<DataType> types = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            types.add(DataTypeVisitor.fromSymbol(symbol));
        }
        return types;
    }

    static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
        if (projections.size() == 0){
            return inputTypes;
        }
        int projectionIdx = projections.size() - 1;
        Projection lastProjection = projections.get(projectionIdx);
        List<DataType> types = new ArrayList<>(lastProjection.outputs().size());

        List<DataType> dataTypes = Objects.firstNonNull(inputTypes, ImmutableList.<DataType>of());

        for (int c = 0; c < lastProjection.outputs().size(); c++) {
            types.add(resolveType(projections, projectionIdx, c, dataTypes));
        }

        return types;
    }

    private static DataType resolveType(List<Projection> projections, int projectionIdx, int columnIdx, List<DataType> inputTypes) {
        Projection projection = projections.get(projectionIdx);
        Symbol symbol = projection.outputs().get(columnIdx);
        DataType type = DataTypeVisitor.fromSymbol(symbol);
        if (type == null) {
            if (symbol.symbolType() == SymbolType.INPUT_COLUMN) {
                columnIdx = ((InputColumn)symbol).index();
            }
            if (projectionIdx > 0) {
                return resolveType(projections, projectionIdx - 1, columnIdx, inputTypes);
            } else {
                assert symbol instanceof InputColumn; // otherwise type shouldn't be null
                return inputTypes.get(((InputColumn) symbol).index());
            }
        }

        return type;
    }

    private String[] indices(AbstractDataAnalysis analysis) {
        String[] indices;
        if (analysis.whereClause().partitions().size() == 0) {
            indices = new String[]{analysis.table().ident().name()};
        } else {
            indices = analysis.whereClause().partitions().toArray(new String[]{});
        }
        return indices;
    }
}
