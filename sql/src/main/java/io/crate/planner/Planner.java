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
import com.google.common.base.*;
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
import io.crate.planner.symbol.Function;
import io.crate.types.DataType;
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

@Singleton
public class Planner extends AnalysisVisitor<Planner.Context, Plan> {

    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();
    static final PlannerReferenceExtractor referenceExtractor = new PlannerReferenceExtractor();
    private final ClusterService clusterService;

    protected static class Context {
        public final Optional<Projection> projection;

        Context() {
            this(null);
        }

        Context(@Nullable Projection optionalProjection) {
            this.projection = Optional.fromNullable(optionalProjection);
        }
    }

    private static final Context EMPTY_CONTEXT = new Context();

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
        return process(analysis, EMPTY_CONTEXT);
    }

    @Override
    protected Plan visitSelectAnalysis(SelectAnalysis analysis, Context context) {
        Plan plan = new Plan();
        plan.expectsAffectedRows(false);

        if (analysis.hasGroupBy()) {
            groupBy(analysis, plan, context);
        } else if (analysis.hasAggregates()) {
            globalAggregates(analysis, plan, context);
        } else {
            WhereClause whereClause = analysis.whereClause();
            if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                    analysis.table().getRouting(whereClause).hasLocations() &&
                    (analysis.table().ident().schema() == null || analysis.table().ident().schema().equals(DocSchemaInfo.NAME))) {

                    if (analysis.ids().size() > 0
                            && analysis.routingValues().size() > 0
                            && !analysis.table().isAlias()) {
                        ESGet(analysis, plan, context);
                    } else {
                        ESSearch(analysis, plan, context);
                    }
            } else {
                normalSelect(analysis, plan, context);
            }
        }
        return plan;
    }

    @Override
    protected Plan visitInsertFromValuesAnalysis(InsertFromValuesAnalysis analysis, Context context) {
        Preconditions.checkState(!analysis.sourceMaps().isEmpty(), "no values given");
        Plan plan = new Plan();
        ESIndex(analysis, plan);
        return plan;
    }

    @Override
    protected Plan visitInsertFromSubQueryAnalysis(InsertFromSubQueryAnalysis analysis, Context context) {
        List<ColumnIdent> columns = Lists.transform(analysis.columns(), new com.google.common.base.Function<Reference, ColumnIdent>() {
            @Nullable
            @Override
            public ColumnIdent apply(@Nullable Reference input) {
                if (input == null) {
                    return null;
                }
                return input.info().ident().columnIdent();
            }
        });
        ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
                analysis.table().ident().name(),
                analysis.table().primaryKey(),
                columns,
                analysis.primaryKeyColumnIndices(),
                analysis.partitionedByIndices(),
                analysis.routingColumn(),
                analysis.routingColumnIndex(),
                ImmutableSettings.EMPTY // TODO: define reasonable writersettings
        );

        SelectAnalysis subQueryAnalysis = analysis.subQueryAnalysis();
        Plan plan = visitSelectAnalysis(subQueryAnalysis, new Context(indexWriterProjection));
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitUpdateAnalysis(UpdateAnalysis analysis, Context context) {
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
    protected Plan visitDeleteAnalysis(DeleteAnalysis analysis, Context context) {
        Plan plan = new Plan();
        if (analysis.ids().size() == 1 && analysis.routingValues().size() == 1) {
            ESDelete(analysis, plan);
        } else {
            ESDeleteByQuery(analysis, plan);
        }
        return plan;
    }

    @Override
    protected Plan visitCopyAnalysis(final CopyAnalysis analysis, Context context) {
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
        List<String> partitionedByNames;
        String tableName = analysis.table().ident().name();
        if (analysis.partitionIdent() == null) {
            partitionedByNames = Lists.newArrayList(
                    Lists.transform(analysis.table().partitionedBy(), ColumnIdent.GET_FQN_NAME_FUNCTION)
            );
        } else {
            /*
             * if there is a partitionIdent in the analysis this means that the file doesn't include
             * the partition ident in the rows.
             *
             * Therefore there is no need to exclude the partition columns from the source and
             * it is possible to import into the partitioned index directly.
             */
            partitionedByNames = Arrays.asList();
        }
        List<Projection> projections = Arrays.<Projection>asList(new SourceIndexWriterProjection(
                tableName,
                analysis.table().primaryKey(),
                analysis.table().partitionedBy(),
                analysis.table().clusteredBy(),
                clusteredByPrimaryKeyIdx,
                analysis.settings(),
                null,
                partitionedByNames.size() > 0 ? partitionedByNames.toArray(new String[partitionedByNames.size()]) : null
        ));

        partitionedByNames.removeAll(Lists.transform(analysis.table().primaryKey(), ColumnIdent.GET_FQN_NAME_FUNCTION));

        int referencesSize = analysis.table().primaryKey().size() + partitionedByNames.size() + 1;
        referencesSize = clusteredByPrimaryKeyIdx == -1 ? referencesSize + 1 : referencesSize;
        List<Symbol> toCollect = new ArrayList<>(referencesSize);
        // add primaryKey columns
        for (ColumnIdent primaryKey : analysis.table().primaryKey()) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(primaryKey))
            );
        }

        // add partitioned columns (if not part of primaryKey)
        for (String partitionedColumn : partitionedByNames) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(ColumnIdent.fromPath(partitionedColumn)))
            );
        }

        // add clusteredBy column (if not part of primaryKey)
        if (clusteredByPrimaryKeyIdx == -1) {
            toCollect.add(
                    new Reference(analysis.table().getColumnInfo(analysis.table().clusteredBy()))
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
    protected Plan visitDropTableAnalysis(DropTableAnalysis analysis, Context context) {
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
    protected Plan visitCreateTableAnalysis(CreateTableAnalysis analysis, Context context) {
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
    protected Plan visitCreateAnalyzerAnalysis(CreateAnalyzerAnalysis analysis, Context context) {
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

    @Override
    public Plan visitSetAnalysis(SetAnalysis analysis, Context context) {
        Plan plan = new Plan();
        ESClusterUpdateSettingsNode node;
        if (analysis.isPersistent()) {
            node = new ESClusterUpdateSettingsNode(analysis.settings());
        } else {
            node = new ESClusterUpdateSettingsNode(ImmutableSettings.EMPTY, analysis.settings());
        }
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

            if (!analysis.table().partitions().isEmpty()) {
                for (String index : indices) {
                    plan.add(new ESDeleteIndexNode(index, true));
                }
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

    private void ESGet(SelectAnalysis analysis, Plan plan, Context context) {
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
        if (analysis.isSorted() || analysis.limit() != null
                || analysis.offset() > 0
                || context.projection.isPresent()) {
            TopNProjection tnp = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    analysis.offset(),
                    contextBuilder.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.<Projection>builder().add(tnp);
            if (context.projection.isPresent()) {
                projectionBuilder.add(context.projection.get());
            }
            plan.add(PlanNodeBuilder.localMerge(projectionBuilder.build(), getNode));
        }
    }

    private void normalSelect(SelectAnalysis analysis, Plan plan, Context context) {
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
            TopNProjection tnp = new TopNProjection(
                    analysis.offset() + analysis.limit(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
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
                analysis.reverseFlags(),
                analysis.nullsFirst()
        );
        tnp.outputs(contextBuilder.outputs());
        ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.<Projection>builder()
                .add(tnp);
        if (context.projection.isPresent()) {
            projectionBuilder.add(context.projection.get());
        }
        plan.add(PlanNodeBuilder.localMerge(projectionBuilder.build(), collectNode));
    }

    private void ESSearch(SelectAnalysis analysis, Plan plan, Context context) {
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

        boolean needsProjection = !Iterables.all(analysis.outputSymbols(), symbolIsReference)
                || context.projection.isPresent();
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
                analysis.nullsFirst(),
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

            ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.<Projection>builder()
                    .add(topN);
            if (context.projection.isPresent()) {
                projectionBuilder.add(context.projection.get());
            }
            plan.add(PlanNodeBuilder.localMerge(projectionBuilder.build(), node));
        }
    }

    private void globalAggregates(SelectAnalysis analysis, Plan plan, Context context) {
        String schema = analysis.table().ident().schema();
        if ((schema == null || schema.equalsIgnoreCase(DocSchemaInfo.NAME))
                && hasOnlyGlobalCount(analysis.outputSymbols())
                && !analysis.hasSysExpressions()
                && !analysis.table().isPartitioned()
                && !context.projection.isPresent()) {
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

        //// the handler stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (contextBuilder.aggregationsWrappedInScalar) {
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        if (context.projection.isPresent()) {
            projections.add(context.projection.get());
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

    private void groupBy(SelectAnalysis analysis, Plan plan, Context context) {
        if (analysis.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()
                || !requiresDistribution(analysis.table().getRouting(analysis.whereClause()))) {
            nonDistributedGroupBy(analysis, plan, context);
        } else if (context.projection.isPresent()) {
            distributedWriterGroupBy(analysis, plan, context.projection.get());
        } else {
            distributedGroupBy(analysis, plan);
        }
    }

    private boolean requiresDistribution(Routing routing) {
        if (!routing.hasLocations()) return false;
        if (routing.locations().size() > 1) return true;

        String nodeId = routing.locations().keySet().iterator().next();
        return !(nodeId == null || nodeId.equals(clusterService.localNode().id()));
    }

    private void nonDistributedGroupBy(SelectAnalysis analysis, Plan plan, Context context) {
        // TODO:  if the routing is HandlerSideRouting or has no locations
        // the localMergeNode isn't needed but instead the topN projection could be added to the
        // collectNode
        boolean ignoreSorting = context.projection.isPresent()
                && analysis.limit() == null
                && analysis.offset() == TopN.NO_OFFSET;
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, analysis.groupBy(), ignoreSorting)
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
        int limit;
        if (ignoreSorting) {
            limit = TopN.NO_LIMIT; // select all the things
        } else {
            limit = Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT);
        }
        TopNProjection topN = new TopNProjection(
                limit,
                analysis.offset(),
                contextBuilder.orderBy(),
                analysis.reverseFlags(),
                analysis.nullsFirst()
        );
        topN.outputs(contextBuilder.outputs());
        ImmutableList.Builder<Projection> builder = ImmutableList.<Projection>builder()
                .add(groupProjection, topN);
        if (context.projection.isPresent()) {
            builder.add(context.projection.get());
        }
        plan.add(PlanNodeBuilder.localMerge(builder.build(), collectNode));
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
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
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
                analysis.reverseFlags(),
                analysis.nullsFirst()
        );
        topN.outputs(outputs);
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);
        plan.add(localMergeNode);
    }

    /**
     * distributed collect on mapper nodes
     * with merge on reducer to final (they have row authority) and index write
     * if no limit and not offset is set
     * <p/>
     * final merge + index write on handler if limit or offset is set
     */
    private void distributedWriterGroupBy(SelectAnalysis analysis, Plan plan, Projection writerProjection) {
        boolean ignoreSorting = !analysis.isLimited();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, analysis.groupBy(), ignoreSorting)
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
        ImmutableList.Builder<Projection> projectionsBuilder = ImmutableList.builder();
        projectionsBuilder.add(new GroupProjection(
                contextBuilder.groupBy(),
                contextBuilder.aggregations()));

        boolean topNDone = false;
        if (analysis.isLimited() || contextBuilder.aggregationsWrappedInScalar) {
            topNDone = true;
            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(contextBuilder.outputs());
            projectionsBuilder.add(topN);
        }
        if (!analysis.isLimited()) {
            projectionsBuilder.add(writerProjection);
        }

        MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, projectionsBuilder.build());
        plan.add(mergeNode);


        // local merge on handler
        ImmutableList.Builder<Projection> builder = ImmutableList.builder();
        if (analysis.isLimited()) {
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
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(outputs);
            builder.add(topN);
            builder.add(writerProjection);
        } else {
            // sum up distributed indexWriter results
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
            builder.add(aggregationProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(builder.build(), mergeNode);
        plan.add(localMergeNode);
    }

    private List<String> nodesFromTable(SelectAnalysis analysis) {
        return Lists.newArrayList(analysis.table().getRouting(analysis.whereClause()).nodes());
    }

    private void ESIndex(InsertFromValuesAnalysis analysis, Plan plan) {
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
