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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.*;
import io.crate.analyze.where.WhereClause;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.relation.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.ddl.CreateTableNode;
import io.crate.planner.node.ddl.DropTableNode;
import io.crate.planner.node.ddl.ESClusterUpdateSettingsNode;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
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

@Singleton
public class Planner extends AnalysisVisitor<Planner.Context, Plan> {

    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();
    static final PlannerReferenceExtractor referenceExtractor = new PlannerReferenceExtractor();
    static final PlannerFunctionArgumentCopier functionArgumentCopier = new PlannerFunctionArgumentCopier();

    private final TableRelationVisitor tableRelationVisitor = new TableRelationVisitor();

    private final ClusterService clusterService;
    private AggregationProjection sumUpResultsProjection;

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
        assert !analysis.hasNoResult() : "analysis has no result. we're wrong here";
        return process(analysis, EMPTY_CONTEXT);
    }

    @Override
    protected Plan visitSelectAnalysis(SelectAnalysis analysis, Context context) {
        Plan plan = new Plan();
        plan.expectsAffectedRows(false);

        if (analysis.querySpecification().hasGroupBy()) {
            groupBy(analysis, plan, context);
        } else if (analysis.hasAggregates()) {
            globalAggregates(analysis, plan, context);
        } else {
            AnalyzedQuerySpecification querySpec = analysis.querySpecification();
            WhereClause whereClause = querySpec.whereClause();
            TableRelation tableRelation = tableRelationVisitor.process(querySpec);
            TableInfo tableInfo = tableRelation.tableInfo();
            if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                    tableInfo.getRouting(whereClause).hasLocations() &&
                    tableInfo.schemaInfo().name().equals(DocSchemaInfo.NAME)
                    && (analysis.isLimited() || tableRelation.ids().size() > 0 || !context.indexWriterProjection.isPresent())) {

                if (tableRelation.ids().size() > 0
                        && tableRelation.routingValues().size() > 0
                        && !tableInfo.isAlias()) {
                    ESGet(analysis, tableRelation, plan, context);
                } else {
                    queryThenFetch(analysis, plan, context);
                }
            } else {
                collect(analysis, plan, context);
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
                ImmutableSettings.EMPTY, // TODO: define reasonable writersettings
                analysis.table().isPartitioned()
        );

        SelectAnalysis subQueryAnalysis = analysis.subQueryAnalysis();
        Plan plan = visitSelectAnalysis(subQueryAnalysis, new Context(indexWriterProjection));
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitUpdateAnalysis(UpdateAnalysis analysis, Context context) {
        Plan plan = new Plan();
        for (UpdateAnalysis.NestedAnalysis nestedAnalysis : analysis.nestedAnalysis()) {
            if (!nestedAnalysis.hasNoResult()) {
                ESUpdateNode node = new ESUpdateNode(
                        indices(nestedAnalysis),
                        nestedAnalysis.assignments(),
                        nestedAnalysis.whereClause(),
                        nestedAnalysis.ids(),
                        nestedAnalysis.routingValues()
                );
                plan.add(node);
            }
        }
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitDeleteAnalysis(DeleteAnalysis analysis, Context context) {
        Plan plan = new Plan();
        for (DeleteAnalysis.NestedDeleteAnalysis nestedDeleteAnalysis : analysis.nestedAnalysis()) {
            if (nestedDeleteAnalysis.ids().size() == 1 &&
                    nestedDeleteAnalysis.routingValues().size() == 1) {
                ESDelete(nestedDeleteAnalysis, plan);
            } else {
                ESDeleteByQuery(nestedDeleteAnalysis, plan);
            }
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
        QueryAndFetchNode queryAndFetchNode = PlanNodeBuilder.queryAndFetch(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(projection),
                null,
                analysis.partitionIdent()
        );
        plan.add(queryAndFetchNode);
        MergeNode mergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(sumUpResultsProjection()), queryAndFetchNode);
        plan.add(mergeNode);
        plan.expectsAffectedRows(true);
    }

    private void copyFromPlan(CopyAnalysis analysis, Plan plan) {
        /**
         * copy from has two "modes":
         *
         * 1: non-partitioned tables or partitioned tables with partition ident --> import into single es index
         *    -> queryAndFetch raw source and import as is
         *
         * 2: partitioned table without partition ident
         *    -> queryAndFetch document and partition by values
         *    -> exclude partitioned by columns from document
         *    -> insert into es index (partition determined by partition by value)
         */

        TableInfo table = analysis.table();
        int clusteredByPrimaryKeyIdx = table.primaryKey().indexOf(analysis.table().clusteredBy());
        List<String> partitionedByNames;
        List<ColumnIdent> partitionByColumns;
        String tableName;

        if (analysis.partitionIdent() == null) {
            tableName = table.ident().name();
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
            tableName = PartitionName.fromPartitionIdent(table.ident().name(), analysis.partitionIdent()).stringValue();
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
                ImmutableList.<Projection>of(sumUpResultsProjection()),
                analysis.settings().get("compression", null),
                analysis.settings().getAsBoolean("shared", null)
        );
        collectNode.configure();
        plan.add(collectNode);
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
        plan.add(new DropTableNode(analysis.table()));
        plan.expectsAffectedRows(true);
        return plan;
    }

    @Override
    protected Plan visitCreateTableAnalysis(CreateTableAnalysis analysis, Context context) {
        Plan plan = new Plan();
        TableIdent tableIdent = analysis.tableIdent();
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "a SCHEMA name other than null isn't allowed.");

        CreateTableNode createTableNode;
        if (analysis.isPartitioned()) {
            createTableNode = CreateTableNode.createPartitionedTableNode(
                    tableIdent.name(),
                    analysis.indexSettings().getByPrefix("index."),
                    analysis.mapping(),
                    analysis.templateName(),
                    analysis.templatePrefix()
            );
        } else {
            createTableNode = CreateTableNode.createTableNode(
                    tableIdent.name(),
                    analysis.indexSettings(),
                    analysis.mapping()
            );
        }
        plan.add(createTableNode);
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
        if (analysis.isReset()) {
            // always reset persistent AND transient settings
            node = new ESClusterUpdateSettingsNode(
                    analysis.settingsToRemove(), analysis.settingsToRemove());
        } else {
            if (analysis.isPersistent()) {
                node = new ESClusterUpdateSettingsNode(analysis.settings());
            } else {
                node = new ESClusterUpdateSettingsNode(ImmutableSettings.EMPTY, analysis.settings());
            }
        }
        plan.add(node);
        plan.expectsAffectedRows(true);
        return plan;
    }

    private void ESDelete(DeleteAnalysis.NestedDeleteAnalysis analysis, Plan plan) {
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

    private void ESDeleteByQuery(DeleteAnalysis.NestedDeleteAnalysis analysis, Plan plan) {
        String[] indices = indices(analysis);

        if (indices.length > 0 && !analysis.whereClause().noMatch()) {
            if (!analysis.whereClause().hasQuery() && analysis.table().isPartitioned()) {
                for (String index : indices) {
                    plan.add(new ESDeleteIndexNode(index, true));
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
        }
        plan.expectsAffectedRows(true);
    }

    private void ESGet(SelectAnalysis analysis, TableRelation tableRelation, Plan plan, Context context) {
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(analysis.outputSymbols())
                .orderBy(querySpec.orderBy());

        TableInfo tableInfo = tableRelation.tableInfo();
        String indexName;
        if (tableInfo.isPartitioned()) {
            assert querySpec.whereClause().partitions().size() == 1 : "ambiguous partitions for ESGet";
            indexName = querySpec.whereClause().partitions().get(0);
        } else {
            indexName = tableInfo.ident().name();
        }
        ESGetNode getNode = new ESGetNode(
                indexName,
                tableRelation.ids(),
                tableRelation.routingValues(),
                tableInfo.partitionedByColumns());
        getNode.outputs(contextBuilder.toCollect());
        getNode.outputTypes(extractDataTypes(analysis.outputSymbols()));
        plan.add(getNode);

        // handle sorting, limit and offset
        if (analysis.isSorted() || querySpec.limit() != null
                || querySpec.offset() > 0
                || context.indexWriterProjection.isPresent()) {
            TopNProjection tnp = new TopNProjection(
                    Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    querySpec.offset(),
                    contextBuilder.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.<Projection>builder().add(tnp);
            if (context.indexWriterProjection.isPresent()) {
                projectionBuilder.add(context.indexWriterProjection.get());
            }
            plan.add(PlanNodeBuilder.localMerge(projectionBuilder.build(), getNode));
        }
    }

    private void collect(SelectAnalysis analysis, Plan plan, Context context) {
        // select on node or shard level or information_schema
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        TableInfo tableInfo = tableRelationVisitor.process(querySpec).tableInfo();
        QueryAndFetchNode node = PlanNodeBuilder.collect(querySpec,
                tableInfo,
                context.indexWriterProjection,
                sumUpResultsProjection());
        plan.add(node);
    }

    private void queryThenFetch(SelectAnalysis analysis, Plan plan, Context context) {
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        TableInfo tableInfo = tableRelationVisitor.process(querySpec).tableInfo();

        QueryThenFetchNode node = PlanNodeBuilder.queryThenFetch(querySpec,
                tableInfo,
                context.indexWriterProjection);
        plan.add(node);
    }

    private void globalAggregates(SelectAnalysis analysis, Plan plan, Context context) {
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        TableInfo tableInfo = tableRelationVisitor.process(querySpec).tableInfo();

        // check for select count(*) from ...
        String schema = tableInfo.ident().schema();
        if ((schema == null || schema.equalsIgnoreCase(DocSchemaInfo.NAME))
                && hasOnlyGlobalCount(querySpec.outputs())
                && !analysis.hasSysExpressions()
                && !context.indexWriterProjection.isPresent()) {
            plan.add(new ESCountNode(tableInfo, querySpec.whereClause()));
            return;
        }
        QueryAndFetchNode node = PlanNodeBuilder.collectAggregate(querySpec, tableInfo);

        // in case of insert from subquery
        if (context.indexWriterProjection.isPresent()) {
            node.addProjection(context.indexWriterProjection.get());
        }
        plan.add(node);
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

    private void groupBy(SelectAnalysis analysis, Plan plan, Context context) {

        if (analysis.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()
                || !requiresDistribution(analysis)) {
            nonDistributedGroupBy(analysis, plan, context);
        } else if (context.indexWriterProjection.isPresent()) {
            distributedWriterGroupBy(analysis, plan, context.indexWriterProjection.get());
        } else {
            distributedGroupBy(analysis, plan);
        }
    }

    private boolean requiresDistribution(SelectAnalysis analysis) {
        TableInfo tableInfo = tableRelationVisitor.process(analysis.querySpecification()).tableInfo();
        Routing routing = tableInfo.getRouting(analysis.querySpecification().whereClause());
        if (!routing.hasLocations()) return false;
        if (groupedByClusteredColumnOrPrimaryKeys(tableInfo, analysis.querySpecification().groupBy())) return false;
        if (routing.locations().size() > 1) return true;

        String nodeId = routing.locations().keySet().iterator().next();
        return !(nodeId == null || nodeId.equals(clusterService.localNode().id()));
    }

    private boolean groupedByClusteredColumnOrPrimaryKeys(TableInfo tableInfo, List<Symbol> groupBy) {
        assert groupBy != null;
        if (groupBy.size() > 1) {
            return groupedByPrimaryKeys(groupBy, tableInfo.primaryKey());
        }

        // this also handles the case if there is only one primary key.
        // as clustered by column == pk column  in that case
        Symbol groupByKey = groupBy.get(0);
        return (groupByKey instanceof Reference
                && ((Reference) groupByKey).info().ident().columnIdent().equals(tableInfo.clusteredBy()));
    }

    private boolean groupedByPrimaryKeys(List<Symbol> groupBy, List<ColumnIdent> primaryKeys) {
        if (groupBy.size() != primaryKeys.size()) {
            return false;
        }
        for (int i = 0, groupBySize = groupBy.size(); i < groupBySize; i++) {
            Symbol groupBySymbol = groupBy.get(i);
            if (groupBySymbol instanceof Reference) {
                ColumnIdent pkIdent = primaryKeys.get(i);
                if (!pkIdent.equals(((Reference) groupBySymbol).info().ident().columnIdent())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private void nonDistributedGroupBy(SelectAnalysis analysis, Plan plan, Context context) {
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        TableRelation tableRelation = tableRelationVisitor.process(querySpec);
        TableInfo tableInfo = tableRelation.tableInfo();

        boolean ignoreSorting = context.indexWriterProjection.isPresent()
                && querySpec.limit() == null
                && querySpec.offset() == TopN.NO_OFFSET;
        boolean groupedByClusteredPk = groupedByClusteredColumnOrPrimaryKeys(tableInfo, querySpec.groupBy());

        int numAggregationSteps = 2;
        if (analysis.rowGranularity() == RowGranularity.DOC) {
            /**
             * this is only the case if the group by key is the clustered by column.
             * collectNode has row-authority and there is no need to group again on the handler node
             */
            numAggregationSteps = 1;
        }
        PlannerContextBuilder contextBuilder =
                new PlannerContextBuilder(numAggregationSteps, querySpec.groupBy(), ignoreSorting)
                .output(analysis.outputSymbols())
                .orderBy(querySpec.orderBy());

        Symbol havingClause = null;
        if (querySpec.having().isPresent() && querySpec.having().get().symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(querySpec.having().get());
        }

        ImmutableList.Builder<Projection> collectProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> projections = ImmutableList.builder();
        GroupProjection groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());

        if(groupedByClusteredPk){
            groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        }

        List<Symbol> toCollect = contextBuilder.toCollect();
        contextBuilder.nextStep();

        collectProjections.add(groupProjection);
        boolean topNDone = addTopNIfApplicableOnReducer(analysis, contextBuilder, collectProjections);

        QueryAndFetchNode queryAndFetchNode = PlanNodeBuilder.queryAndFetch(
                analysis,
                toCollect,
                collectProjections.build(),
                null, // no projections here, added further down FIXME!
                null
        );
        plan.add(queryAndFetchNode);

        contextBuilder.nextStep();

        // handler
        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            if(groupedByClusteredPk){
                fp.requiredGranularity(RowGranularity.SHARD);
            }
            projections.add(fp);
        }
        if (numAggregationSteps == 2) {
            projections.add(new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations()));
        }
        if (!ignoreSorting) {
            List<Symbol> outputs;
            List<Symbol> orderBy;
            if (topNDone) {
                orderBy = contextBuilder.passThroughOrderBy();
                outputs = contextBuilder.passThroughOutputs();
            } else {
                orderBy = contextBuilder.orderBy();
                outputs = contextBuilder.outputs();
            }
            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    querySpec.offset(),
                    orderBy,
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(outputs);
            projections.add(topN);
        }
        if (context.indexWriterProjection.isPresent()) {
            projections.add(context.indexWriterProjection.get());
        }
        plan.add(PlanNodeBuilder.localMerge(projections.build(), queryAndFetchNode));
    }

    /**
     * this method adds a TopNProjection to the given projectBuilder
     * if the analysis has a limit, offset or if an aggregation is wrapped inside a scalar.
     *
     * the limit given to the topN projection will be limit + offset because there will be another
     * topN projection on the handler node which will do the final sort + limiting (if applicable)
     */
    private boolean addTopNIfApplicableOnReducer(SelectAnalysis analysis,
                                                 PlannerContextBuilder contextBuilder,
                                                 ImmutableList.Builder<Projection> projectionBuilder) {
        if (requireLimitOnReducer(analysis, contextBuilder.aggregationsWrappedInScalar)) {
            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(analysis.querySpecification().limit(), Constants.DEFAULT_SELECT_LIMIT)
                            + analysis.querySpecification().offset(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(contextBuilder.outputs());
            projectionBuilder.add(topN);
            return true;
        }
        return false;
    }

    private boolean requireLimitOnReducer(SelectAnalysis analysis, boolean aggregationsWrappedInScalar) {
        return (analysis.querySpecification().limit() != null
                || analysis.querySpecification().offset() > 0
                || aggregationsWrappedInScalar);
    }

    /**
     * distributed queryAndFetch on mapper nodes
     * with merge on reducer to final (they have row authority)
     * <p/>
     * final merge on handler
     */
    private void distributedGroupBy(SelectAnalysis analysis, Plan plan) {
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        TableInfo tableInfo = tableRelationVisitor.process(querySpec).tableInfo();

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, querySpec.groupBy())
                .output(analysis.outputSymbols())
                .orderBy(querySpec.orderBy());

        Symbol havingClause = null;
        if (querySpec.having().isPresent() && querySpec.having().get().symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(querySpec.having().get());
        }
        ImmutableList.Builder<Projection> collectorProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> projections = ImmutableList.builder();

        // collector
        GroupProjection groupProjection = new GroupProjection(
                contextBuilder.groupBy(), contextBuilder.aggregations());
        collectorProjections.add(groupProjection);

        QueryAndFetchNode queryAndFetchNode = new QueryAndFetchNode("distributing collect",
                tableInfo.getRouting(querySpec.whereClause()),
                contextBuilder.toCollect(),
                contextBuilder.outputs(),
                null,
                null,
                null,
                null,
                null,
                collectorProjections.build(),
                null, // projections added further down FIXME!
                querySpec.whereClause(),
                analysis.rowGranularity(),
                tableInfo.isPartitioned()
                );
        queryAndFetchNode.downStreamNodes(nodesFromTable(tableInfo, querySpec.whereClause()));
        queryAndFetchNode.configure();
        plan.add(queryAndFetchNode);

        contextBuilder.nextStep();

        // mergeNode for reducer
        projections.add(new GroupProjection(
                contextBuilder.groupBy(),
                contextBuilder.aggregations()));


        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }

        boolean topNDone = addTopNIfApplicableOnReducer(analysis, contextBuilder, projections);
        MergeNode mergeNode = PlanNodeBuilder.distributedMerge(queryAndFetchNode, projections.build());
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
                Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT),
                querySpec.offset(),
                orderBy,
                analysis.reverseFlags(),
                analysis.nullsFirst()
        );
        topN.outputs(outputs);
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);
        plan.add(localMergeNode);
    }

    /**
     * distributed queryAndFetch on mapper nodes
     * with merge on reducer to final (they have row authority) and index write
     * if no limit and not offset is set
     * <p/>
     * final merge + index write on handler if limit or offset is set
     */
    private void distributedWriterGroupBy(SelectAnalysis analysis, Plan plan, Projection writerProjection) {
        boolean ignoreSorting = !analysis.isLimited();
        AnalyzedQuerySpecification querySpec = analysis.querySpecification();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, querySpec.groupBy(), ignoreSorting)
                .output(analysis.outputSymbols())
                .orderBy(querySpec.orderBy());

        Symbol havingClause = null;
        if (querySpec.having().isPresent() && querySpec.having().get().symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(querySpec.having().get());
        }

        TableInfo tableInfo = tableRelationVisitor.process(querySpec).tableInfo();
        ImmutableList.Builder<Projection> collectorProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> projections = ImmutableList.builder();

        // collector
        GroupProjection groupProjection = new GroupProjection(
                contextBuilder.groupBy(), contextBuilder.aggregations());
        collectorProjections.add(groupProjection);

        QueryAndFetchNode queryAndFetchNode = new QueryAndFetchNode("distributing collect",
                tableInfo.getRouting(querySpec.whereClause()),
                contextBuilder.toCollect(),
                contextBuilder.outputs(),
                analysis.sortSymbols(),
                analysis.reverseFlags(),
                analysis.nullsFirst(),
                analysis.limit(),
                analysis.offset(),
                collectorProjections.build(),
                null, // not added here
                querySpec.whereClause(),
                analysis.rowGranularity(),
                tableInfo.isPartitioned()
        );
        queryAndFetchNode.downStreamNodes(nodesFromTable(tableInfo, querySpec.whereClause()));
        queryAndFetchNode.configure();
        plan.add(queryAndFetchNode);

        contextBuilder.nextStep();

        // mergeNode for reducer

        projections.add(new GroupProjection(
                contextBuilder.groupBy(),
                contextBuilder.aggregations()));


        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }

        boolean topNDone = false;
        if (analysis.isLimited()) {
            topNDone = true;
            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset(),
                    0,
                    querySpec.orderBy(),
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(contextBuilder.outputs());
            projections.add(topN);
        } else {
            projections.add(writerProjection);
        }

        MergeNode mergeNode = PlanNodeBuilder.distributedMerge(queryAndFetchNode, projections.build());
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
                    Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    querySpec.offset(),
                    orderBy,
                    analysis.reverseFlags(),
                    analysis.nullsFirst()
            );
            topN.outputs(outputs);
            builder.add(topN);
            builder.add(writerProjection);
        } else {
            // sum up distributed indexWriter results
            builder.add(sumUpResultsProjection());
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(builder.build(), mergeNode);
        plan.add(localMergeNode);
    }

    private List<String> nodesFromTable(TableInfo tableInfo, WhereClause whereClause) {
        return Lists.newArrayList(tableInfo.getRouting(whereClause).nodes());
    }

    private void ESIndex(InsertFromValuesAnalysis analysis, Plan plan) {
        String[] indices;
        if (analysis.table().isPartitioned()) {
            indices = analysis.partitions().toArray(new String[analysis.partitions().size()]);
        } else {
            indices = new String[]{ analysis.table().ident().name() };
        }

        ESIndexNode indexNode = new ESIndexNode(
                indices,
                analysis.sourceMaps(),
                analysis.ids(),
                analysis.routingValues(),
                analysis.table().isPartitioned(),
                analysis.parameterContext().hasBulkParams()
                );
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

    /**
     * return the ES index names the query should go to
     */
    private String[] indices(AbstractDataAnalysis analysis) {
        TableInfo tableInfo = analysis.table();
        String[] indices;

        if (analysis.whereClause().noMatch()) {
            indices = org.elasticsearch.common.Strings.EMPTY_ARRAY;
        } else if (!tableInfo.isPartitioned()) {
            // table name for non-partitioned tables
            indices = new String[]{ tableInfo.ident().name() };
        } else if (analysis.whereClause().partitions().size() == 0) {
            // all partitions
            indices = new String[tableInfo.partitions().size()];
            for (int i = 0; i < tableInfo.partitions().size(); i++) {
                indices[i] = tableInfo.partitions().get(i).stringValue();
            }

        } else {
            indices = analysis.whereClause().partitions().toArray(
                    new String[analysis.whereClause().partitions().size()]);
        }
        return indices;
    }

    private AggregationProjection sumUpResultsProjection() {
        if (sumUpResultsProjection == null) {
            sumUpResultsProjection = new AggregationProjection(
                    Arrays.asList(new Aggregation(
                                    new FunctionInfo(
                                            new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE)),
                                            DataTypes.LONG
                                    ),
                                    Arrays.<Symbol>asList(new InputColumn(0)),
                                    Aggregation.Step.ITER,
                                    Aggregation.Step.FINAL
                            )
                    )
            );
        }
        return sumUpResultsProjection;
    }

    private static class TableRelationVisitor extends RelationVisitor<Void, TableRelation> {

        public TableRelation process(AnalyzedRelation relation) {
            return process(relation, null);
        }

        @Override
        public TableRelation visitTableRelation(TableRelation tableRelation, Void context) {
            return tableRelation;
        }

        @Override
        public TableRelation visitAliasedRelation(AliasedAnalyzedRelation relation, Void context) {
            return process(relation.child(), context);
        }

        @Override
        public TableRelation visitQuerySpecification(AnalyzedQuerySpecification relation, Void context) {
            return process(relation.sourceRelation(), context);
        }

        @Override
        public TableRelation visitCrossJoinRelation(JoinRelation joinRelation, Void context) {
            throw new UnsupportedOperationException("join relation not supported");
        }
    }
}
