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

package io.crate.action.sql;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.TaskResult;
import io.crate.metadata.PartitionName;
import io.crate.analyze.*;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.AlterTableAliasException;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 *
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
public class DDLStatementDispatcher extends AnalyzedStatementVisitor<Void, ListenableFuture<Long>> {

    private final ClusterService clusterService;
    private final BlobIndices blobIndices;
    private final Provider<Executor> executorProvider;
    private final TransportActionProvider transportActionProvider;

    @Inject
    public DDLStatementDispatcher(ClusterService clusterService,
                                  BlobIndices blobIndices,
                                  Provider<Executor> executorProvider,
                                  TransportActionProvider transportActionProvider) {
        this.clusterService = clusterService;
        this.blobIndices = blobIndices;
        this.executorProvider = executorProvider;
        this.transportActionProvider = transportActionProvider;
    }

    @Override
    protected ListenableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Void context) {
        throw new UnsupportedOperationException(String.format("Can't handle \"%s\"", analyzedStatement));
    }

   @Override
    public ListenableFuture<Long> visitCreateBlobTableStatement(
           CreateBlobTableAnalyzedStatement analysis, Void context) {
        return wrapRowCountFuture(
                blobIndices.createBlobTable(
                        analysis.tableName(),
                        analysis.tableParameter().settings()
                ),
                1L
        );
    }

    @Override
    public ListenableFuture<Long> visitAddColumnStatement(final AddColumnAnalyzedStatement analysis, Void context) {
        final SettableFuture<Long> result = SettableFuture.create();
        if (analysis.newPrimaryKeys()) {
            Plan plan = genCountStarPlan(analysis.table());
            Job job = executorProvider.get().newJob(plan);
            ListenableFuture<List<TaskResult>> resultFuture = Futures.allAsList(executorProvider.get().execute(job));
            Futures.addCallback(resultFuture, new FutureCallback<List<TaskResult>>() {
                @Override
                public void onSuccess(@Nullable List<TaskResult> resultList) {
                    assert resultList != null && resultList.size() == 1;
                    Object[][] rows = resultList.get(0).rows();
                    if ((Long) rows[0][0] == 0L) {
                        addColumnToTable(analysis, result);
                    } else {
                        result.setException(new UnsupportedOperationException(
                                "Cannot add a primary key column to a table that isn't empty"));
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    result.setException(t);
                }
            });
        } else {
            addColumnToTable(analysis, result);
        }
        return result;
    }

    private Plan genCountStarPlan(TableInfo table) {
        Aggregation countAggregationPartial = new Aggregation(
                CountAggregation.COUNT_STAR_FUNCTION,
                ImmutableList.<Symbol>of(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL);
        Aggregation countAggregationFinal = new Aggregation(
                CountAggregation.COUNT_STAR_FUNCTION,
                ImmutableList.<Symbol>of(new InputColumn(0, DataTypes.LONG)),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL);

        CollectNode collectNode = new CollectNode(
                "count",
                table.getRouting(WhereClause.MATCH_ALL),
                ImmutableList.<Symbol>of(),
                Arrays.<Projection>asList(new AggregationProjection(ImmutableList.of(countAggregationPartial))));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.outputTypes(ImmutableList.<DataType>of(DataTypes.UNDEFINED));
        MergeNode mergeNode = new MergeNode("local count merge", collectNode.executionNodes().size());
        mergeNode.projections(ImmutableList.<Projection>of(new AggregationProjection(ImmutableList.of(countAggregationFinal))));
        Plan plan = new Plan();
        plan.add(collectNode);
        plan.add(mergeNode);
        return plan;
    }

    private void addColumnToTable(AddColumnAnalyzedStatement analysis, final SettableFuture<Long> result) {
        boolean updateTemplate = analysis.table().isPartitioned();
        final AtomicInteger operations = new AtomicInteger(updateTemplate ? 2 : 1);
        final Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        if (updateTemplate) {
            String templateName = PartitionName.templateName(analysis.table().ident().schema(), analysis.table().ident().name());
            IndexTemplateMetaData indexTemplateMetaData =
                    clusterService.state().metaData().templates().get(templateName);
            if (indexTemplateMetaData == null) {
                result.setException(new RuntimeException("Template for partitioned table is missing"));
            }
            mergeMappingAndUpdateTemplate(result, mapping, indexTemplateMetaData, operations);
        }

        // need to merge the _meta part of the mapping mapping before-hand because ES doesn't
        // update the _meta column recursively. Instead it is overwritten and therefore partitioned by
        // and collection_type information would be lost.
        String[] indexNames = getIndexNames(analysis.table(), null);
        if (indexNames.length == 0) {
            // if there are no indices yet we can return because we don't need to update existing mapping
            if (operations.decrementAndGet() == 0) {
                result.set(1L);
            }
            return;
        }
        PutMappingRequest request = new PutMappingRequest();
        request.indices(indexNames);
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        IndexMetaData indexMetaData = clusterService.state().getMetaData().getIndices().get(indexNames[0]);
        try {
            Map mergedMeta = (Map)indexMetaData.getMappings()
                    .get(Constants.DEFAULT_MAPPING_TYPE)
                    .getSourceAsMap()
                    .get("_meta");
            if (mergedMeta != null) {
                XContentHelper.update(mergedMeta, (Map) mapping.get("_meta"), false);
                mapping.put("_meta", mergedMeta);
            }
            request.source(mapping);
        } catch (IOException e) {
            result.setException(e);
        }
        transportActionProvider.transportPutMappingAction().execute(request, new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse putMappingResponse) {
                if (operations.decrementAndGet() == 0) {
                    result.set(1L);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
    }

    private Map<String, Object> parseMapping(String mappingSource) throws IOException {
        return XContentFactory.xContent(mappingSource).createParser(mappingSource).mapAndClose();
    }

    private void mergeMappingAndUpdateTemplate(final SettableFuture<Long> result,
                                               final Map<String, Object> mapping,
                                               final IndexTemplateMetaData templateMetaData,
                                               final AtomicInteger operations) {
        Map<String, Object> mergedMapping = mergeMapping(templateMetaData, mapping);
        PutIndexTemplateRequest updateTemplateRequest = new PutIndexTemplateRequest(templateMetaData.name())
                .create(false)
                .mapping(Constants.DEFAULT_MAPPING_TYPE, mergedMapping)
                .settings(templateMetaData.settings())
                .template(templateMetaData.template());

            for (ObjectObjectCursor<String, AliasMetaData> container : templateMetaData.aliases()) {
                Alias alias = new Alias(container.key);
                updateTemplateRequest.alias(alias);
            }
            transportActionProvider.transportPutIndexTemplateAction().execute(updateTemplateRequest, new ActionListener<PutIndexTemplateResponse>() {
                @Override
                public void onResponse(PutIndexTemplateResponse putIndexTemplateResponse) {
                    if (operations.decrementAndGet() == 0) {
                        result.set(1L);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        }

    private Map<String, Object> mergeMapping(IndexTemplateMetaData templateMetaData,
                                             Map<String, Object> newMapping) {
        Map<String, Object> mergedMapping = new HashMap<>();
        for (ObjectObjectCursor<String, CompressedString> cursor : templateMetaData.mappings()) {
            try {
                Map<String, Object> mapping = parseMapping(cursor.value.toString());
                Object o = mapping.get(Constants.DEFAULT_MAPPING_TYPE);
                assert o != null && o instanceof Map;

                XContentHelper.update(mergedMapping, (Map) o, false);
            } catch (IOException e) {
                // pass
            }
        }
        XContentHelper.update(mergedMapping, newMapping, false);
        return mergedMapping;
    }

    @Override
    public ListenableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, Void context) {
        return wrapRowCountFuture(
                blobIndices.alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings()),
                1L);
    }

    @Override
    public ListenableFuture<Long> visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, Void context) {
        return wrapRowCountFuture(blobIndices.dropBlobTable(analysis.table().ident().name()), 1L);
    }

    private String[] getIndexNames(TableInfo tableInfo, @Nullable PartitionName partitionName) {
        String[] indexNames;
        if (tableInfo.isPartitioned()) {
            if (partitionName == null) {
                // all partitions
                indexNames = tableInfo.concreteIndices();
            } else {
                // single partition
                indexNames = new String[] { partitionName.stringValue() };
            }
        } else {
            indexNames = new String[] { tableInfo.ident().esName() };
        }
        return indexNames;
    }

    @Override
    public ListenableFuture<Long> visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, Void context) {
        String[] indexNames = getIndexNames(analysis.table(), analysis.partitionName());
        if (analysis.table().schemaInfo().systemSchema() || indexNames.length == 0) {
            // shortcut when refreshing on system tables
            // or empty partitioned tables
            return Futures.immediateFuture(null);
        } else {
            final SettableFuture<Long> future = SettableFuture.create();
            RefreshRequest request = new RefreshRequest(indexNames);
            transportActionProvider.transportRefreshAction().execute(request, new ActionListener<RefreshResponse>() {
                @Override
                public void onResponse(RefreshResponse refreshResponse) {
                    future.set(null); // no row count
                }

                @Override
                public void onFailure(Throwable e) {
                    future.setException(e);
                }
            });
            return future;
        }
    }

    private ListenableFuture<Long> wrapRowCountFuture(ListenableFuture<?> wrappedFuture, final Long rowCount) {
        final SettableFuture<Long> wrappingFuture = SettableFuture.create();
        Futures.addCallback(wrappedFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
                wrappingFuture.set(rowCount);
            }

            @Override
            public void onFailure(Throwable t) {
                wrappingFuture.setException(t);
            }
        });
        return wrappingFuture;
    }

    @Override
    public ListenableFuture<Long> visitAlterTableStatement(final AlterTableAnalyzedStatement analysis, Void context) {
        final SettableFuture<Long> result = SettableFuture.create();
        final String[] indices;
        boolean updateTemplate = false;
        boolean updateMapping = !analysis.tableParameter().mappings().isEmpty();
        final TableParameter tableParameter = analysis.tableParameter();
        TableParameter concreteTableParameter = tableParameter;
        if (analysis.table().isPartitioned()) {
            if (analysis.partitionName().isPresent()) {
                indices = new String[]{ analysis.partitionName().get().stringValue() };
            } else {
                updateTemplate = true; // only update template when updating whole partitioned table
                indices = analysis.table().concreteIndices();
                AlterPartitionedTableParameterInfo tableSettingsInfo =
                        (AlterPartitionedTableParameterInfo)analysis.table().tableParameterInfo();
                // create new filtered partition table settings
                concreteTableParameter = new TableParameter(
                        analysis.tableParameter().settings(),
                        tableSettingsInfo.partitionTableSettingsInfo().supportedInternalSettings());
            }
        } else {
           indices = new String[]{ analysis.table().ident().esName() };
        }

        if (analysis.table().isAlias()) {
            throw new AlterTableAliasException(analysis.table().ident().fqn());
        }

        final List<ListenableFuture<?>> results = new ArrayList<>(
                indices.length + (updateTemplate ? 1 : 0) + (updateMapping ? 1 : 0)
        );
        if (updateTemplate) {
            final SettableFuture<?> templateFuture = SettableFuture.create();
            results.add(templateFuture);

            // update template
            final String templateName = PartitionName.templateName(analysis.table().ident().schema(), analysis.table().ident().name());
            GetIndexTemplatesRequest getRequest = new GetIndexTemplatesRequest(templateName);

            transportActionProvider.transportGetIndexTemplatesAction().execute(getRequest, new ActionListener<GetIndexTemplatesResponse>() {
                @Override
                public void onResponse(GetIndexTemplatesResponse response) {
                    Map<String, Object> mapping = new HashMap<>();
                    IndexTemplateMetaData template = response.getIndexTemplates().get(0);
                    mapping = mergeMapping(template, analysis.tableParameter().mappings());

                    ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
                    settingsBuilder.put(template.settings());
                    settingsBuilder.put(tableParameter.settings());

                    PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
                            .create(false)
                            .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                            .settings(settingsBuilder.build())
                            .template(template.template());
                    for (ObjectObjectCursor<String, AliasMetaData> container : response.getIndexTemplates().get(0).aliases()) {
                        Alias alias = new Alias(container.key);
                        request.alias(alias);
                    }
                    transportActionProvider.transportPutIndexTemplateAction().execute(request, new ActionListener<PutIndexTemplateResponse>() {
                        @Override
                        public void onResponse(PutIndexTemplateResponse putIndexTemplateResponse) {
                            templateFuture.set(null);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            templateFuture.setException(e);
                        }
                    });

                }

                @Override
                public void onFailure(Throwable e) {
                    templateFuture.setException(e);
                }
            });

        }
        if (!concreteTableParameter.settings().getAsMap().isEmpty()) {
            // update every concrete index
            for (String index : indices) {
                UpdateSettingsRequest request = new UpdateSettingsRequest(
                        concreteTableParameter.settings(),
                        index);
                final SettableFuture<?> future = SettableFuture.create();
                results.add(future);
                transportActionProvider.transportUpdateSettingsAction().execute(request, new ActionListener<UpdateSettingsResponse>() {
                    @Override
                    public void onResponse(UpdateSettingsResponse updateSettingsResponse) {
                        future.set(null);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        future.setException(e);
                    }
                });
            }
        }
        if (updateMapping) {
            PutMappingRequest request = new PutMappingRequest(indices);
            request.type(Constants.DEFAULT_MAPPING_TYPE);
            request.source(analysis.tableParameter().mappings());
            final SettableFuture<?> future = SettableFuture.create();
            results.add(future);
            transportActionProvider.transportPutMappingAction().execute(request, new ActionListener<PutMappingResponse>() {
                @Override
                public void onResponse(PutMappingResponse putMappingResponse) {
                    future.set(null);
                }

                @Override
                public void onFailure(Throwable e) {
                    future.setException(e);
                }
            });
        }
        Futures.addCallback(Futures.allAsList(results), new FutureCallback<List<?>>() {
            @Override
            public void onSuccess(@Nullable List<?> resultList) {
                result.set(null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });

        return result;
    }
}
