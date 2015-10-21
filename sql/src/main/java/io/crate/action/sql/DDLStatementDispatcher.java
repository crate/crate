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
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.blob.v2.BlobIndices;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.exceptions.AlterTableAliasException;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TableCreator;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.OutputName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
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
@Singleton
public class DDLStatementDispatcher extends AnalyzedStatementVisitor<UUID, ListenableFuture<Long>> {

    private final ClusterService clusterService;
    private final BlobIndices blobIndices;
    private final Provider<Executor> executorProvider;
    private final TransportActionProvider transportActionProvider;
    private final Planner planner;
    private final TableCreator tableCreator;

    @Inject
    public DDLStatementDispatcher(ClusterService clusterService,
                                  BlobIndices blobIndices,
                                  TableCreator tableCreator,
                                  Provider<Executor> executorProvider,
                                  TransportActionProvider transportActionProvider,
                                  Planner planner) {
        this.clusterService = clusterService;
        this.blobIndices = blobIndices;
        this.executorProvider = executorProvider;
        this.tableCreator = tableCreator;
        this.transportActionProvider = transportActionProvider;
        this.planner = planner;
    }

    @Override
    protected ListenableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, UUID jobId) {
        throw new UnsupportedOperationException(String.format("Can't handle \"%s\"", analyzedStatement));
    }

   @Override
    public ListenableFuture<Long> visitCreateBlobTableStatement(
           CreateBlobTableAnalyzedStatement analysis, UUID jobId) {
        return wrapRowCountFuture(
                blobIndices.createBlobTable(
                        analysis.tableName(),
                        analysis.tableParameter().settings()
                ),
                1L
        );
    }

    @Override
    public ListenableFuture<Long> visitCreateTableStatement(CreateTableAnalyzedStatement analysis, UUID jobId) {
        return tableCreator.create(analysis);
    }

    @Override
    public ListenableFuture<Long> visitAddColumnStatement(final AddColumnAnalyzedStatement analysis, UUID jobId) {
        final SettableFuture<Long> result = SettableFuture.create();
        if (analysis.newPrimaryKeys()) {
            Plan plan = genCountStarPlan(analysis.table(), jobId);
            Job job = executorProvider.get().newJob(plan);
            ListenableFuture<List<TaskResult>> resultFuture = Futures.allAsList(executorProvider.get().execute(job));
            Futures.addCallback(resultFuture, new FutureCallback<List<TaskResult>>() {
                @Override
                public void onSuccess(@Nullable List<TaskResult> resultList) {
                    assert resultList != null && resultList.size() == 1;
                    Bucket rows = resultList.get(0).rows();
                    Row row = Iterables.getOnlyElement(rows);
                    if ((Long) row.get(0) == 0L) {
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

    private Plan genCountStarPlan(DocTableInfo table, UUID jobId) {
        QuerySpec querySpec = new QuerySpec();
        querySpec.where(WhereClause.MATCH_ALL);
        Function countFunction = new Function(
                CountAggregation.COUNT_STAR_FUNCTION, ImmutableList.<Symbol>of());
        querySpec.outputs(ImmutableList.<Symbol>of(countFunction));
        querySpec.hasAggregates(true);

        QueriedDocTable queriedTable = new QueriedDocTable(
                new DocTableRelation(table),
                ImmutableList.of(new OutputName("count(*)")),
                querySpec
                );
        SelectAnalyzedStatement statement = new SelectAnalyzedStatement(queriedTable);
        return planner.process(statement, new Planner.Context(clusterService, jobId, null));
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
    public ListenableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, UUID jobId) {
        return wrapRowCountFuture(
                blobIndices.alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings()),
                1L);
    }

    @Override
    public ListenableFuture<Long> visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, UUID jobId) {
        return wrapRowCountFuture(blobIndices.dropBlobTable(analysis.table().ident().name()), 1L);
    }

    private static String[] getIndexNames(DocTableInfo tableInfo, @Nullable PartitionName partitionName) {
        String[] indexNames;
        if (tableInfo.isPartitioned()) {
            if (partitionName == null) {
                // all partitions
                indexNames = tableInfo.concreteIndices();
            } else {
                // single partition
                indexNames = new String[] { partitionName.asIndexName() };
            }
        } else {
            indexNames = new String[] { tableInfo.ident().indexName() };
        }
        return indexNames;
    }

    @Override
    public ListenableFuture<Long> visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, UUID jobId) {
        if (analysis.indexNames().isEmpty()) {
            return Futures.immediateFuture(null);
        }
        final SettableFuture<Long> future = SettableFuture.create();
        RefreshRequest request = new RefreshRequest(analysis.indexNames().toArray(
                new String[analysis.indexNames().size()]));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

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
    public ListenableFuture<Long> visitAlterTableStatement(final AlterTableAnalyzedStatement analysis, UUID jobId) {
        DocTableInfo table = analysis.table();
        if (table.isAlias() && !table.isPartitioned()) {
            return Futures.immediateFailedFuture(new AlterTableAliasException(table.ident().fqn()));
        }

        List<ListenableFuture<Long>> results = new ArrayList<>(3);
        if (table.isPartitioned()) {
            // create new filtered partition table settings
            AlterPartitionedTableParameterInfo tableSettingsInfo =
                    (AlterPartitionedTableParameterInfo) table.tableParameterInfo();
            TableParameter parameterWithFilteredSettings = new TableParameter(
                    analysis.tableParameter().settings(),
                    tableSettingsInfo.partitionTableSettingsInfo().supportedInternalSettings());

            if (analysis.partitionName().isPresent()) {
                String index = analysis.partitionName().get().asIndexName();
                results.add(updateMapping(analysis.tableParameter().mappings(), index));
                results.add(updateSettings(parameterWithFilteredSettings, index));
            } else {
                // template gets all changes unfiltered
                results.add(updateTemplate(analysis.tableParameter(), table));

                if (!analysis.excludePartitions()) {
                    // resolve indices on master node to make sure it doesn't hit partitions that are being deleted
                    String index = table.ident().indexName();
                    results.add(updateMapping(analysis.tableParameter().mappings(), index));
                    results.add(updateSettings(parameterWithFilteredSettings, index));
                }
            }
        } else {
            results.add(updateMapping(analysis.tableParameter().mappings(), table.ident().indexName()));
            results.add(updateSettings(analysis.tableParameter(), table.ident().indexName()));
        }

        ListenableFuture<List<Long>> allAsList = Futures.allAsList(Iterables.filter(results, Predicates.notNull()));
        return Futures.transform(allAsList, Functions.<Long>constant(null));
    }

    private ListenableFuture<Long> updateTemplate(final TableParameter tableParameter,
                                                  TableInfo table) {
        final SettableFuture<Long> templateFuture = SettableFuture.create();

        // update template
        final String templateName = PartitionName.templateName(table.ident().schema(), table.ident().name());
        GetIndexTemplatesRequest getRequest = new GetIndexTemplatesRequest(templateName);

        transportActionProvider.transportGetIndexTemplatesAction().execute(getRequest, new ActionListener<GetIndexTemplatesResponse>() {
            @Override
            public void onResponse(GetIndexTemplatesResponse response) {
                IndexTemplateMetaData template = response.getIndexTemplates().get(0);
                Map<String, Object> mapping = mergeMapping(template, tableParameter.mappings());

                ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
                settingsBuilder.put(template.settings());
                settingsBuilder.put(tableParameter.settings());

                PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
                        .create(false)
                        .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                        .order(template.order())
                        .settings(settingsBuilder.build())
                        .template(template.template());
                for (ObjectObjectCursor<String, AliasMetaData> container : response.getIndexTemplates().get(0).aliases()) {
                    Alias alias = new Alias(container.key);
                    request.alias(alias);
                }

                transportActionProvider.transportPutIndexTemplateAction().execute(request,
                        new SettableFutureToNullActionListener<PutIndexTemplateResponse>(templateFuture));
            }

            @Override
            public void onFailure(Throwable e) {
                templateFuture.setException(e);
            }
        });

        return templateFuture;
    }

    private ListenableFuture<Long> updateMapping(Map<String, Object> mappings, String indexOrAlias) {
        if (mappings.isEmpty()) {
            return null;
        }

        Map<String, Object> mapping;
        try {
            MetaData metaData = clusterService.state().metaData();
            String index = metaData.concreteSingleIndex(indexOrAlias, IndicesOptions.lenientExpandOpen());
            mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
        } catch (IOException e) {
            return Futures.immediateFailedFuture(e);
        }
        XContentHelper.update(mapping, mappings, false);
        PutMappingRequest request = new PutMappingRequest(indexOrAlias);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        request.source(mapping);

        final SettableFuture<Long> future = SettableFuture.create();
        transportActionProvider.transportPutMappingAction().execute(request,
                new SettableFutureToNullActionListener<PutMappingResponse>(future));
        return future;
    }

    private ListenableFuture<Long> updateSettings(TableParameter concreteTableParameter, String... indices) {
        if (concreteTableParameter.settings().getAsMap().isEmpty() || indices.length == 0) {
            return null;
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(concreteTableParameter.settings(), indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        final SettableFuture<Long> future = SettableFuture.create();
        transportActionProvider.transportUpdateSettingsAction().execute(request,
                new SettableFutureToNullActionListener<UpdateSettingsResponse>(future));
        return future;
    }

    private static class SettableFutureToNullActionListener<T> implements ActionListener<T> {
        private final SettableFuture<?> future;

        public SettableFutureToNullActionListener(SettableFuture<?> future) {
            this.future = future;
        }

        @Override
        public void onResponse(T response) {
            future.set(null);
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }
}
