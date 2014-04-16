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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.*;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.AlterTableAliasException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 *
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
public class DDLAnalysisDispatcher extends AnalysisVisitor<Void, ListenableFuture<Long>> {

    private final BlobIndices blobIndices;
    private final TransportRefreshAction transportRefreshAction;
    private final TransportUpdateSettingsAction transportUpdateSettingsAction;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final TransportGetIndexTemplatesAction transportGetIndexTemplatesAction;

    @Inject
    public DDLAnalysisDispatcher(BlobIndices blobIndices,
                                  TransportRefreshAction transportRefreshAction,
                                  TransportUpdateSettingsAction transportUpdateSettingsAction,
                                  TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                  TransportGetIndexTemplatesAction transportGetIndexTemplatesAction) {
        this.blobIndices = blobIndices;
        this.transportRefreshAction = transportRefreshAction;
        this.transportUpdateSettingsAction = transportUpdateSettingsAction;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.transportGetIndexTemplatesAction = transportGetIndexTemplatesAction;
    }

    @Override
    protected ListenableFuture<Long> visitAnalysis(Analysis analysis, Void context) {
        throw new UnsupportedOperationException(String.format("Can't handle \"%s\"", analysis));
    }

    @Override
    public ListenableFuture<Long> visitCreateBlobTableAnalysis(
            CreateBlobTableAnalysis analysis, Void context) {
        return wrapRowCountFuture(
                blobIndices.createBlobTable(
                    analysis.tableName(),
                    analysis.numberOfReplicas(),
                    analysis.numberOfShards()
                ),
                1L
        );
    }

    @Override
    public ListenableFuture<Long> visitAlterBlobTableAnalysis(AlterBlobTableAnalysis analysis, Void context) {
        return wrapRowCountFuture(
                blobIndices.alterBlobTable(analysis.table().ident().name(), analysis.numberOfReplicas()),
                1L);
    }

    @Override
    public ListenableFuture<Long> visitDropBlobTableAnalysis(DropBlobTableAnalysis analysis, Void context) {
        return wrapRowCountFuture(blobIndices.dropBlobTable(analysis.table().ident().name()), 1L);
    }

    @Override
    public ListenableFuture<Long> visitRefreshTableAnalysis(RefreshTableAnalysis analysis, Void context) {
        final SettableFuture<Long> future = SettableFuture.create();
        String[] indexNames;
        if (analysis.table().isPartitioned()) {
            if (analysis.partitionName() == null) {
                // refresh all partitions
                indexNames = analysis.table().concreteIndices();
            } else {
                // refresh a single partition
                indexNames = new String[]{ analysis.partitionName().stringValue() };
            }

        } else {
            indexNames = new String[]{analysis.table().ident().name()};
        }
        if (analysis.schema().systemSchema() || indexNames.length == 0) {
            future.set(null); // shortcut when refreshing on system tables
                              // or empty partitioned tables
        } else {
            RefreshRequest request = new RefreshRequest(indexNames);
            transportRefreshAction.execute(request, new ActionListener<RefreshResponse>() {
                @Override
                public void onResponse(RefreshResponse refreshResponse) {
                    future.set(null); // no row count
                }

                @Override
                public void onFailure(Throwable e) {
                    future.setException(e);
                }
            });
        }
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
    public ListenableFuture<Long> visitAlterTableAnalysis(final AlterTableAnalysis analysis, Void context) {
        final SettableFuture<Long> result = SettableFuture.create();
        final String[] indices = analysis.table().isPartitioned()
                ? analysis.table().concreteIndices()
                : new String[]{ analysis.table().ident().name() };

        if (analysis.table().isAlias()) {
            throw new AlterTableAliasException(analysis.table().ident().name());
        }

        final List<ListenableFuture<?>> results = new ArrayList<>(
                indices.length + (analysis.table().isPartitioned() ? 1 : 0)
        );
        if (analysis.table().isPartitioned()) {
            final SettableFuture<?> templateFuture = SettableFuture.create();
            results.add(templateFuture);

            // update template
            final String templateName = PartitionName.templateName(analysis.table().ident().name());
            GetIndexTemplatesRequest getRequest = new GetIndexTemplatesRequest(templateName);

            transportGetIndexTemplatesAction.execute(getRequest, new ActionListener<GetIndexTemplatesResponse>() {
                @Override
                public void onResponse(GetIndexTemplatesResponse response) {
                    String mapping;
                    try {
                        mapping = response.getIndexTemplates().get(0).getMappings().get(Constants.DEFAULT_MAPPING_TYPE).string();
                    } catch (IOException e) {
                        templateFuture.setException(e);
                        return;
                    }
                    ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
                    settingsBuilder.put(response.getIndexTemplates().get(0).settings());
                    settingsBuilder.put(analysis.settings());

                    PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
                            .create(false)
                            .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                            .settings(settingsBuilder.build())
                            .template(response.getIndexTemplates().get(0).template());
                    for (ObjectObjectCursor<String, AliasMetaData> container : response.getIndexTemplates().get(0).aliases()) {
                        Alias alias = new Alias(container.key);
                        request.alias(alias);
                    }
                    transportPutIndexTemplateAction.execute(request, new ActionListener<PutIndexTemplateResponse>() {
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
        // update every concrete index
        for (String index : indices) {
            UpdateSettingsRequest request = new UpdateSettingsRequest(
                    analysis.settings(),
                    index);
            final SettableFuture<?> future = SettableFuture.create();
            results.add(future);
            transportUpdateSettingsAction.execute(request, new ActionListener<UpdateSettingsResponse>() {
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
        Futures.addCallback(Futures.allAsList(results), new FutureCallback<List<?>>() {
            @Override
            public void onSuccess(@Nullable List<?> resultList) {
                result.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                result.setException(t);
            }
        });

        return result;
    }
}
