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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.*;
import io.crate.blob.v2.BlobIndices;
import io.crate.executor.transport.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.UUID;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 *
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
@Singleton
public class DDLStatementDispatcher {

    private final BlobIndices blobIndices;
    private final TransportActionProvider transportActionProvider;
    private final TableCreator tableCreator;
    private final AlterTableOperation alterTableOperation;
    private final RepositoryDDLDispatcher repositoryDDLDispatcher;
    private final SnapshotDDLDispatcher snapshotDDLDispatcher;

    private final InnerVisitor innerVisitor = new InnerVisitor();


    @Inject
    public DDLStatementDispatcher(BlobIndices blobIndices,
                                  TableCreator tableCreator,
                                  AlterTableOperation alterTableOperation,
                                  RepositoryDDLDispatcher repositoryDDLDispatcher,
                                  SnapshotDDLDispatcher snapshotDDLDispatcher,
                                  TransportActionProvider transportActionProvider) {
        this.blobIndices = blobIndices;
        this.tableCreator = tableCreator;
        this.alterTableOperation = alterTableOperation;
        this.transportActionProvider = transportActionProvider;
        this.repositoryDDLDispatcher = repositoryDDLDispatcher;
        this.snapshotDDLDispatcher = snapshotDDLDispatcher;
    }

    public ListenableFuture<Long> dispatch(AnalyzedStatement analyzedStatement, UUID jobId) {
        return innerVisitor.process(analyzedStatement, jobId);
    }

    private class InnerVisitor extends AnalyzedStatementVisitor<UUID, ListenableFuture<Long>> {

        @Override
        protected ListenableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, UUID jobId) {
            throw new UnsupportedOperationException(String.format("Can't handle \"%s\"", analyzedStatement));
        }


        @Override
        public ListenableFuture<Long> visitCreateTableStatement(CreateTableAnalyzedStatement analysis, UUID jobId) {
            return tableCreator.create(analysis);
        }

        @Override
        public ListenableFuture<Long> visitAlterTableStatement(final AlterTableAnalyzedStatement analysis, UUID jobId) {
            return alterTableOperation.executeAlterTable(analysis);
        }

        @Override
        public ListenableFuture<Long> visitAddColumnStatement(AddColumnAnalyzedStatement analysis, UUID context) {
            return alterTableOperation.executeAlterTableAddColumn(analysis);
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
        public ListenableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, UUID jobId) {
            return wrapRowCountFuture(
                    blobIndices.alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings()),
                    1L);
        }

        @Override
        public ListenableFuture<Long> visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, UUID jobId) {
            return wrapRowCountFuture(blobIndices.dropBlobTable(analysis.table().ident().name()), 1L);
        }

        @Override
        public ListenableFuture<Long> visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis, UUID jobId) {
            return repositoryDDLDispatcher.dispatch(analysis);
        }

        @Override
        public ListenableFuture<Long> visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis, UUID jobId) {
            return repositoryDDLDispatcher.dispatch(analysis);
        }

        @Override
        public ListenableFuture<Long> visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis, UUID jobId) {
            return snapshotDDLDispatcher.dispatch(analysis);
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
}
