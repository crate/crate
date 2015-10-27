/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

@Singleton
public class SnapshotDDLDispatcher {

    private static final ESLogger LOGGER = Loggers.getLogger(SnapshotDDLDispatcher.class);
    private final TransportActionProvider transportActionProvider;

    @Inject
    public SnapshotDDLDispatcher(TransportActionProvider transportActionProvider) {
        this.transportActionProvider = transportActionProvider;
    }

    public ListenableFuture<Long> dispatch(final DropSnapshotAnalyzedStatement statement) {
        final SettableFuture<Long> future = SettableFuture.create();
        final String repositoryName = statement.repository();
        final String snapshotName = statement.snapshot();

        transportActionProvider.transportDeleteSnapshotAction().execute(
                new DeleteSnapshotRequest(repositoryName, snapshotName),
                new ActionListener<DeleteSnapshotResponse>() {
                    @Override
                    public void onResponse(DeleteSnapshotResponse response) {
                        if (!response.isAcknowledged()) {
                            LOGGER.info("delete snapshot '{}.{}' not acknowledged", repositoryName, snapshotName);
                        }
                        future.set(1L);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        future.setException(e);
                    }
                }
        );
        return future;

    }
}