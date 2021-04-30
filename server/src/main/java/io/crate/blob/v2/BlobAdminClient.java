/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.blob.v2;

import io.crate.action.FutureActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.CompletableFuture;

import static io.crate.blob.v2.BlobIndex.fullIndexName;
import static io.crate.blob.v2.BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED;

/**
 * DDL Client for blob tables - used to create, update or delete blob tables.
 */
@Singleton
public class BlobAdminClient {

    private final TransportCreateIndexAction createIndexAction;
    private final TransportDeleteIndexAction deleteIndexAction;

    @Inject
    public BlobAdminClient(TransportCreateIndexAction createIndexAction,
                           TransportDeleteIndexAction deleteIndexAction) {
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
    }

    public CompletableFuture<Long> createBlobTable(String tableName, Settings indexSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(indexSettings);
        builder.put(SETTING_INDEX_BLOBS_ENABLED.getKey(), true);

        FutureActionListener<CreateIndexResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(fullIndexName(tableName), builder.build());
        createIndexAction.execute(createIndexRequest, listener);
        return listener;
    }

    public CompletableFuture<Long> dropBlobTable(final String tableName) {
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        deleteIndexAction.execute(new DeleteIndexRequest(fullIndexName(tableName)), listener);
        return listener;
    }
}
