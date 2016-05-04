/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.UUID;

public class UpdateProjector extends DMLProjector<ShardUpsertRequest> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final String[] assignmentsColumns;
    private final Symbol[] assignments;
    @Nullable
    private final Long requiredVersion;

    public UpdateProjector(ClusterService clusterService,
                           IndexNameExpressionResolver indexNameExpressionResolver,
                           Settings settings,
                           ShardId shardId,
                           TransportActionProvider transportActionProvider,
                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                           CollectExpression<Row, ?> collectUidExpression,
                           String[] assignmentsColumns,
                           Symbol[] assignments,
                           @Nullable Long requiredVersion,
                           UUID jobId) {
        super(clusterService, settings, shardId, transportActionProvider, bulkRetryCoordinatorPool,
                collectUidExpression, jobId);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.assignmentsColumns = assignmentsColumns;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
    }

    @Override
    protected BulkShardProcessor<ShardUpsertRequest> createBulkShardProcessor(int bulkSize) {
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                false,
                false,
                assignmentsColumns,
                null,
                jobId
        );
        return new BulkShardProcessor<>(
                clusterService,
                transportActionProvider.transportBulkCreateIndicesAction(),
                indexNameExpressionResolver,
                settings,
                bulkRetryCoordinatorPool,
                false,
                DEFAULT_BULK_SIZE,
                builder,
                transportActionProvider.transportShardUpsertActionDelegate(),
                jobId
        );
    }

    @Override
    protected ShardRequest.Item createItem(String id) {
        return new ShardUpsertRequest.Item(id, assignments, null, requiredVersion);
    }
}
