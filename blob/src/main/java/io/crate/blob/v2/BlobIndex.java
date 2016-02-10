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

package io.crate.blob.v2;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

public class BlobIndex extends AbstractIndexComponent {


    private final OperationRouting operationRouting;
    private final ClusterService clusterService;

    @Inject
    BlobIndex(Index index, Settings indexSettings,
            OperationRouting operationRouting,
            ClusterService clusterService) {
        super(index, indexSettings);
        this.operationRouting = operationRouting;
        this.clusterService = clusterService;
    }

    public ShardId shardId(String digest) {
        ShardIterator si = operationRouting.getShards(clusterService.state(), index.getName(), null, null, digest, "_only_local");
        // TODO: check null and raise
        return si.shardId();
    }

}
