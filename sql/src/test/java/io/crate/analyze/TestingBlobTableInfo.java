/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.Version;
import io.crate.action.sql.SessionContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobTableInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;

class TestingBlobTableInfo extends BlobTableInfo {

    private final Routing routing;

    TestingBlobTableInfo(RelationName ident,
                         String index,
                         int numberOfShards,
                         BytesRef numberOfReplicas,
                         ImmutableMap<String, Object> tableParameters,
                         BytesRef blobsPath,
                         Routing routing) {
        super(
            ident,
            index,
            numberOfShards,
            numberOfReplicas,
            tableParameters,
            blobsPath,
            Version.CURRENT,
            null,
            false);
        this.routing = routing;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return routing;
    }
}
