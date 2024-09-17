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

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;

import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;

import io.crate.expression.reference.sys.shard.ShardSegment;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public class SysSegmentsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "segments");

    @SuppressWarnings("unchecked")
    public static SystemTable<ShardSegment> create(Supplier<DiscoveryNode> localNode) {
        return SystemTable.<ShardSegment>builder(IDENT)
            .add("table_schema", STRING, r -> r.getIndexParts().schema())
            .add("table_name", STRING, r -> r.getIndexParts().table())
            .add("partition_ident", STRING, r -> r.getIndexParts().partitionIdent())
            .add("shard_id", INTEGER, ShardSegment::getShardId)
            .startObject("node")
                .add("id", STRING, ignored -> localNode.get().getId())
                .add("name", STRING, ignored -> localNode.get().getName())
            .endObject()
            .add("segment_name", STRING, r -> r.getSegment().getName())
            .add("generation", LONG, r -> r.getSegment().getGeneration())
            .add("num_docs", INTEGER, r -> r.getSegment().getNumDocs())
            .add("deleted_docs", INTEGER, r -> r.getSegment().getDeletedDocs())
            .add("size", LONG, r -> r.getSegment().sizeInBytes)
            .add("memory", LONG, r -> -1L) // since Lucene9 r.getSegment().memoryInBytes is no longer accounted
            .add("committed", BOOLEAN, r -> r.getSegment().committed)
            .add("primary", BOOLEAN, ShardSegment::primary)
            .add("search", BOOLEAN, r -> r.getSegment().search)
            .add("version", STRING, r -> r.getSegment().getVersion().toString())
            .add("compound", BOOLEAN, r -> r.getSegment().compound)
            .add("attributes", DataTypes.UNTYPED_OBJECT, r -> (Map<String, Object>) (Map<?, ?>) r.getSegment().getAttributes())
            .withRouting((state, routingProvider, sessionSettings) -> Routing.forTableOnAllNodes(IDENT, state.nodes()))
            .build();
    }
}
