/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.partitioned.PartitionedColumnExpression;
import io.crate.operation.reference.sys.shard.*;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.Locale;

public class ShardReferenceResolver extends AbstractReferenceResolver {

    private static final Logger LOGGER = Loggers.getLogger(ShardReferenceResolver.class);

    public ShardReferenceResolver(ClusterService clusterService,
                                  Schemas schemas,
                                  IndexShard indexShard) {
        ShardId shardId = indexShard.shardId();
        Index index = shardId.getIndex();

        ImmutableMap.Builder<ReferenceIdent, ReferenceImplementation> builder = ImmutableMap.builder();
        if (PartitionName.isPartition(index.getName())) {
            addPartitions(index, schemas, builder);
        }
        implementations.put(SysShardsTableInfo.ReferenceIdents.ID, new LiteralReferenceImplementation<>(shardId.getId()));
        implementations.put(SysShardsTableInfo.ReferenceIdents.SIZE, new ShardSizeExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.NUM_DOCS, new ShardNumDocsExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PRIMARY, new ShardPrimaryExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.RELOCATING_NODE,
            new ShardRelocatingNodeExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.SCHEMA_NAME,
            new ShardSchemaNameExpression(shardId));
        implementations.put(SysShardsTableInfo.ReferenceIdents.STATE, new ShardStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.ROUTING_STATE, new ShardRoutingStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.TABLE_NAME, new ShardTableNameExpression(shardId));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PARTITION_IDENT,
            new ShardPartitionIdentExpression(shardId));
        implementations.put(SysShardsTableInfo.ReferenceIdents.ORPHAN_PARTITION,
            new ShardPartitionOrphanedExpression(shardId, clusterService));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PATH, new ShardPathExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.BLOB_PATH, new LiteralReferenceImplementation<>(null));
        implementations.put(SysShardsTableInfo.ReferenceIdents.MIN_LUCENE_VERSION,
            new ShardMinLuceneVersionExpression(indexShard));
        this.implementations.putAll(builder.build());
    }

    private void addPartitions(Index index,
                               Schemas schemas,
                               ImmutableMap.Builder<ReferenceIdent, ReferenceImplementation> builder) {
        PartitionName partitionName;
        try {
            partitionName = PartitionName.fromIndexOrTemplate(index.getName());
        } catch (IllegalArgumentException e) {
            throw new UnhandledServerException(String.format(Locale.ENGLISH,
                "Unable to load PARTITIONED BY columns from partition %s", index.getName()), e);
        }
        TableIdent tableIdent = partitionName.tableIdent();
        try {
            DocTableInfo info = (DocTableInfo) schemas.getTableInfo(tableIdent);
            if (!schemas.isOrphanedAlias(info)) {
                assert info.isPartitioned() : "table must be partitioned";
                int i = 0;
                int numPartitionedColumns = info.partitionedByColumns().size();

                assert partitionName.values().size() ==
                       numPartitionedColumns : "invalid number of partitioned columns";
                for (Reference partitionedInfo : info.partitionedByColumns()) {
                    builder.put(partitionedInfo.ident(), new PartitionedColumnExpression(
                        partitionedInfo,
                        partitionName.values().get(i)
                    ));
                    i++;
                }
            } else {
                LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, tableIdent.fqn());
            }
        } catch (ResourceUnknownException e) {
            LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, tableIdent.fqn());
        }
    }
}
