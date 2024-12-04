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

package io.crate.metadata.shard;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.Index;

import io.crate.common.collections.MapBuilder;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexParts;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;

public class ShardReferenceResolver implements ReferenceResolver<NestableInput<?>> {

    private static final Logger LOGGER = LogManager.getLogger(ShardReferenceResolver.class);
    // Use a dummy Roles (List::of) here, as we resolve privileges at a table level, not a shard level,
    // and the shards need to only be protected (resolve privileges) in SysShardsTableInfo (sys.shards)
    private static final StaticTableReferenceResolver<ShardRowContext> SHARD_REFERENCE_RESOLVER_DELEGATE =
        new StaticTableReferenceResolver<>(SysShardsTableInfo.create(List::of).expressions());
    private static final ReferenceResolver<NestableInput<?>> EMPTY_RESOLVER =
        new MapBackedRefResolver(Collections.emptyMap());

    private static ReferenceResolver<NestableInput<?>> createPartitionColumnResolver(Index index, Schemas schemas) {
        PartitionName partitionName;
        try {
            partitionName = PartitionName.fromIndexOrTemplate(index.getName());
        } catch (IllegalArgumentException e) {
            throw new UnhandledServerException(String.format(Locale.ENGLISH,
                "Unable to load PARTITIONED BY columns from partition %s", index.getName()), e);
        }
        RelationName relationName = partitionName.relationName();
        MapBuilder<ColumnIdent, NestableInput<?>> builder = MapBuilder.newMapBuilder();
        try {
            DocTableInfo info = schemas.getTableInfo(relationName);
            assert info.isPartitioned() : "table must be partitioned";
            int i = 0;
            int numPartitionedColumns = info.partitionedByColumns().size();

            List<String> partitionValue = partitionName.values();
            assert partitionValue.size() ==
                   numPartitionedColumns : "invalid number of partitioned columns";
            for (Reference partitionedInfo : info.partitionedByColumns()) {
                builder.put(
                    partitionedInfo.column(),
                    constant(partitionedInfo.valueType().implicitCast(partitionValue.get(i)))
                );
                i++;
            }
        } catch (Exception e) {
            if (e instanceof ResourceUnknownException) {
                LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, relationName.fqn());
            } else {
                throw e;
            }
        }
        return new MapBackedRefResolver(builder.immutableMap());
    }

    private final ShardRowContext shardRowContext;
    private final ReferenceResolver<NestableInput<?>> partitionColumnResolver;

    public ShardReferenceResolver(Schemas schemas, ShardRowContext shardRowContext) {
        this.shardRowContext = shardRowContext;
        IndexParts indexParts = shardRowContext.indexParts();
        if (indexParts.isPartitioned()) {
            partitionColumnResolver = createPartitionColumnResolver(
                shardRowContext.indexShard().shardId().getIndex(), schemas);
        } else {
            partitionColumnResolver = EMPTY_RESOLVER;
        }
    }

    @Override
    public NestableInput<?> getImplementation(Reference ref) {
        NestableInput<?> partitionColImpl = partitionColumnResolver.getImplementation(ref);
        if (partitionColImpl != null) {
            return partitionColImpl;
        }
        NestableCollectExpression<ShardRowContext, ?> impl = SHARD_REFERENCE_RESOLVER_DELEGATE.getImplementation(ref);
        impl.setNextRow(shardRowContext);
        return impl;
    }
}
