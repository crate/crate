/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.jetbrains.annotations.Nullable;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.types.DataTypes;

public class IndexUUID {

    public static Version INDICES_RESOLVED_BY_UUID_VERSION = Version.V_6_1_0;

    public static Supplier<String> createResolver(Metadata metadata,
                                                  RelationName relationName,
                                                  @Nullable String partitionIdent,
                                                  @Nullable List<Input<?>> partitionedByInputs) {
        if (partitionIdent == null && partitionedByInputs != null && partitionedByInputs.isEmpty() == false) {
            final List<Input<?>> partitionedByInputs1 = partitionedByInputs;
            final LoadingCache<List<String>, String> cache = Caffeine.newBuilder()
                .executor(Runnable::run)
                .initialCapacity(10)
                .maximumSize(20)
                .build(key -> {
                    List<String> indexUUIDS = metadata.getIndices(relationName, key, true, IndexMetadata::getIndexUUID);
                    return indexUUIDS.isEmpty() ? null : indexUUIDS.getFirst();
                });
            return () -> {
                // copy because the values of the inputs are mutable
                List<String> partitions = Lists.map(partitionedByInputs1, input -> DataTypes.STRING.implicitCast(input.value()));
                return cache.get(partitions);
            };
        }
        List<String> partitionedBy = PartitionName.decodeIdent(partitionIdent);

        return () -> {
            List<String> indexUUIDS = metadata.getIndices(relationName, partitionedBy, true, IndexMetadata::getIndexUUID);
            return indexUUIDS.isEmpty() ? null : indexUUIDS.getFirst();
        };
    }
}
