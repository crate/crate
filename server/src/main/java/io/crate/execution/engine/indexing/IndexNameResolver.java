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

package io.crate.execution.engine.indexing;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

import static io.crate.common.StringUtils.nullOrString;

public class IndexNameResolver {

    private IndexNameResolver() {
    }

    public static Supplier<String> create(RelationName relationName,
                                          @Nullable String partitionIdent,
                                          @Nullable List<Input<?>> partitionedByInputs) {
        if (partitionIdent == null && (partitionedByInputs == null || partitionedByInputs.isEmpty())) {
            return forTable(relationName);
        }
        if (partitionIdent == null) {
            return forPartition(relationName, partitionedByInputs);
        }
        return forPartition(relationName, partitionIdent);
    }

    public static Supplier<String> forTable(final RelationName relationName) {
        return relationName::indexNameOrAlias;
    }

    private static Supplier<String> forPartition(RelationName relationName, String partitionIdent) {
        return () -> IndexParts.toIndexName(relationName, partitionIdent);
    }

    private static Supplier<String> forPartition(final RelationName relationName, final List<Input<?>> partitionedByInputs) {
        assert partitionedByInputs.size() > 0 : "must have at least 1 partitionedByInput";
        final LoadingCache<List<String>, String> cache = CacheBuilder.newBuilder()
            .initialCapacity(10)
            .maximumSize(20)
            .build(new CacheLoader<List<String>, String>() {
                @Override
                public String load(@Nonnull List<String> key) {
                    return IndexParts.toIndexName(relationName, PartitionName.encodeIdent(key));
                }
            });
        return () -> {
            // copy because the values of the inputs are mutable
            List<String> partitions = Lists2.map(partitionedByInputs, input -> nullOrString(input.value()));
            return cache.getUnchecked(partitions);
        };
    }
}
