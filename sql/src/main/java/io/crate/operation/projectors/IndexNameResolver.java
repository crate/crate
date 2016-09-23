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

package io.crate.operation.projectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.Inputs;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class IndexNameResolver {

    private IndexNameResolver() {
    }

    ;

    public static Supplier<String> create(TableIdent tableIdent,
                                          @Nullable String partitionIdent,
                                          @Nullable List<Input<?>> partitionedByInputs) {
        if (partitionIdent == null && (partitionedByInputs == null || partitionedByInputs.isEmpty())) {
            return forTable(tableIdent);
        }
        if (partitionIdent == null) {
            return forPartition(tableIdent, partitionedByInputs);
        }
        return forPartition(tableIdent, partitionIdent);
    }

    public static Supplier<String> forTable(final TableIdent tableIdent) {
        return Suppliers.ofInstance(tableIdent.indexName());
    }

    public static Supplier<String> forPartition(TableIdent tableIdent, String partitionIdent) {
        return Suppliers.ofInstance(PartitionName.indexName(tableIdent, partitionIdent));
    }

    public static Supplier<String> forPartition(final TableIdent tableIdent, final List<Input<?>> partitionedByInputs) {
        assert partitionedByInputs.size() > 0 : "must have at least 1 partitionedByInput";
        final LoadingCache<List<BytesRef>, String> cache = CacheBuilder.newBuilder()
            .initialCapacity(10)
            .maximumSize(20)
            .build(new CacheLoader<List<BytesRef>, String>() {
                @Override
                public String load(@Nonnull List<BytesRef> key) throws Exception {
                    return PartitionName.indexName(tableIdent, PartitionName.encodeIdent(key));
                }
            });
        return new Supplier<String>() {

            @Override
            public String get() {
                // copy because transform returns a view and the values of the inputs are mutable
                List<BytesRef> partitions = Collections.unmodifiableList(
                    Lists.newArrayList(Lists.transform(
                        partitionedByInputs,
                        Inputs.TO_BYTES_REF)));
                try {
                    return cache.get(partitions);
                } catch (ExecutionException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
