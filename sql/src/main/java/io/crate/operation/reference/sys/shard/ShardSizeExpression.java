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

package io.crate.operation.reference.sys.shard;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.shard.ShardReferenceImplementation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.IndexShard;

import java.util.concurrent.TimeUnit;

public class ShardSizeExpression extends SimpleObjectExpression<Long> implements ShardReferenceImplementation<Long> {

    public static final String NAME = "size";

    private final Supplier<Long> sizeSupplier;

    @Inject
    public ShardSizeExpression(final IndexShard indexShard) {
        sizeSupplier = Suppliers.memoizeWithExpiration(new Supplier<Long>() {
            @Override
            public Long get() {
                return indexShard.storeStats().getSizeInBytes();
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public Long value() {
        return sizeSupplier.get();
    }
}
