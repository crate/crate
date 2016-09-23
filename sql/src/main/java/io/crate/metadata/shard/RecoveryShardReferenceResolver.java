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

package io.crate.metadata.shard;

import io.crate.metadata.AbstractReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.sys.shard.ShardRecoveryExpression;
import org.elasticsearch.index.shard.IndexShard;


public class RecoveryShardReferenceResolver extends AbstractReferenceResolver {

    /**
     * <p>
     * The RecoveryShardReferenceResolver is necessary to be able to instantiate
     * the ShardRecoveryExpression at runtime.
     * This is required because the ShardRecoveryExpression needs to push the same recoveryState
     * to all of its childImplementations in order to receive the same state
     * when having multiple recovery objects in the select list of a query, e.g.
     * </p>
     * <code>
     * SELECT recovery['size'], recovery['files'] FROM sys.nodes;
     * </code>
     */

    private final ReferenceResolver<ReferenceImplementation<?>> staticReferencesResolver;

    public RecoveryShardReferenceResolver(ReferenceResolver<ReferenceImplementation<?>> shardResolver, IndexShard indexShard) {
        staticReferencesResolver = shardResolver;
        implementations.put(SysShardsTableInfo.ReferenceIdents.RECOVERY,
            new ShardRecoveryExpression(indexShard));
    }

    @Override
    public ReferenceImplementation getImplementation(Reference refInfo) {
        ReferenceImplementation impl = staticReferencesResolver.getImplementation(refInfo);
        if (impl == null) {
            impl = super.getImplementation(refInfo);
        }
        return impl;
    }

}
