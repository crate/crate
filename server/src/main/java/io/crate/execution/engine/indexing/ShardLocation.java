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

import org.elasticsearch.index.shard.ShardId;

public class ShardLocation {
    final ShardId shardId;
    final String nodeId;

    public ShardLocation(ShardId shardId, String nodeId) {
        this.shardId = shardId;
        this.nodeId = nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardLocation that = (ShardLocation) o;

        if (!shardId.equals(that.shardId)) return false;
        return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
        return result;
    }

}
