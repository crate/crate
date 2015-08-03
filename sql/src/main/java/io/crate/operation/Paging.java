/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation;

import javax.annotation.Nullable;

public class Paging {

    // this must not be final so tests could adjust it
    public static int DEFAULT_PAGE_SIZE = 1_000_000;

    public static int getNodePageSize(@Nullable Integer limit, int numTotalShards, int numShardsOnNode) {
        assert numTotalShards >= numShardsOnNode : "there can't be more shards on a node than there are shards in total";
        if (limit != null && limit < numTotalShards) {
            return limit;
        }
        return getShardPageSize(limit, numTotalShards) * Math.max(1, numShardsOnNode);
    }

    public static int getShardPageSize(@Nullable Integer limit, int numShardsEstimate) {
        if (limit == null || numShardsEstimate == 0) {
            return DEFAULT_PAGE_SIZE;
        }
        if (limit < numShardsEstimate) {
            return limit;
        }
        int i = (int) ((limit / numShardsEstimate) * 1.5);
        if (i > limit) {
            return limit;
        }
        return i;
    }
}
