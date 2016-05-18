/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.sql.tree.ClusteredBy;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class NumberOfShards {

    static final Integer MIN_NUM_SHARDS = 4;

    private final ClusterService clusterService;

    @Inject
    public NumberOfShards(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    int fromClusteredByClause(ClusteredBy clusteredBy, Object[] parameters) {
        if (clusteredBy.numberOfShards().isPresent()) {
            int numShards = ExpressionToNumberVisitor.convert(clusteredBy.numberOfShards().get(), parameters).intValue();
            if (numShards < 1) {
                throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
            }
            return numShards;
        }
        return defaultNumberOfShards();
    }

    int defaultNumberOfShards() {
        int numDataNodes = clusterService.state().nodes().dataNodes().size();
        assert numDataNodes > 0 : "number of data nodes cannot be less than 0";
        int numShards = numDataNodes * 2;
        return numShards < MIN_NUM_SHARDS ? MIN_NUM_SHARDS : numShards;
    }

}
