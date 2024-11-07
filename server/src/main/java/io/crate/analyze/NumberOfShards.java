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

package io.crate.analyze;

import java.util.Locale;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.sql.tree.ClusteredBy;
import io.crate.types.DataTypes;

@Singleton
public class NumberOfShards {

    private static final Integer MIN_NUM_SHARDS = 4;

    private final ClusterService clusterService;

    @Inject
    public NumberOfShards(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public int fromNumberOfShards(Object numberOfShards) {
        if (!(numberOfShards instanceof Number)) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "invalid number '%s'", numberOfShards));
        }
        var numShards = DataTypes.INTEGER.sanitizeValue(numberOfShards);
        if (numShards < 1) {
            throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
        }
        return numShards;
    }

    public int fromClusteredByClause(ClusteredBy<Object> clusteredBy) {
        return clusteredBy.numberOfShards()
            .map(this::fromNumberOfShards)
            .orElseGet(this::defaultNumberOfShards);
    }

    public int defaultNumberOfShards() {
        int numDataNodes = clusterService.state().nodes().getDataNodes().size();
        assert numDataNodes >= 0 : "number of data nodes cannot be less than 0";
        return Math.max(MIN_NUM_SHARDS, numDataNodes * 2);
    }
}
