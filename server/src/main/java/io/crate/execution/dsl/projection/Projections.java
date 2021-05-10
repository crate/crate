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

package io.crate.execution.dsl.projection;

import com.google.common.collect.Collections2;
import io.crate.metadata.RowGranularity;

import java.util.Collection;

public class Projections {

    public static Collection<? extends Projection> shardProjections(Collection<? extends Projection> projections) {
        return Collections2.filter(projections, Projection.IS_SHARD_PROJECTION::test);
    }

    public static Collection<? extends Projection> nodeProjections(Collection<? extends Projection> projections) {
        return Collections2.filter(projections, Projection.IS_NODE_PROJECTION::test);
    }

    public static boolean hasAnyShardProjections(Iterable<? extends Projection> projections) {
        for (Projection projection : projections) {
            if (projection.requiredGranularity() == RowGranularity.SHARD) {
                return true;
            }
        }
        return false;
    }
}
