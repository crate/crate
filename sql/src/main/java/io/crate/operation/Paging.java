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

import io.crate.planner.Plan;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.firstNonNull;

public class Paging {

    // this must not be final so tests could adjust it
    public static int PAGE_SIZE = 500_000;
    private static final double OVERHEAD_FACTOR = 1.5;

    public static int getWeightedPageSize(@Nullable Integer limit, double weight) {
        return getWeightedPageSize(limit, weight, OVERHEAD_FACTOR);
    }

    private static int getWeightedPageSize(@Nullable Integer limit, double weight, double overheadFactor) {
        Integer limitOrPageSize = firstNonNull(limit, PAGE_SIZE);
        if (1.0 / weight > limitOrPageSize) {
            return limitOrPageSize;
        }
        int dynPageSize = Math.max((int) (limitOrPageSize * weight * overheadFactor), 1);
        if (limit == null) {
            return dynPageSize;
        }
        return Math.min(dynPageSize, limit);
    }

    public static boolean shouldPage(int maxRowsPerNode) {
        return maxRowsPerNode == -1 || maxRowsPerNode > PAGE_SIZE;
    }

    public static void updateNodePageSizeHint(Plan subPlan, int nodePageSize) {
        if (!(subPlan instanceof Collect) || nodePageSize == -1) {
            return;
        }
        CollectPhase collectPhase = ((Collect) subPlan).collectPhase();
        if (collectPhase instanceof RoutedCollectPhase) {
            ((RoutedCollectPhase) collectPhase).pageSizeHint(nodePageSize);
        }
    }
}
