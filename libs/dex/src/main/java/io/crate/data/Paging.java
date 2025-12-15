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

package io.crate.data;

import org.jspecify.annotations.Nullable;
import java.lang.management.ManagementFactory;
import java.util.Objects;


public class Paging {

    // Indicates that no paging should be done but all results will be sent all at once.
    public static final int NO_PAGING = Integer.MAX_VALUE;

    // this must not be final so tests could adjust it
    public static int PAGE_SIZE = 500_000;
    public static final long MAX_PAGE_BYTES = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * 0.10);
    private static final double OVERHEAD_FACTOR = 1.5;

    public static int getWeightedPageSize(@Nullable Integer limit, double weight) {
        return getWeightedPageSize(limit, weight, OVERHEAD_FACTOR);
    }

    private static int getWeightedPageSize(@Nullable Integer limit, double weight, double overheadFactor) {
        Integer limitOrPageSize = Objects.requireNonNullElse(limit, PAGE_SIZE);
        if (1.0 / weight > limitOrPageSize) {
            return limitOrPageSize;
        }
        /* Don't adapt pageSize for small limits
         * The overhead of an internal "searchMore" / paging operation is bigger than it is to sort a couple of more records.
         *
         * Ex.
         *  SELECT x FROM t1 ORDER BY x LIMIT ?
         *  4 SHARDS:
         *  weight=0.25
         *
         *  with adaption    |   without
         *  -----------------+- ------------
         * limit | duration  |
         *    2  | 0.471     |    2 | 0.447
         *    5  | 1.772     |    5 | 0.447
         *   10  | 0.806     |   10 | 0.459
         *   25  | 0.806     |   25 | 0.481
         *   50  | 0.833     |   50 | 0.521
         *  100  | 0.863     |  100 | 0.565
         * 1600  | 1.351     | 1600 | 2.190
         *
         * There is a point where the adaption starts yielding better performance, but where this point is depends
         * on number-of-shards per node & data.
         * The 150 constant here was picked after graphing a few different configurations.
         */
        if (limitOrPageSize <= 150) {
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

}
