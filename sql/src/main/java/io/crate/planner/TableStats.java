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

package io.crate.planner;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.TableIdent;

/**
 * Holds table statistics that are updated periodically by {@link TableStatsService}.
 */
public class TableStats {

    private static final Stats EMPTY_STATS = new Stats();

    private volatile ObjectObjectMap<TableIdent, Stats> tableStats = new ObjectObjectHashMap<>(0);

    public void updateTableStats(ObjectObjectMap<TableIdent, Stats> tableStats) {
        this.tableStats = tableStats;
    }

    /**
     * Returns the number of docs a table has.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(TableIdent tableIdent) {
        return tableStats.getOrDefault(tableIdent, EMPTY_STATS).numDocs;
    }

    /**
     * Returns the size of the table in bytes.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns 0 if the table isn't in the cache
     */
    private long sizeInBytes(TableIdent tableIdent) {
        return tableStats.getOrDefault(tableIdent, EMPTY_STATS).sizeInBytes;
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long estimatedSizePerRow(TableIdent tableIdent) {
        long numDocs = numDocs(tableIdent);
        if (numDocs <= 0) {
            return numDocs;
        }
        return sizeInBytes(tableIdent) / numDocs(tableIdent);
    }

    @VisibleForTesting
    public static class Stats {

        @VisibleForTesting
        final long numDocs;
        @VisibleForTesting
        final long sizeInBytes;

        private Stats() {
            numDocs = -1;
            sizeInBytes = -1;
        }

        @VisibleForTesting
        public Stats(long numDocs, long sizeInBytes) {
            this.numDocs = numDocs;
            this.sizeInBytes = sizeInBytes;
        }
    }
}
