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

package io.crate.operation.reference.sys.node.local.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceImplementation;
import io.crate.monitor.ExtendedFsStats;
import io.crate.operation.reference.NestedObjectExpression;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class NodeFsTotalExpression extends NestedObjectExpression {

    private static final String SIZE = "size";
    private static final String USED = "used";
    private static final String AVAILABLE = "available";
    private static final String READS = "reads";
    private static final String BYTES_READ = "bytes_read";
    private static final String WRITES = "writes";
    private static final String BYTES_WRITTEN = "bytes_written";

    private static final List<String> ALL_TOTALS = ImmutableList.of(
        SIZE, USED, AVAILABLE, READS, BYTES_READ, WRITES, BYTES_WRITTEN);
    private static final Logger logger = Loggers.getLogger(NodeFsTotalExpression.class);

    private final ExtendedFsStats extendedFsStats;


    // cache that collects all totals at once, even if only one total value is queried
    private final LoadingCache<String, Long> totals = CacheBuilder.newBuilder()
        .expireAfterWrite(500, TimeUnit.MILLISECONDS)
        .maximumSize(ALL_TOTALS.size())
        .build(new CacheLoader<String, Long>() {
            @Override
            public Long load(@Nonnull String key) throws Exception {
                // actually not needed if only queried with getAll()
                throw new UnsupportedOperationException("load not supported on sys.nodes.fs.total cache");
            }

            @Override
            public Map<String, Long> loadAll(@Nonnull Iterable<? extends String> keys) throws Exception {
                return getTotals();
            }
        });

    NodeFsTotalExpression(ExtendedFsStats extendedFsStats) {
        this.extendedFsStats = extendedFsStats;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(SIZE, new NodeFSTotalChildExpression(SIZE));
        childImplementations.put(USED, new NodeFSTotalChildExpression(USED));
        childImplementations.put(AVAILABLE, new NodeFSTotalChildExpression(AVAILABLE));
        childImplementations.put(READS, new NodeFSTotalChildExpression(READS));
        childImplementations.put(BYTES_READ, new NodeFSTotalChildExpression(BYTES_READ));
        childImplementations.put(WRITES, new NodeFSTotalChildExpression(WRITES));
        childImplementations.put(BYTES_WRITTEN, new NodeFSTotalChildExpression(BYTES_WRITTEN));
    }

    private Map<String, Long> getTotals() {
        Map<String, Long> totals = new HashMap<>(ALL_TOTALS.size());
        ExtendedFsStats.Info totalInfo = extendedFsStats.total();
        totals.put(SIZE, totalInfo.total() == -1 ? -1 : totalInfo.total() * 1024);
        totals.put(USED, totalInfo.used() == -1 ? -1 : totalInfo.used() * 1024);
        totals.put(AVAILABLE, totalInfo.available() == -1 ? -1 : totalInfo.available() * 1024);
        totals.put(READS, totalInfo.diskReads());
        totals.put(BYTES_READ, totalInfo.diskReadSizeInBytes());
        totals.put(WRITES, totalInfo.diskWrites());
        totals.put(BYTES_WRITTEN, totalInfo.diskWriteSizeInBytes());
        return totals;
    }

    private class NodeFSTotalChildExpression implements ReferenceImplementation<Long> {

        private final String name;

        NodeFSTotalChildExpression(String name) {
            this.name = name;
        }

        @Override
        public Long value() {
            try {
                return totals.getAll(ALL_TOTALS).get(name);
            } catch (ExecutionException e) {
                logger.trace("error getting fs {} total", e, name);
                return null;
            }
        }
    }
}
