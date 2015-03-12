/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.node.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarPermissionDeniedException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class NodeFsTotalExpression extends SysNodeObjectReference {

    public static final String NAME = "total";

    public static final String SIZE = "size";
    public static final String USED = "used";
    public static final String AVAILABLE = "available";
    public static final String READS = "reads";
    public static final String BYTES_READ = "bytes_read";
    public static final String WRITES = "writes";
    public static final String BYTES_WRITTEN = "bytes_written";

    private static final List<String> ALL_TOTALS = ImmutableList.of(
            SIZE, USED, AVAILABLE, READS, BYTES_READ, WRITES, BYTES_WRITTEN);
    private static final ESLogger logger = Loggers.getLogger(NodeFsTotalExpression.class);

    private final SigarService sigarService;

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

    protected NodeFsTotalExpression(SigarService sigarService) {
        this.sigarService = sigarService;
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

    private Map<String,Long> getTotals() {
        Map<String, Long> totals = new HashMap<>(7);
        long size=-1L, used=-1L, available=-1L,
             reads=-1L, bytes_read=-1L,
             writes=-1L, bytes_written=-1L;
        if (sigarService.sigarAvailable()) {
            try {
                for (FileSystem fs : sigarService.sigar().getFileSystemList()) {
                    if (!FileSystems.SUPPORTED_FS_TYPE.apply(fs)) {
                        continue;
                    }
                    try {
                        FileSystemUsage usage = sigarService.sigar().getFileSystemUsage(fs.getDirName());
                        size = setOrIncrementBy(size, usage.getTotal() * 1024);
                        used = setOrIncrementBy(used, usage.getUsed());
                        available = setOrIncrementBy(available, usage.getAvail() * 1024);
                        reads = setOrIncrementBy(reads, usage.getDiskReads());
                        bytes_read = setOrIncrementBy(bytes_read, usage.getDiskReadBytes());
                        writes = setOrIncrementBy(writes, usage.getDiskWrites());
                        bytes_written = setOrIncrementBy(bytes_written, usage.getDiskWriteBytes());
                    } catch (SigarPermissionDeniedException e) {
                        logger.warn(String.format(
                            "Permission denied: couldn't get file system usage for \"%s\"", fs.getDirName()));
                    }
                }
            } catch (SigarException e) {
                logger.warn("error getting filesystem totals", e);
            }
        } else {
            logger.trace("sigar not available");
        }
        totals.put(SIZE, size);
        totals.put(USED, used);
        totals.put(AVAILABLE, available);
        totals.put(READS, reads);
        totals.put(BYTES_READ, bytes_read);
        totals.put(WRITES, writes);
        totals.put(BYTES_WRITTEN, bytes_written);
        return totals;
    }

    private static long setOrIncrementBy(long l, long val) {
        if (val >= 0) {
            if (l < 0) {
                l = val;
            } else {
                l += val;
            }
        }
        return l;
    }

    protected class NodeFSTotalChildExpression extends ChildExpression<Long> {

        private final String name;

        protected NodeFSTotalChildExpression(String name) {
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
