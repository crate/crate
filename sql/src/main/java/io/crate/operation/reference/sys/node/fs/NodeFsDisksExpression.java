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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.sys.SysNodeObjectArrayReference;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import io.crate.operation.reference.sys.SysObjectReference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarPermissionDeniedException;

import java.util.ArrayList;
import java.util.List;

public class NodeFsDisksExpression extends SysNodeObjectArrayReference {

    public static final String NAME = "disks";
    private static final ESLogger logger = Loggers.getLogger(NodeFsDisksExpression.class);
    private final SigarService sigarService;

    NodeFsDisksExpression(SigarService sigarService) {
        super(new ColumnIdent(NodeFsExpression.NAME, ImmutableList.of(NAME)));
        this.sigarService = sigarService;
    }

    @Override
    protected List<SysObjectReference> getChildImplementations() {
        List<SysObjectReference> diskRefs;
        if (sigarService.sigarAvailable()) {
            try {
                FileSystem[] fileSystems = sigarService.sigar().getFileSystemList();
                diskRefs = new ArrayList<>(fileSystems.length);
                for (FileSystem fs : fileSystems) {
                    // no disk usage possible for rootfs
                    if (!FileSystems.SUPPORTED_FS_TYPE.apply(fs) || "rootfs".equals(fs.getDevName())) {
                        continue;
                    }
                    try {
                        FileSystemUsage usage = sigarService.sigar().getFileSystemUsage(fs.getDirName());
                        diskRefs.add(new NodeFsDiskChildExpression(fs, usage));
                    } catch (SigarPermissionDeniedException e) {
                        logger.warn(String.format(
                            "Permission denied: couldn't get file system usage for \"%s\"", fs.getDirName()));
                    }
                }
            } catch (SigarException e) {
                logger.warn("error getting disk stats", e);
                diskRefs = ImmutableList.of();
            }

        } else {
            logger.trace("sigar not available");
            diskRefs = ImmutableList.of();
        }
        return diskRefs;
    }

    private class NodeFsDiskChildExpression extends SysNodeObjectReference {

        public static final String DEV = "dev";
        public static final String SIZE = "size";
        public static final String USED = "used";
        public static final String AVAILABLE = "available";
        public static final String READS = "reads";
        public static final String BYTES_READ = "bytes_read";
        public static final String WRITES = "writes";
        public static final String BYTES_WRITTEN = "bytes_written";

        private final BytesRef dev;
        private final FileSystemUsage usage;

        protected NodeFsDiskChildExpression(FileSystem fileSystem, FileSystemUsage usage) {
            this.dev = new BytesRef(fileSystem.getDevName());
            this.usage = usage;
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(DEV, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return dev;
                }
            });
            childImplementations.put(SIZE, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getTotal()*1024;
                }
            });
            childImplementations.put(USED, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getUsed()*1024;
                }
            });
            childImplementations.put(AVAILABLE, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getAvail()*1024;
                }
            });
            childImplementations.put(READS, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getDiskReads();
                }
            });
            childImplementations.put(BYTES_READ, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getDiskReadBytes();
                }
            });
            childImplementations.put(WRITES, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getDiskWrites();
                }
            });
            childImplementations.put(BYTES_WRITTEN, new ChildExpression<Long>() {
                @Override
                public Long value() {
                    return usage.getDiskWriteBytes();
                }
            });

        }

    }
}
