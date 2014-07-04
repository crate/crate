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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.SigarException;

import java.util.ArrayList;
import java.util.List;

class NodeFsDisksExpression extends SysNodeObjectArrayReference {

    public static final String NAME = "disks";
    private final ESLogger logger = Loggers.getLogger(getClass());
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
                    FileSystemUsage usage = sigarService.sigar().getFileSystemUsage(fs.getDirName());
                    diskRefs.add(new NodeFsDiskChildExpression(fs, usage));
                }
            } catch (SigarException e) {
                logger.warn("error getting disk stats", e);
                diskRefs = ImmutableList.of();
            }

        } else {
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

        private final String dev;
        private final FileSystemUsage usage;

        protected NodeFsDiskChildExpression(FileSystem fileSystem, FileSystemUsage usage) {
            super(NodeFsDisksExpression.this.info().ident().columnIdent());
            this.dev = fileSystem.getDevName();
            this.usage = usage;
            addChildImplementations();
        }

        private void addChildImplementations() {
            childImplementations.put(DEV, new ChildExpression<String>(DEV) {
                @Override
                public String value() {
                    return dev;
                }
            });
            childImplementations.put(SIZE, new ChildExpression<Long>(SIZE) {
                @Override
                public Long value() {
                    return usage.getTotal()*1024;
                }
            });
            childImplementations.put(USED, new ChildExpression<Long>(USED) {
                @Override
                public Long value() {
                    return usage.getUsed();
                }
            });
            childImplementations.put(AVAILABLE, new ChildExpression<Long>(AVAILABLE) {
                @Override
                public Long value() {
                    return usage.getAvail()*1024;
                }
            });
            childImplementations.put(READS, new ChildExpression<Long>(READS) {
                @Override
                public Long value() {
                    return usage.getDiskReads();
                }
            });
            childImplementations.put(BYTES_READ, new ChildExpression<Long>(BYTES_READ) {
                @Override
                public Long value() {
                    return usage.getDiskReadBytes();
                }
            });
            childImplementations.put(WRITES, new ChildExpression<Long>(WRITES) {
                @Override
                public Long value() {
                    return usage.getDiskWrites();
                }
            });
            childImplementations.put(BYTES_WRITTEN, new ChildExpression<Long>(BYTES_WRITTEN) {
                @Override
                public Long value() {
                    return usage.getDiskWriteBytes();
                }
            });

        }

    }
}
