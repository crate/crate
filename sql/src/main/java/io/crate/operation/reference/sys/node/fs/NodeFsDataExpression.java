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
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import io.crate.operation.reference.sys.SysNodeStaticObjectArrayReference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeFsDataExpression extends SysNodeStaticObjectArrayReference {

    public static final String NAME = "data";

    private static final ESLogger LOGGER = Loggers.getLogger(NodeFsDataExpression.class);

    private final SigarService sigarService;
    private final NodeEnvironment nodeEnvironment;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    protected NodeFsDataExpression(SigarService sigarService, NodeEnvironment nodeEnvironment) {
        super(new ColumnIdent(NodeFsExpression.NAME, ImmutableList.of(NAME)));
        this.sigarService = sigarService;
        this.nodeEnvironment = nodeEnvironment;
    }

    @Override
    protected List<NestedObjectExpression> getChildImplementations() {
        if (!initialized.getAndSet(true)) {
            addChildImplementations();
        }
        return childImplementations;
    }

    private void addChildImplementations() {
        if (!sigarService.sigarAvailable()) {
            LOGGER.trace("sigar is not available");
            return;
        }
        if (!nodeEnvironment.hasNodeFile()) {
            LOGGER.trace("no node files available");
            return;
        }
        try {
            FileSystem[] fsList = sigarService.sigar().getFileSystemList();
            for (Path dataLocation : nodeEnvironment.nodeDataPaths()) {
                FileSystem winner = null;
                String absDataLocation = dataLocation.toFile().getCanonicalPath();
                for (FileSystem fs : fsList) {
                    // ignore rootfs as ist might shadow another mount on /
                    if (!FileSystems.SUPPORTED_FS_TYPE.apply(fs) || "rootfs".equals(fs.getDevName())) {
                        continue;
                    }
                    if (absDataLocation.startsWith(fs.getDirName())
                            && (winner == null || winner.getDirName().length() < fs.getDirName().length())) {
                        winner = fs;
                    }
                }
                childImplementations.add(new NodeFsDataChildExpression(
                        winner != null ? new BytesRef(winner.getDevName()) : null,
                        new BytesRef(absDataLocation)
                ));
            }
        } catch (Exception e) {
            LOGGER.warn("error getting fs['data'] expression", e);
        }
    }


    private class NodeFsDataChildExpression extends SysNodeObjectReference {

        public static final String DEV = "dev";
        public static final String PATH = "path";

        protected NodeFsDataChildExpression(final BytesRef device, final BytesRef dataPath) {
            childImplementations.put(DEV, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return device;
                }
            });
            childImplementations.put(PATH, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return dataPath;
                }
            });
        }
    }
}
