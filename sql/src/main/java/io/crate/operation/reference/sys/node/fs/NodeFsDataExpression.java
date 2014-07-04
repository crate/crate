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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.SigarException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

class NodeFsDataExpression extends SysNodeObjectArrayReference {

    public static final String NAME = "data";

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final SigarService sigarService;
    private final NodeEnvironment nodeEnvironment;

    protected NodeFsDataExpression(SigarService sigarService, NodeEnvironment nodeEnvironment) {
        super(new ColumnIdent(NodeFsExpression.NAME, ImmutableList.of(NAME)));
        this.sigarService = sigarService;
        this.nodeEnvironment = nodeEnvironment;
    }

    @Override
    protected List<SysObjectReference> getChildImplementations() {
        List<SysObjectReference> dataRefs;
        if (sigarService.sigarAvailable() && nodeEnvironment.hasNodeFile()) {
            dataRefs = new ArrayList<>(
                    nodeEnvironment.nodeDataLocations().length
            );
            try {
                FileSystemMap fileSystemMap = sigarService.sigar().getFileSystemMap();
                for (File dataLocation : nodeEnvironment.nodeDataLocations()) {
                    FileSystem fs = fileSystemMap.getMountPoint(dataLocation.getAbsolutePath());
                    if (fs != null) {
                        dataRefs.add(new NodeFsDataChildExpression(
                                fs.getDevName(),
                                dataLocation.getAbsolutePath()
                        ));
                    }
                }
            } catch (SigarException e) {
                logger.warn("error getting filesystem map", e);
                return ImmutableList.of();
            }
        } else {
            dataRefs = ImmutableList.of();
        }
        return dataRefs;
    }

    private class NodeFsDataChildExpression extends SysNodeObjectReference {

        public static final String DEV = "dev";
        public static final String PATH = "path";

        protected NodeFsDataChildExpression(final String device, final String dataPath) {
            super(NodeFsDataExpression.this.info().ident().columnIdent());
            childImplementations.put(DEV, new ChildExpression<String>(DEV) {
                @Override
                public String value() {
                    return device;
                }
            });
            childImplementations.put(PATH, new ChildExpression<String>(PATH) {
                @Override
                public String value() {
                    return dataPath;
                }
            });
        }
    }
}
