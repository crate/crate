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

package io.crate.operation.reference.sys.node;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.service.NodeService;

public class NodeProcessExpression extends SysNodeObjectReference {

    public static final String NAME = "process";

    abstract class ProcessExpression extends SysNodeExpression<Long> {
        ProcessExpression(String name) { super(new ColumnIdent(NAME, ImmutableList.of(name))); }
    }

    public static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
    public static final String MAX_OPEN_FILE_DESCRIPTORS = "max_open_file_descriptors";

    private final NodeService nodeService;

    @Inject
    protected NodeProcessExpression(final NodeService nodeService) {
        super(NAME);
        this.nodeService = nodeService;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(OPEN_FILE_DESCRIPTORS, new ProcessExpression(OPEN_FILE_DESCRIPTORS) {
            @Override
            public Long value() {
                ProcessStats processStats = nodeService.stats().getProcess();
                if (processStats != null) {
                    return processStats.getOpenFileDescriptors();
                } else { return -1L; }
            }
        });
        childImplementations.put(MAX_OPEN_FILE_DESCRIPTORS, new ProcessExpression(MAX_OPEN_FILE_DESCRIPTORS) {
            @Override
            public Long value() {
                ProcessInfo processInfo = nodeService.info().getProcess();
                if (processInfo != null) {
                    return processInfo.getMaxFileDescriptors();
                } else { return -1L; }
            }
        });

    }

}
