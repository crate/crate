/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.node.service.NodeService;

public class NodeFsExpression extends SysNodeObjectReference<Object> {

    public static final String NAME = "fs";

    abstract class FsExpression extends SysNodeExpression<Object> {
        FsExpression(String name) {
            super(new ColumnIdent(NAME, ImmutableList.of(name)));
        }
    }

    public static final String TOTAL = "total";
    public static final String FREE = "free";
    public static final String USED = "used";
    public static final String FREE_PERCENT = "free_percent";
    public static final String USED_PERCENT = "used_percent";

    private final NodeService nodeService;

    @Inject
    public NodeFsExpression(NodeService nodeService) {
        super(NAME);
        this.nodeService = nodeService;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(TOTAL, new FsExpression(TOTAL) {
            @Override
            public Long value() {
                return getTotalBytesFromAllDisks();
            }
        });
        childImplementations.put(FREE, new FsExpression(FREE) {
            @Override
            public Long value() {
                return getFreeBytesFromAllDisks();
            }
        });
        childImplementations.put(USED, new FsExpression(USED) {
            @Override
            public Long value() {
                return getTotalBytesFromAllDisks() - getFreeBytesFromAllDisks();
            }
        });
        childImplementations.put(FREE_PERCENT, new FsExpression(FREE_PERCENT) {
            @Override
            public Double value() {
                return new Double((getFreeBytesFromAllDisks() / (double) getTotalBytesFromAllDisks()) * 100);
            }
        });
        childImplementations.put(USED_PERCENT, new FsExpression(USED_PERCENT) {
            @Override
            public Double value() {
                Long total_bytes = getTotalBytesFromAllDisks();
                Long used_bytes = total_bytes - getFreeBytesFromAllDisks();
                return new Double((used_bytes / (double) total_bytes) * 100);
            }
        });
    }

    private long getFreeBytesFromAllDisks() {
        Long bytes = 0L;
        for (FsStats.Info fsInfo : nodeService.stats().getFs()) {
            bytes += fsInfo.getFree().bytes();
        }
        return bytes;
    }

    private long getTotalBytesFromAllDisks() {
        Long bytes = 0L;
        for (FsStats.Info fsInfo : nodeService.stats().getFs()) {
            bytes += fsInfo.getTotal().bytes();
        }
        return bytes;
    }

}
