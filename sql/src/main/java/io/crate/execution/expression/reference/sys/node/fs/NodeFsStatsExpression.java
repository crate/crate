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

package io.crate.execution.expression.reference.sys.node.fs;

import com.google.common.collect.ImmutableMap;
import io.crate.execution.expression.reference.sys.node.SimpleNodeStatsExpression;

import java.util.Map;

public class NodeFsStatsExpression extends SimpleNodeStatsExpression<Map<String, Object>> {

    private static final String TOTAL = "total";
    private static final String DISKS = "disks";
    private static final String DATA = "data";

    static final String DEV = "dev";
    static final String PATH = "path";

    static final String SIZE = "size";
    static final String USED = "used";
    static final String AVAILABLE = "available";

    @Deprecated // for individual devices
    static final String READS = "reads";
    @Deprecated // for individual devices
    static final String BYTES_READ = "bytes_read";
    @Deprecated // for individual devices
    static final String WRITES = "writes";
    @Deprecated // for individual devices
    static final String BYTES_WRITTEN = "bytes_written";

    private final NodeFsTotalStatsExpression total;
    private final NodeStatsFsDisksExpression disks;
    private final NodeStatsFsDataExpression data;

    public NodeFsStatsExpression() {
        total = new NodeFsTotalStatsExpression();
        disks = new NodeStatsFsDisksExpression();
        data = new NodeStatsFsDataExpression();
    }

    @Override
    public Map<String, Object> innerValue() {
        total.setNextRow(this.row);
        disks.setNextRow(this.row);
        data.setNextRow(this.row);
        return ImmutableMap.<String, Object>builder()
            .put(TOTAL, total.value())
            .put(DISKS, disks.value())
            .put(DATA, data.value())
            .build();
    }
}
