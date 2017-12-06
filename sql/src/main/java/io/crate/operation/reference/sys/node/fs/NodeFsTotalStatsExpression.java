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

package io.crate.operation.reference.sys.node.fs;

import com.google.common.collect.ImmutableMap;
import io.crate.monitor.FsInfoHelpers;
import io.crate.operation.reference.sys.node.SimpleNodeStatsExpression;
import org.elasticsearch.monitor.fs.FsInfo;

import java.util.Map;

import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.AVAILABLE;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.BYTES_READ;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.BYTES_WRITTEN;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.READS;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.SIZE;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.USED;
import static io.crate.operation.reference.sys.node.fs.NodeFsStatsExpression.WRITES;

public class NodeFsTotalStatsExpression extends SimpleNodeStatsExpression<Map<String, Long>> {

    public NodeFsTotalStatsExpression() {
    }

    @Override
    public Map<String, Long> innerValue() {
        FsInfo info = this.row.fsInfo();
        FsInfo.Path path = info.getTotal();
        FsInfo.IoStats ioStats = info.getIoStats();
        return ImmutableMap.<String, Long>builder()
            .put(SIZE, FsInfoHelpers.Path.size(path))
            .put(USED, FsInfoHelpers.Path.used(path))
            .put(AVAILABLE, FsInfoHelpers.Path.available(path))
            .put(READS, FsInfoHelpers.Stats.readOperations(ioStats))
            .put(BYTES_READ, FsInfoHelpers.Stats.bytesRead(ioStats))
            .put(WRITES, FsInfoHelpers.Stats.writeOperations(ioStats))
            .put(BYTES_WRITTEN, FsInfoHelpers.Stats.bytesWritten(ioStats))
            .build();
    }
}
