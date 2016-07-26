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

import io.crate.monitor.ExtendedFsStats;
import io.crate.operation.reference.RowCollectNestedObjectExpression;
import io.crate.operation.reference.sys.node.*;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

import static io.crate.operation.reference.sys.node.fs.NodeFsExpression.*;

public class NodeFsDisksExpression extends DiscoveryNodeObjectArrayRowCtxExpression {

    NodeFsDisksExpression() {
    }


    @Override
    protected List<RowCollectNestedObjectExpression<DiscoveryNodeContext>> getChildImplementations() {
        List<RowCollectNestedObjectExpression<DiscoveryNodeContext>> diskRefs = new ArrayList<>(this.row.extendedFsStats.size());
        for (ExtendedFsStats.Info info : this.row.extendedFsStats) {
            NodeFsDiskChildExpression childExpression = new NodeFsDiskChildExpression(info);
            childExpression.setNextRow(this.row);
            diskRefs.add(childExpression);
        }
        return diskRefs;
    }

    private static class NodeFsDiskChildExpression extends NestedDiscoveryNodeExpression {

        private static final String DEV = "dev";

        protected NodeFsDiskChildExpression(final ExtendedFsStats.Info fsInfo) {
            childImplementations.put(DEV, new SimpleDiscoveryNodeExpression<BytesRef>() {
                @Override
                public BytesRef innerValue() {
                    return fsInfo.dev();
                }
            });
            childImplementations.put(SIZE, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.total();
                }
            });
            childImplementations.put(USED, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.used();
                }
            });
            childImplementations.put(AVAILABLE, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.available();
                }
            });
            childImplementations.put(READS, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.diskReads();
                }
            });
            childImplementations.put(BYTES_READ, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.diskReadSizeInBytes();
                }
            });
            childImplementations.put(WRITES, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.diskWrites();
                }
            });
            childImplementations.put(BYTES_WRITTEN, new SimpleDiscoveryNodeExpression<Long>() {
                @Override
                public Long innerValue() {
                    return fsInfo.diskWriteSizeInBytes();
                }
            });

        }

    }
}
