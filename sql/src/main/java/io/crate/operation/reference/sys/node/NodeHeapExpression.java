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

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.monitor.jvm.JvmStats;

public class NodeHeapExpression extends SysNodeObjectReference {

    abstract class HeapExpression extends SysNodeExpression<Object> {
    }

    private static final String MAX = "max";
    private static final String FREE = "free";
    private static final String USED = "used";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";

    NodeHeapExpression(JvmStats stats) {
        addChildImplementations(stats);
    }

    private void addChildImplementations(final JvmStats stats) {
        childImplementations.put(FREE, new HeapExpression() {
            @Override
            public Long value() {
                return stats.getMem().getHeapMax().bytes() - stats.getMem().getHeapUsed().bytes();
            }
        });
        childImplementations.put(USED, new HeapExpression() {
            @Override
            public Long value() {
                return stats.getMem().getHeapUsed().bytes();
            }
        });
        childImplementations.put(MAX, new HeapExpression() {
            @Override
            public Long value() {
                return stats.getMem().getHeapMax().bytes();
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new SysNodeExpression<Long>() {
            @Override
            public Long value() {
                return stats.getTimestamp();
            }
        });
    }

}
