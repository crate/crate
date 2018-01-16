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

package io.crate.execution.expression.reference.sys.node;

import org.apache.lucene.util.BytesRef;

public class NodeOsInfoStatsExpression extends NestedNodeStatsExpression {

    private static final String AVAILABLE_PROCESSORS = "available_processors";
    private static final String OS = "name";
    private static final String ARCH = "arch";
    private static final String VERSION = "version";
    private static final String JVM = "jvm";

    public NodeOsInfoStatsExpression() {
        childImplementations.put(AVAILABLE_PROCESSORS, new SimpleNodeStatsExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return this.row.osInfo().getAvailableProcessors();
            }
        });
        childImplementations.put(OS, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osName();
            }
        });
        childImplementations.put(ARCH, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osArch();
            }
        });
        childImplementations.put(VERSION, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osVersion();
            }
        });
        childImplementations.put(JVM, new NodeOsJvmStatsExpression());
    }
}

