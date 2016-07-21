/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import org.apache.lucene.util.BytesRef;

public class NodeOsInfoExpression extends NestedDiscoveryNodeExpression {

    private static final String AVAILABLE_PROCESSORS = "available_processors";
    private static final String OS = "name";
    private static final String ARCH = "arch";
    private static final String VERSION = "version";
    private static final String JVM = "jvm";

    private abstract class OsInfoExpression extends SimpleDiscoveryNodeExpression<Object> {
    }

    public NodeOsInfoExpression() {
        childImplementations.put(AVAILABLE_PROCESSORS, new OsInfoExpression() {
            @Override
            public Integer value() {
                return this.row.osInfo.getAvailableProcessors();
            }
        });
        childImplementations.put(OS, new OsInfoExpression() {
            @Override
            public BytesRef value() {
                return this.row.OS_NAME;
            }
        });
        childImplementations.put(ARCH, new OsInfoExpression() {
            @Override
            public BytesRef value() {
                return this.row.OS_ARCH;
            }
        });
        childImplementations.put(VERSION, new OsInfoExpression() {
            @Override
            public BytesRef value() {
                return this.row.OS_VERSION;
            }
        });
        childImplementations.put(JVM, new NodeOsJvmExpression());
    }

}

