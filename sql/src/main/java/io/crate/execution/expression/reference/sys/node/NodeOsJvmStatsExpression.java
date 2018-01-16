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

class NodeOsJvmStatsExpression extends NestedNodeStatsExpression {

    private static final String VERSION = "version";
    private static final String VM_NAME = "vm_name";
    private static final String VM_VENDOR = "vm_vendor";
    private static final String VM_VERSION = "vm_version";

    NodeOsJvmStatsExpression() {
        childImplementations.put(VERSION, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.javaVersion();
            }
        });
        childImplementations.put(VM_NAME, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.jvmName();
            }
        });
        childImplementations.put(VM_VENDOR, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.jvmVendor();
            }
        });
        childImplementations.put(VM_VERSION, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.jvmVersion();
            }
        });
    }
}
