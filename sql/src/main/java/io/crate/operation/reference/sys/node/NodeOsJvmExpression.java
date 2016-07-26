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

public class NodeOsJvmExpression extends NestedDiscoveryNodeExpression {

    private static final String VERSION = "version";
    private static final String VM_NAME = "vm_name";
    private static final String VM_VENDOR = "vm_vendor";
    private static final String VM_VERSION = "vm_version";

    private abstract class JvmExpression extends SimpleDiscoveryNodeExpression<BytesRef> {
    }

    public NodeOsJvmExpression() {
        childImplementations.put(VERSION, new JvmExpression() {
            @Override
            public BytesRef innerValue() {
                return this.row.JAVA_VERSION;
            }
        });
        childImplementations.put(VM_NAME, new JvmExpression() {
            @Override
            public BytesRef innerValue() {
                return this.row.JVM_NAME;
            }
        });
        childImplementations.put(VM_VENDOR, new JvmExpression() {
            @Override
            public BytesRef innerValue() {
                return this.row.JVM_VENDOR;
            }
        });
        childImplementations.put(VM_VERSION, new JvmExpression() {
            @Override
            public BytesRef innerValue() {
                return this.row.JVM_VERSION;
            }
        });
    }
}
