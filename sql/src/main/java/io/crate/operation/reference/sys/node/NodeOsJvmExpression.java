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

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;

public class NodeOsJvmExpression extends SysNodeObjectReference {

    private static final String VERSION = "version";
    private static final String VM_NAME = "vm_name";
    private static final String VM_VENDOR = "vm_vendor";
    private static final String VM_VERSION = "vm_version";

    private static final BytesRef JAVA_VERSION = BytesRefs.toBytesRef(Constants.JAVA_VERSION);
    private static final BytesRef JVM_NAME = BytesRefs.toBytesRef(Constants.JVM_NAME);
    private static final BytesRef JVM_VENDOR = BytesRefs.toBytesRef(Constants.JVM_VENDOR);
    private static final BytesRef JVM_VERSION = BytesRefs.toBytesRef(Constants.JVM_VERSION);

    abstract class JvmExpression extends SysNodeExpression<Object> {
    }

    public NodeOsJvmExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(VERSION, new JvmExpression() {
            @Override
            public BytesRef value() {
                return JAVA_VERSION;
            }
        });
        childImplementations.put(VM_NAME, new JvmExpression() {
            @Override
            public BytesRef value() {
                return JVM_NAME;
            }
        });
        childImplementations.put(VM_VENDOR, new JvmExpression() {
            @Override
            public BytesRef value() {
                return JVM_VENDOR;
            }
        });
        childImplementations.put(VM_VERSION, new JvmExpression() {
            @Override
            public BytesRef value() {
                return JVM_VERSION;
            }
        });
    }
}
