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

package io.crate.expression.reference.sys.node.local;

import io.crate.expression.ReferenceImplementation;
import io.crate.expression.reference.NestedObjectExpression;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.lucene.BytesRefs;

class NodeOsJvmExpression extends NestedObjectExpression {

    private static final String VERSION = "version";
    private static final String VM_NAME = "vm_name";
    private static final String VM_VENDOR = "vm_vendor";
    private static final String VM_VERSION = "vm_version";

    private static final BytesRef JAVA_VERSION = BytesRefs.toBytesRef(Constants.JAVA_VERSION);
    private static final BytesRef JVM_NAME = BytesRefs.toBytesRef(Constants.JVM_NAME);
    private static final BytesRef JVM_VENDOR = BytesRefs.toBytesRef(Constants.JVM_VENDOR);
    private static final BytesRef JVM_VERSION = BytesRefs.toBytesRef(Constants.JVM_VERSION);

    private static final ReferenceImplementation<BytesRef> JAVA_VERSION_EXR = () -> JAVA_VERSION;
    private static final ReferenceImplementation<BytesRef> JVM_NAME_EXPR = () -> JVM_NAME;
    private static final ReferenceImplementation<BytesRef> JVM_VENDOR_EXPR = () -> JVM_VENDOR;
    private static final ReferenceImplementation<BytesRef> JVM_VERSION_EXPR = () -> JVM_VERSION;

    NodeOsJvmExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(VERSION, JAVA_VERSION_EXR);
        childImplementations.put(VM_NAME, JVM_NAME_EXPR);
        childImplementations.put(VM_VENDOR, JVM_VENDOR_EXPR);
        childImplementations.put(VM_VERSION, JVM_VERSION_EXPR);
    }
}
