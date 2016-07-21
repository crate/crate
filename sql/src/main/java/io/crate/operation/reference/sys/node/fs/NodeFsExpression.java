/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.node.fs;

import io.crate.operation.reference.sys.node.NestedDiscoveryNodeExpression;

public class NodeFsExpression extends NestedDiscoveryNodeExpression {

    private static final String TOTAL = "total";
    private static final String DISKS = "disks";
    private static final String DATA = "data";

    /**
     * Names of subcolumns that are common across NodeFSTotalExpression and NodeFsDiskExpression.
     */
    static final String SIZE = "size";
    static final String USED = "used";
    static final String AVAILABLE = "available";
    static final String READS = "reads";
    static final String BYTES_READ = "bytes_read";
    static final String WRITES = "writes";
    static final String BYTES_WRITTEN = "bytes_written";

    public NodeFsExpression() {
        childImplementations.put(TOTAL, new NodeFsTotalExpression());
        childImplementations.put(DISKS, new NodeFsDisksExpression());
        childImplementations.put(DATA, new NodeFsDataExpression());
    }

}
