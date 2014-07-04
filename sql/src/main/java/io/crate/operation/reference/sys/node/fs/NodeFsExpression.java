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

import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.sigar.SigarService;

public class NodeFsExpression extends SysNodeObjectReference {

    public static final String NAME = "fs";

    @Inject
    public NodeFsExpression(SigarService sigarService,
                            NodeEnvironment nodeEnvironment) {
        super(NAME);
        childImplementations.put(NodeFsTotalExpression.NAME, new NodeFsTotalExpression(sigarService));
        childImplementations.put(NodeFsDisksExpression.NAME, new NodeFsDisksExpression(sigarService));
        childImplementations.put(NodeFsDataExpression.NAME, new NodeFsDataExpression(sigarService, nodeEnvironment));
    }

}
