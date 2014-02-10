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

package io.crate.operator.reference.sys.node;

import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SysNodesTableInfo;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.node.service.NodeService;

public class NodeNameExpression extends SysExpression<String> {

    public static final String COLNAME = "name";


    public static final ReferenceInfo INFO_NAME = SysNodesTableInfo.register(COLNAME, DataType.STRING, null);


    private final NodeService nodeService;

    @Inject
    public NodeNameExpression(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @Override
    public String value() {
        return nodeService.stats().getNode().getName();
    }

    @Override
    public ReferenceInfo info() {
        return INFO_NAME;
    }

}
