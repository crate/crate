/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMPZ;

import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;

import io.crate.expression.reference.sys.operation.OperationContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public class SysOperationsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "operations");

    public static SystemTable<OperationContext> create(Supplier<DiscoveryNode> localNode) {
        return SystemTable.<OperationContext>builder(IDENT)
            .add("id", STRING, c -> String.valueOf(c.id()))
            .add("job_id", STRING, c -> c.jobId().toString())
            .add("name", STRING, OperationContext::name)
            .add("started", TIMESTAMPZ, OperationContext::started)
            .add("used_bytes", LONG, OperationContext::usedBytes)
            .startObject("node")
                .add("id", STRING, ignored -> localNode.get().getId())
                .add("name", STRING, ignored -> localNode.get().getName())
            .endObject()
            .withRouting((state, routingProvider, sessionContext) -> Routing.forTableOnAllNodes(IDENT, state.getNodes()))
            .build();
    }
}
