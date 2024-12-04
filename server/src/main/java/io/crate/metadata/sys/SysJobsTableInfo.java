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

import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMPZ;

import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;

import io.crate.expression.reference.sys.job.JobContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public class SysJobsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "jobs");

    public static SystemTable<JobContext> create(Supplier<DiscoveryNode> localNode) {
        return SystemTable.<JobContext>builder(IDENT)
            .add("id", STRING, c -> c.id().toString())
            .add("username", STRING, JobContext::username)
            .startObject("node")
                .add("id", STRING, ignored -> localNode.get().getId())
                .add("name", STRING, ignored -> localNode.get().getName())
            .endObject()
            .add("stmt", STRING, JobContext::stmt)
            .add("started", TIMESTAMPZ, JobContext::started)
            .withRouting((state, routingProvider, sessionSettings) -> Routing.forTableOnAllNodes(IDENT, state.nodes()))
            .setPrimaryKeys(ColumnIdent.of("id"))
            .build();
    }
}
