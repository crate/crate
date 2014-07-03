/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysExpression;
import io.crate.operation.reference.sys.SysNodeObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.node.service.NodeService;


public class NodeOsExpression extends SysNodeObjectReference {

    public static final String NAME = "os";

    abstract class OsExpression extends SysNodeExpression<Object> {
        OsExpression(String name) { super(new ColumnIdent(NAME, ImmutableList.of(name))); }
    }

    public static final String UPTIME = "uptime";
    public static final String TIMESTAMP = "timestamp";

    private final NodeService nodeService;

    @Inject
    public NodeOsExpression(NodeService nodeService) {
        super(new ColumnIdent(NAME));
        this.nodeService = nodeService;
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(UPTIME, new OsExpression(UPTIME) {
            @Override
            public Long value() {
                return nodeService.stats().getOs().uptime().millis();
            }
        });
        childImplementations.put(TIMESTAMP, new OsExpression(TIMESTAMP) {
            @Override
            public Long value() {
                return System.currentTimeMillis();
            }
        });
        childImplementations.put(NodeOsCpuExpression.NAME,
                (SysExpression) new NodeOsCpuExpression(nodeService));
    }

}
