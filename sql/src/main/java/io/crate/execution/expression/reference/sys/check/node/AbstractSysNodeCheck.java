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

package io.crate.execution.expression.reference.sys.check.node;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.Id;
import io.crate.execution.expression.reference.sys.check.AbstractSysCheck;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.List;
import java.util.function.Function;

public abstract class AbstractSysNodeCheck extends AbstractSysCheck implements SysNodeCheck {

    private static final Function<List<BytesRef>, String> PK_FUNC = Id.compile(2, -1);


    private static final String LINK_PATTERN = "https://cr8.is/d-node-check-";
    protected final ClusterService clusterService;

    private BytesRef nodeId;
    private BytesRef rowId;
    private boolean acknowledged;

    AbstractSysNodeCheck(int id, String description, Severity severity, ClusterService clusterService) {
        super(id, description, severity, LINK_PATTERN);
        this.clusterService = clusterService;
        acknowledged = false;
    }

    @Override
    public BytesRef nodeId() {
        assert nodeId != null : "local node required, setNodeId not called";
        return nodeId;
    }

    @Override
    public BytesRef rowId() {
        assert rowId != null : "local node required, setNodeId not called";
        return rowId;
    }

    @Override
    public boolean acknowledged() {
        return acknowledged;
    }

    @Override
    public void acknowledged(boolean value) {
        acknowledged = value;
    }

    private BytesRef generateId(BytesRef nodeId) {
        //noinspection ConstantConditions
        return new BytesRef(PK_FUNC.apply(ImmutableList.of(DataTypes.STRING.value(id()), nodeId)));
    }

    public void setNodeId(BytesRef nodeId) {
        this.nodeId = nodeId;
        this.rowId = generateId(nodeId);
    }

}
