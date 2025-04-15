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

package io.crate.expression.reference.sys.check.node;

import java.util.List;
import java.util.function.BiFunction;

import io.crate.analyze.Id;
import io.crate.expression.reference.sys.check.AbstractSysCheck;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class AbstractSysNodeCheck extends AbstractSysCheck implements SysNodeCheck {

    private static final BiFunction<List<String>, List<DataType<?>>, String> PK_FUNC = Id.compile(2, -1);


    private static final String LINK_PATTERN = "https://cr8.is/d-node-check-";

    private String nodeId;
    private String rowId;
    private boolean acknowledged;

    AbstractSysNodeCheck(int id, String description, Severity severity) {
        super(id, description, severity, LINK_PATTERN);
        acknowledged = false;
    }

    @Override
    public String nodeId() {
        assert nodeId != null : "local node required, setNodeId not called";
        return nodeId;
    }

    @Override
    public String rowId() {
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

    private String generateId(String nodeId) {
        return PK_FUNC.apply(List.of(String.valueOf(id()), nodeId), List.of(DataTypes.STRING, DataTypes.STRING));
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        this.rowId = generateId(nodeId);
    }
}
