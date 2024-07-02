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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

import java.util.EnumSet;
import java.util.Set;

import io.crate.expression.reference.sys.check.node.SysNodeCheck;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.Operation;


public class SysNodeChecksTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "node_checks");

    private static final Set<Operation> SUPPORTED_OPERATIONS = EnumSet.of(Operation.READ, Operation.UPDATE);

    public static class Columns {
        public static final ColumnIdent ACKNOWLEDGED = ColumnIdent.of("acknowledged");
    }

    public static SystemTable<SysNodeCheck> INSTANCE = SystemTable.<SysNodeCheck>builder(IDENT)
        .add("id", INTEGER, SysNodeCheck::id)
        .add("node_id", STRING, SysNodeCheck::nodeId)
        .add("severity", INTEGER, (SysNodeCheck x) -> x.severity().value())
        .add("description", STRING, SysNodeCheck::description)
        .add("passed", BOOLEAN, SysNodeCheck::isValid)
        .add("acknowledged", BOOLEAN, SysNodeCheck::acknowledged)
        .add(DocSysColumns.ID.COLUMN.name(), STRING, SysNodeCheck::rowId)
        .withRouting((state, routingProvider, sessionSettings) -> Routing.forTableOnAllNodes(IDENT, state.nodes()))
        .withSupportedOperations(SUPPORTED_OPERATIONS)
        .setPrimaryKeys(
            ColumnIdent.of("id"),
            ColumnIdent.of("node_id")
        )
        .build();
}
