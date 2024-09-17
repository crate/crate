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
import static io.crate.types.DataTypes.TIMESTAMPZ;

import java.net.InetAddress;
import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.jetbrains.annotations.Nullable;

import io.crate.action.sql.Session;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public final class SysSessionsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "sessions");

    private static final String ID = "id";

    private static final String AUTHENTICATED_USER = "auth_user";
    private static final String SESSION_USER = "session_user";
    private static final String HANDLER_NODE = "handler_node";
    private static final String CLIENT_ADDRESS = "client_address";
    private static final String TIME_CREATED = "time_created";
    private static final String PROTOCOL = "protocol";
    private static final String SSL = "ssl";
    private static final String SETTINGS = "settings";
    private static final String LAST_STMT = "last_statement";

    private SysSessionsTableInfo() {}

    public static SystemTable<Session> create(Supplier<DiscoveryNode> localNode) {
        return SystemTable.<Session>builder(IDENT)
            .add(ID, INTEGER, Session::id)
            .add(SESSION_USER, STRING, s -> s.sessionSettings().sessionUser().name())
            .add(AUTHENTICATED_USER, STRING, s -> s.sessionSettings().authenticatedUser().name())
            .add(HANDLER_NODE, STRING, ignored -> localNode.get().getName())
            .add(CLIENT_ADDRESS, STRING, s -> resolveClientNetAddress(s.clientAddress()))
            .add(PROTOCOL, STRING, s -> s.protocol().toString())
            .add(SSL, BOOLEAN, Session::hasSSL)
            .add(TIME_CREATED, TIMESTAMPZ, Session::timeCreated)
            .addDynamicObject(SETTINGS, STRING, s -> s == null ? null : s.sessionSettings().toMap())
            .add(LAST_STMT, STRING, Session::lastStmt)
            .setPrimaryKeys(ColumnIdent.of(HANDLER_NODE), ColumnIdent.of(ID))
            .withRouting((state, routingProvider, sessionSettings) -> Routing.forTableOnAllNodes(IDENT, state.nodes()))
            .build();
    }

    private static String resolveClientNetAddress(@Nullable InetAddress clientAddress) {
        if (clientAddress == null) {
            return "localhost";
        }
        return clientAddress.getHostAddress();
    }
}
