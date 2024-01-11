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

package io.crate.role.metadata;

import static io.crate.types.DataTypes.STRING;

import java.util.stream.StreamSupport;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.role.Privilege;
import io.crate.role.Role;

public class SysPrivilegesTableInfo {

    private static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "privileges");

    @SuppressWarnings("WeakerAccess")
    public static class PrivilegeRow {
        private final String grantee;
        private final Privilege privilege;

        PrivilegeRow(String grantee, Privilege privilege) {
            this.grantee = grantee;
            this.privilege = privilege;
        }
    }

    public static SystemTable<PrivilegeRow> create() {
        return SystemTable.<PrivilegeRow>builder(IDENT)
            .add("grantee", STRING, x -> x.grantee)
            .add("grantor", STRING, x -> x.privilege.grantor())
            .add("state", STRING, x -> x.privilege.state().toString())
            .add("type", STRING, x -> x.privilege.ident().type().toString())
            .add("class", STRING, x -> x.privilege.ident().securable().toString())
            .add("ident", STRING, x -> x.privilege.ident().ident())
            .setPrimaryKeys(
                new ColumnIdent("grantee"),
                new ColumnIdent("state"),
                new ColumnIdent("type"),
                new ColumnIdent("class"),
                new ColumnIdent("ident")
            )
            .build();
    }

    public static Iterable<PrivilegeRow> buildPrivilegesRows(Iterable<Role> roles) {
        return () -> StreamSupport.stream(roles.spliterator(), false)
            .flatMap(u -> StreamSupport.stream(u.privileges().spliterator(), false)
                .map(p -> new PrivilegeRow(u.name(), p))
            ).iterator();
    }
}
