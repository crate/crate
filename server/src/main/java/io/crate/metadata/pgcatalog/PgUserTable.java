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

package io.crate.metadata.pgcatalog;

import static io.crate.role.metadata.SysUsersTableInfo.PASSWORD_PLACEHOLDER;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.TIMESTAMPZ;

import java.util.Map;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.role.Role;
import io.crate.role.Roles;

public final class PgUserTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_user");

    private PgUserTable() {}

    public static SystemTable<Role> create(Roles roles) {
        return SystemTable.<Role>builder(IDENT)
            .add("usename", STRING, Role::name)
            .add("usesysid", INTEGER, r -> OidHash.userOid(r.name()))
            .add("usecreatedb", BOOLEAN, ignored -> null) // There is no create database functionality
            .add("usesuper", BOOLEAN, Role::isSuperUser)
            .add("userepl", BOOLEAN, roles::hasALPrivileges)
            .add("usebypassrls", BOOLEAN, ignored -> null) // No row-level security in CrateDB
            .add("passwd", STRING, r -> r.password() != null ? PASSWORD_PLACEHOLDER : null)
            .add("valuntil", TIMESTAMPZ, ignored -> null)
            .add("useconfig", STRING_ARRAY, r -> {
                Map<String, Object> settings = r.sessionSettings();
                if (settings.isEmpty()) {
                    return null;
                }
                return settings.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .toList();
            })
            .build();
    }
}
