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

import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.TIMESTAMPZ;
import static io.crate.role.metadata.SysUsersTableInfo.PASSWORD_PLACEHOLDER;

import io.crate.exceptions.MissingPrivilegeException;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.role.Privilege;
import io.crate.role.Privileges;
import io.crate.role.Role;

public final class PgRolesTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_roles");

    private PgRolesTable() {}

    public static SystemTable<Role> create() {
        return SystemTable.<Role>builder(IDENT)
            .add("rolname", STRING, Role::name)
            .add("rolsuper", BOOLEAN, Role::isSuperUser)
            .add("rolinherit", BOOLEAN, r -> true) // Always inherit
            .add("rolcreaterole", BOOLEAN, PgRolesTable::canCreateRoleOrSubscription)
            .add("rolcreatedb", BOOLEAN, ignored -> null) // There is no create database functionality
            .add("rolcanlogin", BOOLEAN, Role::isUser)
            .add("rolreplication", BOOLEAN, PgRolesTable::canCreateRoleOrSubscription)
            .add("rolconnlimit", INTEGER, r -> -1)
            .add("rolpassword", STRING, r -> r.password() != null ? PASSWORD_PLACEHOLDER : null)
            .add("rolvaliduntil", TIMESTAMPZ, ignored -> null)
            .add("rolbypassrls", BOOLEAN, ignored -> null)
            .add("rolconfig", STRING_ARRAY, ignored -> null)
            .add("oid", INTEGER, r -> OidHash.userOid(r.name()))
            .build();
    }

    private static boolean canCreateRoleOrSubscription(Role role) {
        try {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.AL,
                Privilege.Clazz.CLUSTER,
                null,
                role);
            return true;
        } catch (MissingPrivilegeException e) {
            return false;
        }
    }
}
