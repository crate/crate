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

import static io.crate.metadata.pgcatalog.PgCatalogTableDefinitions.RoleMember;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.role.Roles;

public final class PgAuthMembersTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_auth_members");

    private PgAuthMembersTable() {}

    public static SystemTable<RoleMember> create(Roles roles) {
        return SystemTable.<RoleMember>builder(IDENT)
            .add("oid", INTEGER, r -> OidHash.userOid(r.role() + r.member()))
            .add("roleid", INTEGER, r -> OidHash.userOid(r.role()))
            .add("member", INTEGER, r -> OidHash.userOid(r.member()))
            .add("grantor", INTEGER, r -> OidHash.userOid(r.grantor()))
            .add("admin_option", BOOLEAN, r -> roles.hasALPrivileges(roles.findRole(r.role())))
            .add("inherit_option", BOOLEAN, _ -> true) // Always inherit
            .add("set_option", BOOLEAN, _ -> false) // Only 'crate' superuser can do it, which is not listed
            .build();
    }
}
