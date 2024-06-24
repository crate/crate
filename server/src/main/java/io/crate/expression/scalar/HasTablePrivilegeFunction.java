/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import static io.crate.metadata.Scalar.Feature.DETERMINISTIC;
import static io.crate.metadata.Scalar.Feature.NULLABLE;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;

import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.types.DataTypes;

public class HasTablePrivilegeFunction {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "has_table_privilege");

    public static boolean checkByTableName(Roles roles, Role user, Object table, Collection<Permission> permissions, Schemas schemas) {
        String tableFqn = RelationName.fqnFromIndexName((String) table);
        return checkPrivileges(roles, user, tableFqn, permissions);
    }

    public static boolean checkByTableOid(Roles roles, Role user, Object table, Collection<Permission> permissions, Schemas schemas) {
        int tableOid = (int) table;
        RelationName relationName = schemas.getRelation(tableOid);
        String tableFqn;
        if (relationName == null) {
            // Proceed to checkPrivileges with tableFqn as 'null' which will return 'true' for a superuser or a
            // user with appropriate cluster scope privileges.
            // Note that a user with schema privileges will always get a 'false' since we cannot identify the schema
            // name from the given table oid(since relationName is null, schema name is unknown).
            tableFqn = null;
        } else {
            tableFqn = relationName.fqn();
        }
        return checkPrivileges(roles, user, tableFqn, permissions);
    }

    /**
     * @param permissionNames is a comma separated list.
     * Valid permissionNames are 'SELECT' and 'INSERT', 'UPDATE', and 'DELETE' which map to DQL and DML respectively.
     * Extra whitespaces between privilege names and repetition of a valid argument are allowed.
     *
     * @see HasPrivilegeFunction.ParsePermissions#parse(String)
     */
    public static Collection<Permission> parsePermissions(String permissionNames) {
        Collection<Permission> toCheck = new HashSet<>();
        String[] permissions = permissionNames.toLowerCase(Locale.ENGLISH).split(",");
        for (String p : permissions) {
            p = p.trim();
            if (p.equals("insert") || p.equals("update") || p.equals("delete")) {
                toCheck.add(Permission.DML);
            } else if (p.equals("select")) {
                toCheck.add(Permission.DQL);
            } else {
                // Same error as PG
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unrecognized permission: %s", p));
            }
        }
        return toCheck;
    }

    private static boolean checkPrivileges(Roles roles, Role user, String table, Collection<Permission> permissions) {
        for (Permission permission : permissions) {
            if (roles.hasPrivilege(user, permission, Securable.TABLE, table)) {
                return true;
            }
        }
        for (Permission permission : permissions) {
            if (roles.hasPrivilege(user, permission, Securable.VIEW, table)) {
                return true;
            }
        }
        return false;
    }

    public static void register(Functions.Builder module) {
        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasTablePrivilegeFunction::checkByTableName,
                HasTablePrivilegeFunction::parsePermissions
            )
        );

        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasTablePrivilegeFunction::checkByTableOid,
                HasTablePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasTablePrivilegeFunction::checkByTableName,
                HasTablePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasTablePrivilegeFunction::checkByTableOid,
                HasTablePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByOid,
                HasTablePrivilegeFunction::checkByTableName,
                HasTablePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByOid,
                HasTablePrivilegeFunction::checkByTableOid,
                HasTablePrivilegeFunction::parsePermissions
            )
        );
    }

    private HasTablePrivilegeFunction() {
    }
}
