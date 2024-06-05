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

package io.crate.expression.scalar;

import static io.crate.metadata.Scalar.Feature.DETERMINISTIC;
import static io.crate.metadata.Scalar.Feature.NULLABLE;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;

import io.crate.Constants;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.role.Permission;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.types.DataTypes;

public class HasDatabasePrivilegeFunction {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "has_database_privilege");

    public static boolean checkByDbName(Roles roles, Role user, Object db, Collection<Permission> permissions, Schemas schemas) {
        if (Constants.DB_NAME.equals(db) == false) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "database \"%s\" does not exist",
                db));
        }
        return checkPrivileges(user, permissions);
    }

    public static boolean checkByDbOid(Roles roles, Role user, Object db, Collection<Permission> permissions, Schemas schemas) {
        if (Constants.DB_OID != (Integer) db) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "database with OID \"%s\" does not exist",
                db));
        }
        return checkPrivileges(user, permissions);
    }

    private static boolean checkPrivileges(Role user, Collection<Permission> permissions) {
        if (permissions.contains(Permission.DQL)) { // CONNECT
            return true;
        }

        boolean result = true;
        if (permissions.contains(Permission.DML)) { // TEMP privilege
            result = false;
        }
        if (permissions.contains(Permission.DDL)) { // CREATE privilege
            result = hasCreatePrivilege(user);
        }
        return result;
    }

    private static boolean hasCreatePrivilege(Role user) {
        if (user.isSuperUser()) {
            return true;
        }
        for (Privilege p : user.privileges()) {
            if (p.subject().permission() == Permission.DDL &&
                (p.subject().securable() == Securable.SCHEMA || p.subject().securable() == Securable.CLUSTER)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param permissionNames is a comma separated list.
     * Valid permissionNames are 'CONNECT', 'CREATE' and 'TEMP' or `TEMPORARY` which map to DQL, DDL and DML respectively.
     * Extra whitespaces between privilege names and repetition of the valid argument are allowed.
     *
     * @see HasPrivilegeFunction.ParsePermissions#parse(String)
     */
    public static Collection<Permission> parsePermissions(String permissionNames) {
        Collection<Permission> toCheck = new HashSet<>();
        String[] permissions = permissionNames.toLowerCase(Locale.ENGLISH).split(",");
        for (String p : permissions) {
            p = p.trim();
            switch (p) {
                case "connect" -> toCheck.add(Permission.DQL);
                case "create" -> toCheck.add(Permission.DDL);
                case "temp" -> toCheck.add(Permission.DML);
                case "temporary" -> toCheck.add(Permission.DML);
                default ->
                    // Same error as PG
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                     "Unrecognized permission: %s",
                                                                     p));
            }
        }
        return toCheck;
    }

    public static void register(Functions.Builder module) {
        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasDatabasePrivilegeFunction::checkByDbName,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );

        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasDatabasePrivilegeFunction::checkByDbOid,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasDatabasePrivilegeFunction::checkByDbName,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasDatabasePrivilegeFunction::checkByDbOid,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByOid,
                HasDatabasePrivilegeFunction::checkByDbName,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(EnumSet.of(DETERMINISTIC, NULLABLE)),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByOid,
                HasDatabasePrivilegeFunction::checkByDbOid,
                HasDatabasePrivilegeFunction::parsePermissions
            )
        );
    }

    private HasDatabasePrivilegeFunction() {
    }
}
