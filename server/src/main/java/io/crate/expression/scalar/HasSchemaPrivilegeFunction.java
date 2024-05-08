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

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.function.BiFunction;

import org.elasticsearch.common.inject.Provider;
import org.jetbrains.annotations.Nullable;

import io.crate.common.FiveFunction;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogTableDefinitions;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.types.DataTypes;

public class HasSchemaPrivilegeFunction extends HasPrivilegeFunction {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "has_schema_privilege");

    private static final FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> CHECK_BY_SCHEMA_NAME =
        (roles, user, schema, permissions, schemasProvider) -> {
            String schemaName = (String) schema;
            boolean result = false;
            for (Permission permission : permissions) {
                // USAGE is allowed for public schemas
                if (permission == Permission.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaName)) {
                    return true;
                }
                // Last argument is null as it's not used for Privilege.Securable.SCHEMA
                result |= roles.hasPrivilege(user, permission, Securable.SCHEMA, schemaName);
            }
            return result;
        };

    private static final FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> CHECK_BY_SCHEMA_OID =
        (roles, user, schema, permissions, schemasProvider) -> {
            Integer schemaOid = (Integer) schema;
            boolean result = false;
            for (Permission permission : permissions) {
                // USAGE is allowed for public schemas
                if (permission == Permission.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaOid)) {
                    return true;
                }
                result |= roles.hasSchemaPrivilege(user, permission, schemaOid); // Returns true if has any privilege out of 2 possible
            }
            return result;
        };

    /**
     * @param permissionNames is a comma separated list.
     * Valid permissionNames are 'CREATE' and 'USAGE' which map to DDL and DQL respectively.
     * Extra whitespaces between privilege names and repetition of a valid argument are allowed.
     *
     * @see HasPrivilegeFunction#parsePermissions(String)
     */
    @Nullable
    protected Collection<Permission> parsePermissions(String permissionNames) {
        Collection<Permission> toCheck = new HashSet<>();
        String[] permissions = permissionNames.toLowerCase(Locale.ENGLISH).split(",");
        for (String p : permissions) {
            p = p.trim();
            if (p.equals("create")) {
                toCheck.add(Permission.DDL);
            } else if (p.equals("usage")) {
                toCheck.add(Permission.DQL);
            } else {
                // Same error as PG
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unrecognized permission: %s", p));
            }
        }
        return toCheck;
    }

    public static void register(Functions.Builder module) {
        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_SCHEMA_NAME)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_SCHEMA_OID)
        );
    }

    protected HasSchemaPrivilegeFunction(Signature signature,
                                         BoundSignature boundSignature,
                                         BiFunction<Roles, Object, Role> getUser,
                                         FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> checkPrivilege) {
        super(signature, boundSignature, getUser, checkPrivilege);
    }
}
