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

import org.jetbrains.annotations.Nullable;

import io.crate.common.FourFunction;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogTableDefinitions;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public class HasSchemaPrivilegeFunction extends HasPrivilegeFunction {

    public static final String NAME = "has_schema_privilege";

    private static final FourFunction<Roles, Role, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_SCHEMA_NAME =
        (roles, user, schema, privileges) -> {
            String schemaName = (String) schema;
            boolean result = false;
            for (Privilege.Type type: privileges) {
                // USAGE is allowed for public schemas
                if (type == Privilege.Type.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaName)) {
                    return true;
                }
                // Last argument is null as it's not used for Privilege.Clazz.SCHEMA
                result |= roles.hasPrivilege(user, type, Privilege.Clazz.SCHEMA, schemaName);
            }
            return result;
        };

    private static final FourFunction<Roles, Role, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_SCHEMA_OID =
        (roles, user, schema, privileges) -> {
            Integer schemaOid = (Integer) schema;
            boolean result = false;
            for (Privilege.Type type: privileges) {
                // USAGE is allowed for public schemas
                if (type == Privilege.Type.DQL && PgCatalogTableDefinitions.isPgCatalogOrInformationSchema(schemaOid)) {
                    return true;
                }
                result |= roles.hasSchemaPrivilege(user, type, schemaOid); // Returns true if has any privilege out of 2 possible
            }
            return result;
        };

    /**
     * @param privilege is a comma separated list.
     * Valid privileges are 'CREATE' and 'USAGE' which map to DDL and DQL respectively.
     * Case of the privilege string is not significant, and extra whitespace is allowed between privilege names.
     * Repetition of the valid argument is allowed.
     *
     * @see HasPrivilegeFunction#parsePrivileges(String)
     */
    @Nullable
    protected Collection<Privilege.Type> parsePrivileges(String privilege) {
        Collection<Privilege.Type> toCheck = new HashSet<>();
        String[] privileges = privilege.toLowerCase(Locale.ENGLISH).split(",");
        for (String p : privileges) {
            p = p.trim();
            if (p.equals("create")) {
                toCheck.add(Privilege.Type.DDL);
            } else if (p.equals("usage")) {
                toCheck.add(Privilege.Type.DQL);
            } else {
                // Same error as PG
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unrecognized privilege type: %s", p));
            }
        }
        return toCheck;
    }

    public static void register(ScalarFunctionModule module) {
        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_NAME)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_SCHEMA_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Schema
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasSchemaPrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_SCHEMA_NAME)
        );

        module.register(
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
                                         FourFunction<Roles, Role, Object, Collection<Privilege.Type>, Boolean> checkPrivilege) {
        super(signature, boundSignature, getUser, checkPrivilege);
    }
}
