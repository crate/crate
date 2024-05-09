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

import java.util.Collection;
import java.util.function.BiFunction;

import org.elasticsearch.common.inject.Provider;

import io.crate.common.FiveFunction;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.types.DataTypes;

public class HasTablePrivilegeFunction extends HasSchemaPrivilegeFunction {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "has_table_privilege");

    private static final FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> CHECK_BY_TABLE_NAME =
        (roles, user, table, permissions, schemasProvider) -> {
            String tableFqn = RelationName.fqnFromIndexName((String) table);
            boolean result = false;
            for (Permission permission : permissions) {
                result |= roles.hasPrivilege(user, permission, Securable.TABLE, tableFqn);
            }
            if (!result) {
                for (Permission permission : permissions) {
                    result |= roles.hasPrivilege(user, permission, Securable.VIEW, tableFqn);
                }
            }
            return result;
        };

    private static final FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> CHECK_BY_TABLE_OID =
        (roles, user, table, permissions, schemasProvider) -> {
            int tableOid = (int) table;
            String tableFqn = schemasProvider.get().oidToName(tableOid);
            if (tableFqn == null) {
                throw new IllegalArgumentException("Cannot find corresponding relation name by the given oid");
            }
            boolean result = false;
            for (Permission permission : permissions) {
                result |= roles.hasPrivilege(user, permission, Securable.TABLE, tableFqn);
            }
            if (!result) {
                for (Permission permission : permissions) {
                    result |= roles.hasPrivilege(user, permission, Securable.VIEW, tableFqn);
                }
            }
            return result;
        };

    public static void register(Functions.Builder module) {
        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_TABLE_NAME)
        );

        // Signature without user, takes user from session.
        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_TABLE_OID)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_TABLE_NAME)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_TABLE_OID)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_TABLE_NAME)
        );

        module.add(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Table
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasTablePrivilegeFunction(signature, boundSignature, USER_BY_OID, CHECK_BY_TABLE_OID)
        );
    }

    protected HasTablePrivilegeFunction(Signature signature,
                                        BoundSignature boundSignature,
                                        BiFunction<Roles, Object, Role> getUser,
                                        FiveFunction<Roles, Role, Object, Collection<Permission>, Provider<Schemas>, Boolean> checkPrivilege) {
        super(signature, boundSignature, getUser, checkPrivilege);
    }
}
