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
import static io.crate.metadata.Scalar.Feature.STRICTNULL;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;

import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.role.Permission;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public class HasFunctionPrivilegeFunction {
    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "has_function_privilege");

    public static boolean checkByFunctionName(Roles roles, Role user, Object functionObject, Collection<Permission> permissions, Schemas schemas) {
        String functionName = (String) functionObject;
        if (!functionName.endsWith("()")) {
            new IllegalArgumentException(String.format(Locale.ENGLISH, "expected a left parenthesis"));
        }
        return true;
    }

    public static boolean checkByFunctionOid(Roles roles, Role user, Object functionObject, Collection<Permission> permissions, Schemas schemas) {
        return true;
    }

    /**
     * @param permissionNames is a comma separated list.
     * Valid permissionNames are 'CREATE' and 'USAGE' which map to DDL and DQL respectively.
     * Extra whitespaces between privilege names and repetition of a valid argument are allowed.
     *
     * @see HasPrivilegeFunction.ParsePermissions#parse(String)
     */
    public static Collection<Permission> parsePermissions(String permissionNames) {
        Collection<Permission> toCheck = new HashSet<>();
        String[] permissions = permissionNames.toLowerCase(Locale.ENGLISH).split(",");
        for (String p : permissions) {
            p = p.trim();
            if (p.equals("execute")) {
                toCheck.add(Permission.DDL);
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
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(), // function as TEXT
                    DataTypes.STRING.getTypeSignature()) // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasFunctionPrivilegeFunction::checkByFunctionName,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );

        // Signature without user, takes user from session
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.INTEGER.getTypeSignature(),  // function as OID
                    DataTypes.STRING.getTypeSignature())   // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasFunctionPrivilegeFunction::checkByFunctionOid,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );

        // Signature with user
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),  // user as TEXT
                    DataTypes.INTEGER.getTypeSignature(),  // function as OID
                    DataTypes.STRING.getTypeSignature())   // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasFunctionPrivilegeFunction::checkByFunctionOid,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );

        // Signature with user
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.INTEGER.getTypeSignature(),  // user as OID
                    DataTypes.INTEGER.getTypeSignature(),  // function as OID
                    DataTypes.STRING.getTypeSignature())   // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByOid,
                HasFunctionPrivilegeFunction::checkByFunctionOid,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );

        // Signature with user
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),  // user as TEXT
                    DataTypes.STRING.getTypeSignature(),  // function as TEXT
                    DataTypes.STRING.getTypeSignature())  // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasFunctionPrivilegeFunction::checkByFunctionName,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );

        // Signature with user
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.INTEGER.getTypeSignature(),  // user as OID
                    DataTypes.STRING.getTypeSignature(),  // function as TEXT
                    DataTypes.STRING.getTypeSignature())   // privilege
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(EnumSet.of(DETERMINISTIC, STRICTNULL))
                .build(),
            (signature, boundSignature) -> new HasPrivilegeFunction(
                signature,
                boundSignature,
                HasPrivilegeFunction::userByName,
                HasFunctionPrivilegeFunction::checkByFunctionOid,
                HasFunctionPrivilegeFunction::parsePermissions
            )
        );
    }
}
