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

import org.elasticsearch.common.TriFunction;

import io.crate.Constants;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookup;

public class HasDatabasePrivilegeFunction extends HasPrivilegeFunction {

    public static final String NAME = "has_database_privilege";

    private static final TriFunction<User, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_DB_NAME =
        (user, db, privileges) -> {
            if (Constants.DB_NAME.equals(db) == false) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                 "database \"%s\" does not exist",
                                                                 db));
            }
            return checkPrivileges(user, privileges);
        };

    private static final TriFunction<User, Object, Collection<Privilege.Type>, Boolean> CHECK_BY_DB_OID =
        (user, db, privileges) -> {
            if (Constants.DB_OID != (Integer) db) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                 "database with OID \"%d\" does not exist",
                                                                 db));
            }
            return checkPrivileges(user, privileges);
        };

    private static boolean checkPrivileges(User user, Collection<Privilege.Type> privileges) {
        if (privileges.contains(Privilege.Type.DQL)) { // CONNECT
            return true;
        }

        boolean result = true;
        if (privileges.contains(Privilege.Type.DML)) { // TEMP privilege
            result = false;
        }
        if (privileges.contains(Privilege.Type.DDL)) { // CREATE privilege
            result = hasCreatePrivilege(user);
        }
        return result;
    }

    private static boolean hasCreatePrivilege(User user) {
        if (user.isSuperUser()) {
            return true;
        }
        for (Privilege p : user.privileges()) {
            if (p.ident().type() == Privilege.Type.DDL &&
                (p.ident().clazz() == Privilege.Clazz.SCHEMA || p.ident().clazz() == Privilege.Clazz.CLUSTER)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param privilege is a comma separated list.
     * Valid privileges are 'CONNECT', 'CREATE' and 'TEMP' or `TEMPORARY` which map to DQL, DDL and DML respectively.
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
            switch (p) {
                case "connect" -> toCheck.add(Privilege.Type.DQL);
                case "create" -> toCheck.add(Privilege.Type.DDL);
                case "temp" -> toCheck.add(Privilege.Type.DML);
                case "temporary" -> toCheck.add(Privilege.Type.DML);
                default ->
                    // Same error as PG
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                     "Unrecognized privilege type: %s",
                                                                     p));
            }
        }
        return toCheck;
    }

    public static void register(ScalarFunctionModule module) {
        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature, USER_BY_NAME, CHECK_BY_DB_NAME)
        );

        // Signature without user, takes user from session.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature,
                                                                            USER_BY_NAME, CHECK_BY_DB_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(), // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature,
                                                                            USER_BY_NAME, CHECK_BY_DB_NAME)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),  // User
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature,
                                                                            USER_BY_NAME, CHECK_BY_DB_OID)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.STRING.getTypeSignature(),  // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature,
                                                                            USER_BY_OID, CHECK_BY_DB_NAME)
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(), // User
                DataTypes.INTEGER.getTypeSignature(), // Database
                DataTypes.STRING.getTypeSignature(),  // Privilege
                DataTypes.BOOLEAN.getTypeSignature()
            ).withFeatures(DETERMINISTIC_ONLY),
            (signature, boundSignature) -> new HasDatabasePrivilegeFunction(signature, boundSignature,
                                                                            USER_BY_OID, CHECK_BY_DB_OID)
        );
    }

    protected HasDatabasePrivilegeFunction(Signature signature,
                                           BoundSignature boundSignature,
                                           BiFunction<UserLookup, Object, User> getUser,
                                           TriFunction<User, Object, Collection<Privilege.Type>, Boolean> checkPrivilege) {
        super(signature, boundSignature, getUser, checkPrivilege);
    }
}
