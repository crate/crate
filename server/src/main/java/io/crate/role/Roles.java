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

package io.crate.role;

import java.util.Collection;

import org.jetbrains.annotations.Nullable;

import io.crate.common.FourFunction;
import io.crate.metadata.pgcatalog.OidHash;

public interface Roles {

    FourFunction<Role, Privilege.Type, Privilege.Clazz, Object, Boolean> HAS_PRIVILEGE_FUNCTION = (r, t, c, o) ->
        r.privileges().matchPrivilege(t, c, (String) o);

    FourFunction<Role, Privilege.Type, Privilege.Clazz, Object, Boolean> HAS_ANY_PRIVILEGE_FUNCTION = (r, t, c, o) ->
        r.privileges().matchPrivilegeOfAnyType(c, (String) o);

    FourFunction<Role, Privilege.Type, Privilege.Clazz, Object, Boolean> HAS_SCHEMA_PRIVILEGE_FUNCTION =
        (r, t, c, o) -> {
            for (Privilege privilege : r.privileges()) {
                if (privilege.state() == PrivilegeState.GRANT && privilege.ident().type() == t) {
                    if (privilege.ident().clazz() == Privilege.Clazz.CLUSTER) {
                        return true;
                    }
                    if (privilege.ident().clazz() == Privilege.Clazz.SCHEMA &&
                        OidHash.schemaOid(privilege.ident().ident()) == (Integer) o) {
                        return true;
                    }
                }
            }
            return false;
        };

    /**
     * finds a role by role name
     */
    @Nullable
    default Role findRole(String roleName) {
        for (var role : roles()) {
            if (role.name().equals(roleName)) {
                return role;
            }
        }
        return null;
    }

    /**
     * finds a user by username
     */
    @Nullable
    default Role findUser(String userName) {
        Role role = findRole(userName);
        if (role != null && role.isUser()) {
            return role;
        }
        return null;
    }

    /**
     * finds a user by OID
     */
    @Nullable
    default Role findUser(int userOid) {
        for (var role : roles()) {
            if (role.isUser() && userOid == OidHash.userOid(role.name())) {
                return role;
            }
        }
        return null;
    }

    /**
     * Checks if the user has a privilege that matches the given class, type, ident and
     * default schema. Currently only the type is checked since Class is always
     * CLUSTER and ident null.
     * @param user           user
     * @param type           privilege type
     * @param clazz          privilege class (ie. CLUSTER, TABLE, etc)
     * @param ident          ident of the object
     */
    default boolean hasPrivilege(Role user, Privilege.Type type, Privilege.Clazz clazz, @Nullable String ident) {
        return user.isSuperUser() || HAS_PRIVILEGE_FUNCTION.apply(user, type, clazz, ident);
    }

    /**
     * Checks if the user has a schema privilege that matches the given type and ident OID.
     * @param user           user
     * @param type           privilege type
     * @param schemaOid      OID of the schema
     */
    default boolean hasSchemaPrivilege(Role user, Privilege.Type type, Integer schemaOid) {
        return user.isSuperUser() || HAS_SCHEMA_PRIVILEGE_FUNCTION.apply(user, type, null, schemaOid);
    }

    /**
     * Checks if the user has any privilege that matches the given class, type and ident
     * currently we check for any privilege, since Class is always CLUSTER and ident null.
     *
     * @param user  user
     * @param clazz     privilege class (ie. CLUSTER, TABLE, etc)
     * @param ident     ident of the object
     */
    default boolean hasAnyPrivilege(Role user, Privilege.Clazz clazz, @Nullable String ident) {
        return user.isSuperUser() || HAS_ANY_PRIVILEGE_FUNCTION.apply(user, null, clazz, ident);
    }

    Collection<Role> roles();
}
