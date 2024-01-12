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

import static io.crate.role.PrivilegeState.DENY;
import static io.crate.role.PrivilegeState.GRANT;
import static io.crate.role.PrivilegeState.REVOKE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.common.FourFunction;
import io.crate.metadata.pgcatalog.OidHash;

public interface Roles {

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
     * Checks if the user has a privilege that matches the given securable, type, ident and
     * default schema. Currently only the type is checked since Class is always
     * CLUSTER and ident null.
     * @param user           user
     * @param permission     permission type
     * @param securable      Securable (ie. CLUSTER, TABLE, etc)
     * @param ident          ident of the object
     */
    default boolean hasPrivilege(Role user, Permission permission, Securable securable, @Nullable String ident) {
        return user.isSuperUser()
            || hasPrivilege(user, permission, securable, ident, (r, t, c, o) -> r.privileges().matchPrivilege(t, c, (String) o)) == GRANT;
    }

    /**
     * Checks if the user has a schema privilege that matches the given type and ident OID.
     * @param user           user
     * @param permission     permission type
     * @param schemaOid      OID of the schema
     */
    default boolean hasSchemaPrivilege(Role user, Permission permission, Integer schemaOid) {
        return user.isSuperUser()
            || hasPrivilege(user, permission, null, schemaOid, (r, t, c, o) -> r.matchSchema(t, (Integer) o)) == GRANT;
    }

    /**
     * Checks if the user has any privilege that matches the given securable, type and ident
     * currently we check for any privilege, since Class is always CLUSTER and ident null.
     *
     * @param user      user
     * @param securable securable (ie. CLUSTER, TABLE, etc)
     * @param ident     ident of the object
     */
    default boolean hasAnyPrivilege(Role user, Securable securable, @Nullable String ident) {
        return user.isSuperUser()
            || hasPrivilege(user, null, securable, ident, (r, t, c, o) -> r.privileges().matchPrivilegeOfAnyType(c, (String) o)) == GRANT;
    }

    Collection<Role> roles();

    default Set<String> findAllParents(String roleName) {
        Set<String> allParents = new HashSet<>();
        Role role = findRole(roleName);
        assert role != null : "role must exist";
        findParents(role, allParents);
        return allParents;
    }

    private void findParents(Role role, Set<String> allParents) {
        allParents.addAll(role.grantedRoleNames());
        for (var grantedRoleName : role.grantedRoleNames()) {
            var parentRole = findRole(grantedRoleName);
            assert parentRole != null : "parent role must exist";
            findParents(parentRole, allParents);
        }
    }


    /**
     * Resolves privilege recursively in a depth-first fashion.
     * DENY has precedence, so given a role, if for one of its parents the privilege resolves to DENY,
     * then the privilege resolves to DENY for the role.
     */
    private PrivilegeState hasPrivilege(
        Role role,
        Permission permission,
        @Nullable Securable securable,
        @Nullable Object object,
        FourFunction<Role, Permission, Securable, Object, PrivilegeState> function) {

        if (role.isSuperUser()) {
            return GRANT;
        }
        PrivilegeState resolution = function.apply(role, permission, securable, object);
        if (resolution == DENY || resolution == GRANT) {
            return resolution;
        }


        PrivilegeState result = REVOKE;
        for (String parentRoleName : role.grantedRoleNames()) {
            var parentRole = findRole(parentRoleName);
            assert parentRole != null : "role must exist";
            var partialResult = hasPrivilege(parentRole, permission, securable, object, function);
            if (partialResult == DENY) {
                return DENY;
            }
            if (result == REVOKE) {
                result = partialResult;
            }
        }
        return result;
    }
}
