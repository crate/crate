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

import static io.crate.role.PrivilegeType.DENY;
import static io.crate.role.PrivilegeType.GRANT;
import static io.crate.role.PrivilegeType.REVOKE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.common.FourFunction;
import io.crate.metadata.pgcatalog.OidHash;

public interface Roles {

    FourFunction<Role, Privilege.Permission, Privilege.Securable, Object, PrivilegeType> HAS_PRIVILEGE_FUNCTION =
        (r, t, c, o) -> r.privileges().matchPrivilege(t, c, (String) o);

    FourFunction<Role, Privilege.Permission, Privilege.Securable, Object, PrivilegeType> HAS_ANY_PRIVILEGE_FUNCTION =
        (r, t, c, o) -> r.privileges().matchPrivilegeOfAnyType(c, (String) o);

    FourFunction<Role, Privilege.Permission, Privilege.Securable, Object, PrivilegeType> HAS_SCHEMA_PRIVILEGE_FUNCTION =
        (r, t, c, o) -> {
            PrivilegeType result = PrivilegeType.REVOKE;
            for (Privilege privilege : r.privileges()) {
                if (privilege.ident().type() == t) {
                    if (privilege.ident().clazz() == Privilege.Securable.SCHEMA
                        && OidHash.schemaOid(privilege.ident().ident()) == (Integer) o) {
                        return privilege.state();
                    } else if (privilege.ident().clazz() == Privilege.Securable.CLUSTER) {
                        result = privilege.state();
                    }
                }
            }
            return result;
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
     * @param user             user
     * @param permission       privilege type
     * @param securable        privilege class (ie. CLUSTER, TABLE, etc)
     * @param subject          ident of the object
     */
    default boolean hasPrivilege(Role user, Privilege.Permission permission, Privilege.Securable securable, @Nullable String subject) {
        return user.isSuperUser()
            || hasPrivilege(user, permission, securable, subject, HAS_PRIVILEGE_FUNCTION) == GRANT;
    }

    /**
     * Checks if the user has a schema privilege that matches the given type and ident OID.
     * @param user           user
     * @param permission           privilege type
     * @param schemaOid      OID of the schema
     */
    default boolean hasSchemaPrivilege(Role user, Privilege.Permission permission, Integer schemaOid) {
        return user.isSuperUser()
            || hasPrivilege(user, permission, null, schemaOid, HAS_SCHEMA_PRIVILEGE_FUNCTION) == GRANT;
    }

    /**
     * Checks if the user has any privilege that matches the given class, type and ident
     * currently we check for any privilege, since Class is always CLUSTER and ident null.
     *
     * @param user        user
     * @param securable   privilege class (ie. CLUSTER, TABLE, etc)
     * @param subject     ident of the object
     */
    default boolean hasAnyPrivilege(Role user, Privilege.Securable securable, @Nullable String subject) {
        return user.isSuperUser()
            || hasPrivilege(user, null, securable, subject, HAS_ANY_PRIVILEGE_FUNCTION) == GRANT;
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
    private PrivilegeType hasPrivilege(
        Role role,
        Privilege.Permission permission,
        @Nullable Privilege.Securable securable,
        @Nullable Object subject,
        FourFunction<Role, Privilege.Permission, Privilege.Securable, Object, PrivilegeType> resolveType) {

        if (role.isSuperUser()) {
            return GRANT;
        }
        PrivilegeType resolution = resolveType.apply(role, permission, securable, subject);
        if (resolution == DENY || resolution == GRANT) {
            return resolution;
        }


        PrivilegeType result = REVOKE;
        for (String parentRoleName : role.grantedRoleNames()) {
            var parentRole = findRole(parentRoleName);
            assert parentRole != null : "role must exist";
            var partialResult = hasPrivilege(parentRole, permission, securable, subject, resolveType);
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
