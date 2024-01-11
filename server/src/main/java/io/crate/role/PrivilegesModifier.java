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

package io.crate.role;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.Metadata;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;
import io.crate.role.metadata.RolesMetadata;

public final class PrivilegesModifier {

    private PrivilegesModifier() {
    }


    /**
     * Applies the provided privileges to the specified users.
     *
     * @return the number of affected privileges (doesn't count no-ops eg. granting a privilege a user already has)
     */
    public static long applyPrivileges(RolesMetadata rolesMetadata,
                                       Collection<String> userNames,
                                       Iterable<Privilege> newPrivileges) {
        long affectedPrivileges = 0L;
        for (String userName : userNames) {
            affectedPrivileges += applyPrivileges(rolesMetadata.roles(), userName, newPrivileges);
        }
        return affectedPrivileges;
    }

    private static long applyPrivileges(Map<String, Role> roles, String userName, Iterable<Privilege> newPrivileges) {
        var role = roles.get(userName);
        assert role != null : "role must exist for user=" + userName;

        HashSet<Privilege> privileges = HashSet.newHashSet(role.privileges().size());
        for (var privilege : role.privileges()) {
            privileges.add(privilege);
        }

        long affectedCount = 0L;
        for (Privilege newPrivilege : newPrivileges) {
            Iterator<Privilege> iterator = privileges.iterator();
            boolean userHadPrivilegeOnSameObject = false;
            while (iterator.hasNext()) {
                Privilege userPrivilege = iterator.next();
                PrivilegeIdent privilegeIdent = userPrivilege.ident();
                if (privilegeIdent.equals(newPrivilege.ident())) {
                    userHadPrivilegeOnSameObject = true;
                    if (newPrivilege.state().equals(PrivilegeState.REVOKE)) {
                        iterator.remove();
                        affectedCount++;
                        break;
                    } else {
                        // we only want to process a new GRANT/DENY privilege if the user doesn't already have it
                        if (userPrivilege.equals(newPrivilege) == false) {
                            iterator.remove();
                            privileges.add(newPrivilege);
                            affectedCount++;
                        }
                        break;
                    }
                }
            }

            if (userHadPrivilegeOnSameObject == false && newPrivilege.state().equals(PrivilegeState.REVOKE) == false) {
                // revoking a privilege that was not granted is a no-op
                affectedCount++;
                privileges.add(newPrivilege);
            }

            if (affectedCount > 0) {
                roles.put(userName, role.with(privileges));
            }
        }

        return affectedCount;
    }

    /**
     * Returns a copy of the {@link RolesMetadata} including a copied list of privileges if at least one
     * privilege was replaced. Otherwise returns the NULL to indicate that nothing was changed.
     * Privileges of {@link Securable#TABLE} whose idents are matching the given source ident are replaced
     * by a copy where the ident is changed to the given target ident.
     */
    @Nullable
    public static RolesMetadata maybeCopyAndReplaceTableIdents(RolesMetadata oldMetadata,
                                                               String sourceIdent,
                                                               String targetIdent) {
        boolean privilegesChanged = false;
        Map<String, Role> newRoles = HashMap.newHashMap(oldMetadata.roles().size());
        for (Map.Entry<String, Role> entry : oldMetadata.roles().entrySet()) {
            var role = entry.getValue();
            Set<Privilege> privileges = HashSet.newHashSet(role.privileges().size());
            for (Privilege privilege : role.privileges()) {
                PrivilegeIdent privilegeIdent = privilege.ident();
                if (privilegeIdent.securable() != Securable.TABLE) {
                    privileges.add(privilege);
                    continue;
                }

                String ident = privilegeIdent.ident();
                assert ident != null : "ident must not be null for securable 'TABLE'";
                if (ident.equals(sourceIdent)) {
                    privileges.add(new Privilege(privilege.state(), privilegeIdent.type(), privilegeIdent.securable(),
                        targetIdent, privilege.grantor()));
                    privilegesChanged = true;
                } else {
                    privileges.add(privilege);
                }
            }
            newRoles.put(entry.getKey(), role.with(privileges));
        }

        if (privilegesChanged) {
            return new RolesMetadata(newRoles);
        }
        return null;
    }

    public static long dropTableOrViewPrivileges(Metadata.Builder mdBuilder,
                                                 RolesMetadata rolesMetadata,
                                                 String tableOrViewIdent) {
        long affectedPrivileges = 0L;
        Map<String, Role> newRoles = HashMap.newHashMap(rolesMetadata.roles().size());
        for (Map.Entry<String, Role> entry : rolesMetadata.roles().entrySet()) {
            var role = entry.getValue();
            Set<Privilege> updatedPrivileges = new HashSet<>();
            for (Privilege privilege : role.privileges()) {
                PrivilegeIdent privilegeIdent = privilege.ident();
                Securable securable = privilegeIdent.securable();
                if (securable != Securable.TABLE && securable != Securable.VIEW) {
                    continue;
                }

                String ident = privilegeIdent.ident();
                assert ident != null : "ident must not be null for securable 'TABLE'";
                if (ident.equals(tableOrViewIdent)) {
                    affectedPrivileges++;
                } else {
                    updatedPrivileges.add(privilege);
                }
            }
            newRoles.put(role.name(), role.with(updatedPrivileges));
        }
        mdBuilder.putCustom(RolesMetadata.TYPE, new RolesMetadata(newRoles));
        return affectedPrivileges;
    }

    public static RolesMetadata swapPrivileges(RolesMetadata oldMetadata,
                                               RelationName source,
                                               RelationName target) {
        Map<String, Role> newRoles = HashMap.newHashMap(oldMetadata.roles().size());
        for (Map.Entry<String, Role> entry : oldMetadata.roles().entrySet()) {
            String user = entry.getKey();
            var role = entry.getValue();
            Set<Privilege> updatedPrivileges = new HashSet<>();
            for (Privilege privilege : role.privileges()) {
                PrivilegeIdent ident = privilege.ident();
                if (ident.securable() == Securable.TABLE) {
                    if (source.fqn().equals(ident.ident())) {
                        updatedPrivileges.add(
                            new Privilege(privilege.state(), ident.type(), ident.securable(), target.fqn(), privilege.grantor()));
                    } else if (target.fqn().equals(ident.ident())) {
                        updatedPrivileges.add(
                            new Privilege(privilege.state(), ident.type(), ident.securable(), source.fqn(), privilege.grantor()));
                    } else {
                        updatedPrivileges.add(privilege);
                    }
                } else {
                    updatedPrivileges.add(privilege);
                }
            }
            newRoles.put(user, role.with(updatedPrivileges));
        }
        return new RolesMetadata(newRoles);
    }
}
