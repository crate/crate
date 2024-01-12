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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.IndexParts;

public class RolePrivileges implements Iterable<Privilege> {

    private final Map<Subject, Privilege> privilegeByIdent;
    private final Policy anyClusterPrivilege;
    private final Map<String, Policy> schemaPrivileges = new HashMap<>();
    private final Map<String, Policy> tablePrivileges = new HashMap<>();
    private final Map<String, Policy> viewPrivileges = new HashMap<>();

    public RolePrivileges(Collection<Privilege> privileges) {
        privilegeByIdent = HashMap.newHashMap(privileges.size());
        Policy anyClusterPriv = Policy.REVOKE;
        for (Privilege privilege : privileges) {
            Subject subject = privilege.subject();
            privilegeByIdent.put(subject, privilege);
            switch (subject.securable()) {
                case CLUSTER:
                    if (anyClusterPriv != Policy.DENY) {
                        anyClusterPriv = privilege.policy();
                    }
                    break;
                case SCHEMA:
                    if (schemaPrivileges.get(subject.ident()) != Policy.DENY) {
                        schemaPrivileges.put(subject.ident(), privilege.policy());
                    }
                    break;
                case TABLE:
                    if (tablePrivileges.get(subject.ident()) != Policy.DENY) {
                        tablePrivileges.put(subject.ident(), privilege.policy());
                    }
                    break;
                case VIEW:
                    if (viewPrivileges.get(subject.ident()) != Policy.DENY) {
                        viewPrivileges.put(subject.ident(), privilege.policy());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported securable=" + subject.securable());
            }
        }
        this.anyClusterPrivilege = anyClusterPriv;
    }

    /**
     * Try to match a privilege at the given collection ignoring the type.
     */
    public Policy matchPrivilegeOfAnyType(Securable securable, @Nullable String ident) {
        Policy resolution;
        switch (securable) {
            case CLUSTER:
                resolution = hasAnyClusterPrivilege();
                break;
            case SCHEMA:
                resolution = hasAnySchemaPrivilege(ident);
                if (resolution == Policy.REVOKE) {
                    resolution = hasAnyClusterPrivilege();
                }
                break;
            case TABLE:
                resolution = hasAnyTablePrivilege(ident);
                if (resolution == Policy.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == Policy.REVOKE) {
                        resolution = hasAnyClusterPrivilege();
                    }
                }
                break;
            case VIEW:
                resolution = hasAnyViewPrivilege(ident);
                if (resolution == Policy.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == Policy.REVOKE) {
                        resolution = hasAnyClusterPrivilege();
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unsupported securable=" + securable);
        }
        return resolution;
    }

    /**
     * Try to match a privilege at the given collection.
     * If none is found for the current {@link Securable}, try to find one on the upper securable.
     * If a privilege with a {@link Policy#DENY} policy is found, false is returned.
     */
    public Policy matchPrivilege(@Nullable Permission permission, Securable securable, @Nullable String ident) {
        Privilege foundPrivilege = privilegeByIdent.get(new Subject(permission, securable, ident));
        if (foundPrivilege == null) {
            switch (securable) {
                case SCHEMA:
                    foundPrivilege = privilegeByIdent.get(new Subject(permission, Securable.CLUSTER, null));
                    break;
                case TABLE, VIEW:
                    String schemaIdent = new IndexParts(ident).getSchema();
                    foundPrivilege = privilegeByIdent.get(new Subject(permission, Securable.SCHEMA, schemaIdent));
                    if (foundPrivilege == null) {
                        foundPrivilege = privilegeByIdent.get(new Subject(permission, Securable.CLUSTER, null));
                    }
                    break;
                default:
            }
        }

        if (foundPrivilege == null) {
            return Policy.REVOKE;
        }
        return foundPrivilege.policy();
    }

    @NotNull
    @Override
    public Iterator<Privilege> iterator() {
        return privilegeByIdent.values().iterator();
    }

    private Policy hasAnyClusterPrivilege() {
        return anyClusterPrivilege;
    }

    private Policy hasAnySchemaPrivilege(String ident) {
        var policy = schemaPrivileges.get(ident);
        if (policy == null) {
            return Policy.REVOKE;
        }
        return policy;
    }

    private Policy hasAnyTablePrivilege(String ident) {
        var policy = tablePrivileges.get(ident);
        if (policy == null) {
            return Policy.REVOKE;
        }
        return policy;

    }

    private Policy hasAnyViewPrivilege(String ident) {
        var policy = viewPrivileges.get(ident);
        if (policy == null) {
            return Policy.REVOKE;
        }
        return policy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RolePrivileges that = (RolePrivileges) o;
        return privilegeByIdent.equals(that.privilegeByIdent);
    }

    @Override
    public int hashCode() {
        return privilegeByIdent.hashCode();
    }

    public int size() {
        return privilegeByIdent.size();
    }
}
