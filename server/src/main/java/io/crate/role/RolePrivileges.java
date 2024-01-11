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

    private final Map<PrivilegeIdent, Privilege> privilegeByIdent;
    private final PrivilegeState anyClusterPrivilege;
    private final Map<String, PrivilegeState> schemaPrivileges = new HashMap<>();
    private final Map<String, PrivilegeState> tablePrivileges = new HashMap<>();
    private final Map<String, PrivilegeState> viewPrivileges = new HashMap<>();

    public RolePrivileges(Collection<Privilege> privileges) {
        privilegeByIdent = HashMap.newHashMap(privileges.size());
        PrivilegeState anyClusterPriv = PrivilegeState.REVOKE;
        for (Privilege privilege : privileges) {
            PrivilegeIdent privilegeIdent = privilege.ident();
            privilegeByIdent.put(privilegeIdent, privilege);
            switch (privilegeIdent.securable()) {
                case CLUSTER:
                    if (anyClusterPriv != PrivilegeState.DENY) {
                        anyClusterPriv = privilege.state();
                    }
                    break;
                case SCHEMA:
                    if (schemaPrivileges.get(privilegeIdent.ident()) != PrivilegeState.DENY) {
                        schemaPrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                case TABLE:
                    if (tablePrivileges.get(privilegeIdent.ident()) != PrivilegeState.DENY) {
                        tablePrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                case VIEW:
                    if (viewPrivileges.get(privilegeIdent.ident()) != PrivilegeState.DENY) {
                        viewPrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported securable=" + privilegeIdent.securable());
            }
        }
        this.anyClusterPrivilege = anyClusterPriv;
    }

    /**
     * Try to match a privilege at the given collection ignoring the type.
     */
    public PrivilegeState matchPrivilegeOfAnyType(Securable securable, @Nullable String ident) {
        PrivilegeState resolution;
        switch (securable) {
            case CLUSTER:
                resolution = hasAnyClusterPrivilege();
                break;
            case SCHEMA:
                resolution = hasAnySchemaPrivilege(ident);
                if (resolution == PrivilegeState.REVOKE) {
                    resolution = hasAnyClusterPrivilege();
                }
                break;
            case TABLE:
                resolution = hasAnyTablePrivilege(ident);
                if (resolution == PrivilegeState.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == PrivilegeState.REVOKE) {
                        resolution = hasAnyClusterPrivilege();
                    }
                }
                break;
            case VIEW:
                resolution = hasAnyViewPrivilege(ident);
                if (resolution == PrivilegeState.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == PrivilegeState.REVOKE) {
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
     * If a privilege with a {@link PrivilegeState#DENY} state is found, false is returned.
     */
    public PrivilegeState matchPrivilege(@Nullable Privilege.Type type, Securable securable, @Nullable String ident) {
        Privilege foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, securable, ident));
        if (foundPrivilege == null) {
            switch (securable) {
                case SCHEMA:
                    foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Securable.CLUSTER, null));
                    break;
                case TABLE, VIEW:
                    String schemaIdent = new IndexParts(ident).getSchema();
                    foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Securable.SCHEMA, schemaIdent));
                    if (foundPrivilege == null) {
                        foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Securable.CLUSTER, null));
                    }
                    break;
                default:
            }
        }

        if (foundPrivilege == null) {
            return PrivilegeState.REVOKE;
        }
        return foundPrivilege.state();
    }

    @NotNull
    @Override
    public Iterator<Privilege> iterator() {
        return privilegeByIdent.values().iterator();
    }

    private PrivilegeState hasAnyClusterPrivilege() {
        return anyClusterPrivilege;
    }

    private PrivilegeState hasAnySchemaPrivilege(String ident) {
        var state = schemaPrivileges.get(ident);
        if (state == null) {
            return PrivilegeState.REVOKE;
        }
        return state;
    }

    private PrivilegeState hasAnyTablePrivilege(String ident) {
        var state = tablePrivileges.get(ident);
        if (state == null) {
            return PrivilegeState.REVOKE;
        }
        return state;

    }

    private PrivilegeState hasAnyViewPrivilege(String ident) {
        var state = viewPrivileges.get(ident);
        if (state == null) {
            return PrivilegeState.REVOKE;
        }
        return state;
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
