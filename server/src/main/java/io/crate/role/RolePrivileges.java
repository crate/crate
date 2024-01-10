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
    private final PrivilegeType anyClusterPrivilege;
    private final Map<String, PrivilegeType> schemaPrivileges = new HashMap<>();
    private final Map<String, PrivilegeType> tablePrivileges = new HashMap<>();
    private final Map<String, PrivilegeType> viewPrivileges = new HashMap<>();

    public RolePrivileges(Collection<Privilege> privileges) {
        privilegeByIdent = HashMap.newHashMap(privileges.size());
        PrivilegeType anyClusterPriv = PrivilegeType.REVOKE;
        for (Privilege privilege : privileges) {
            PrivilegeIdent privilegeIdent = privilege.ident();
            privilegeByIdent.put(privilegeIdent, privilege);
            switch (privilegeIdent.clazz()) {
                case CLUSTER:
                    if (anyClusterPriv != PrivilegeType.DENY) {
                        anyClusterPriv = privilege.state();
                    }
                    break;
                case SCHEMA:
                    if (schemaPrivileges.get(privilegeIdent.ident()) != PrivilegeType.DENY) {
                        schemaPrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                case TABLE:
                    if (tablePrivileges.get(privilegeIdent.ident()) != PrivilegeType.DENY) {
                        tablePrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                case VIEW:
                    if (viewPrivileges.get(privilegeIdent.ident()) != PrivilegeType.DENY) {
                        viewPrivileges.put(privilegeIdent.ident(), privilege.state());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported privilege class=" + privilegeIdent.clazz());
            }
        }
        this.anyClusterPrivilege = anyClusterPriv;
    }

    /**
     * Try to match a privilege at the given collection ignoring the type.
     */
    public PrivilegeType matchPrivilegeOfAnyType(Privilege.Securable clazz, @Nullable String ident) {
        PrivilegeType resolution;
        switch (clazz) {
            case CLUSTER:
                resolution = hasAnyClusterPrivilege();
                break;
            case SCHEMA:
                resolution = hasAnySchemaPrivilege(ident);
                if (resolution == PrivilegeType.REVOKE) {
                    resolution = hasAnyClusterPrivilege();
                }
                break;
            case TABLE:
                resolution = hasAnyTablePrivilege(ident);
                if (resolution == PrivilegeType.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == PrivilegeType.REVOKE) {
                        resolution = hasAnyClusterPrivilege();
                    }
                }
                break;
            case VIEW:
                resolution = hasAnyViewPrivilege(ident);
                if (resolution == PrivilegeType.REVOKE) {
                    String schemaIdent = new IndexParts(ident).getSchema();
                    resolution = hasAnySchemaPrivilege(schemaIdent);
                    if (resolution == PrivilegeType.REVOKE) {
                        resolution = hasAnyClusterPrivilege();
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unsupported privilege class=" + clazz);
        }
        return resolution;
    }

    /**
     * Try to match a privilege at the given collection.
     * If none is found for the current {@link Privilege.Securable}, try to find one on the upper class.
     * If a privilege with a {@link PrivilegeType#DENY} state is found, false is returned.
     */
    public PrivilegeType matchPrivilege(@Nullable Privilege.Permission type, Privilege.Securable clazz, @Nullable String ident) {
        Privilege foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, clazz, ident));
        if (foundPrivilege == null) {
            switch (clazz) {
                case SCHEMA:
                    foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Privilege.Securable.CLUSTER, null));
                    break;
                case TABLE, VIEW:
                    String schemaIdent = new IndexParts(ident).getSchema();
                    foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Privilege.Securable.SCHEMA, schemaIdent));
                    if (foundPrivilege == null) {
                        foundPrivilege = privilegeByIdent.get(new PrivilegeIdent(type, Privilege.Securable.CLUSTER, null));
                    }
                    break;
                default:
            }
        }

        if (foundPrivilege == null) {
            return PrivilegeType.REVOKE;
        }
        return foundPrivilege.state();
    }

    @NotNull
    @Override
    public Iterator<Privilege> iterator() {
        return privilegeByIdent.values().iterator();
    }

    private PrivilegeType hasAnyClusterPrivilege() {
        return anyClusterPrivilege;
    }

    private PrivilegeType hasAnySchemaPrivilege(String ident) {
        var state = schemaPrivileges.get(ident);
        if (state == null) {
            return PrivilegeType.REVOKE;
        }
        return state;
    }

    private PrivilegeType hasAnyTablePrivilege(String ident) {
        var state = tablePrivileges.get(ident);
        if (state == null) {
            return PrivilegeType.REVOKE;
        }
        return state;

    }

    private PrivilegeType hasAnyViewPrivilege(String ident) {
        var state = viewPrivileges.get(ident);
        if (state == null) {
            return PrivilegeType.REVOKE;
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
