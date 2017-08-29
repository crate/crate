/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.user;

import io.crate.analyze.user.Privilege;
import io.crate.analyze.user.PrivilegeIdent;
import io.crate.metadata.Schemas;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class UserPrivileges implements Iterable<Privilege> {

    private final Iterable<Privilege> privileges;
    private final Map<PrivilegeIdent, Privilege> privilegesMap;
    private final boolean anyClusterPrivilege;
    private final Map<String, Boolean> anySchemaPrivilege = new HashMap<>();
    private final Map<String, Boolean> anyTablePrivilege = new HashMap<>();

    UserPrivileges(Collection<Privilege> privileges) {
        this.privileges = privileges;
        privilegesMap = new HashMap<>(privileges.size());
        boolean anyClusterPrivilege = false;
        for (Privilege privilege : privileges) {
            PrivilegeIdent privilegeIdent = privilege.ident();
            this.privilegesMap.put(privilegeIdent, privilege);
            if (privilege.state() != Privilege.State.DENY) {
                switch (privilegeIdent.clazz()) {
                    case CLUSTER:
                        anyClusterPrivilege = true;
                        break;
                    case SCHEMA:
                        anySchemaPrivilege.put(privilegeIdent.ident(), true);
                        break;
                    case TABLE:
                        anyTablePrivilege.put(privilegeIdent.ident(), true);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported privilege class=" + privilegeIdent.clazz());
                }
            }
        }
        this.anyClusterPrivilege = anyClusterPrivilege;
    }

    /**
     * Try to match a privilege at the given collection ignoring the type.
     */
    boolean matchPrivilegeOfAnyType(Privilege.Clazz clazz,
                                    @Nullable String ident) {
        boolean foundPrivilege;
        switch (clazz) {
            case CLUSTER:
                foundPrivilege = hasAnyClusterPrivilege();
                break;
            case SCHEMA:
                foundPrivilege = hasAnySchemaPrivilege(ident);
                if (foundPrivilege == false) {
                    foundPrivilege = hasAnyClusterPrivilege();
                }
                break;
            case TABLE:
                foundPrivilege = hasAnyTablePrivilege(ident);
                if (foundPrivilege == false) {
                    String schemaIdent = Schemas.getSchemaName(ident);
                    foundPrivilege = hasAnySchemaPrivilege(schemaIdent);
                    if (foundPrivilege == false) {
                        foundPrivilege = hasAnyClusterPrivilege();
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unsupported privilege class=" + clazz);
        }
        return foundPrivilege;
    }

    /**
     * Try to match a privilege at the given collection.
     * If none is found for the current {@link Privilege.Clazz}, try to find one on the upper class.
     * If a privilege with a {@link Privilege.State#DENY} state is found, false is returned.
     */
    boolean matchPrivilege(@Nullable Privilege.Type type,
                           Privilege.Clazz clazz,
                           @Nullable String ident) {
        Privilege foundPrivilege = privilegesMap.get(new PrivilegeIdent(type, clazz, ident));
        if (foundPrivilege == null) {
            switch (clazz) {
                case SCHEMA:
                    foundPrivilege = privilegesMap.get(new PrivilegeIdent(type, Privilege.Clazz.CLUSTER, null));
                    break;
                case TABLE:
                    String schemaIdent = Schemas.getSchemaName(ident);
                    foundPrivilege = privilegesMap.get(new PrivilegeIdent(type, Privilege.Clazz.SCHEMA, schemaIdent));
                    if (foundPrivilege == null) {
                        foundPrivilege = privilegesMap.get(new PrivilegeIdent(type, Privilege.Clazz.CLUSTER, null));
                    }
                    break;
            }
        }

        if (foundPrivilege == null) {
            return false;
        }
        switch (foundPrivilege.state()) {
            case GRANT:
                return true;
            case DENY:
            default:
                return false;
        }
    }

    @Nonnull
    @Override
    public Iterator<Privilege> iterator() {
        return privileges.iterator();
    }

    private boolean hasAnyClusterPrivilege() {
        return anyClusterPrivilege;
    }

    private boolean hasAnySchemaPrivilege(String ident) {
        return anySchemaPrivilege.get(ident) != null;
    }

    private boolean hasAnyTablePrivilege(String ident) {
        return anyTablePrivilege.get(ident) != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPrivileges that = (UserPrivileges) o;
        return privileges.equals(that.privileges);
    }

    @Override
    public int hashCode() {
        return privileges.hashCode();
    }

}
