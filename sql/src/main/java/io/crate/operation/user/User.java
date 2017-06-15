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

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class User {

    public enum Role {
        SUPERUSER
    }

    private final Set<Role> roles;

    private final String name;

    private final Set<Privilege> privileges;

    public User(String name, Set<Role> roles, Set<Privilege> privileges) {
        this.roles = roles;
        this.name = name;
        this.privileges = privileges;
    }

    public Set<Role> roles() {
        return roles;
    }

    public String name() {
        return name;
    }

    public Set<Privilege> privileges() {
        return privileges;
    }

    public boolean isSuperUser() {
        return roles.contains(Role.SUPERUSER);
    }

    /**
     * Checks if the user has a privilege that matches the given class, type and ident
     * currently only the type is checked since Class is always CLUSTER and ident null.
     * @param type           privilege type
     * @param clazz          privilege class (ie. CLUSTER, TABLE, etc)
     * @param ident          ident of the object
     */
    public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident) {
        Optional<Privilege> hasPrivilege = privileges.stream()
            .filter(p -> p.type().equals(type))
            .findFirst();
        return hasPrivilege.isPresent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User that = (User) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(roles, that.roles) &&
               Objects.equals(privileges, that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, roles, privileges);
    }
}
