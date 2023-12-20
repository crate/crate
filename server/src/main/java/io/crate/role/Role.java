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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.pgcatalog.OidHash;

public class Role implements ToXContent {

    public static final Role CRATE_USER = new Role("crate",
        true,
        Set.of(),
        null,
        EnumSet.of(UserRole.SUPERUSER));

    public enum UserRole {
        SUPERUSER
    }

    private final String name;
    private final boolean isUser;
    private final RolePrivileges privileges;
    @Nullable
    private final SecureHash password;
    private final Set<UserRole> userRoles;

    public Role(String name,
                boolean isUser,
                Set<Privilege> privileges,
                @Nullable SecureHash password,
                Set<UserRole> userRoles) {
        if (isUser == false) {
            assert password == null : "Cannot create a Role with password";
            assert userRoles.isEmpty() : "Cannot create a Role with UserRoles";
        }

        this.name = name;
        this.isUser = isUser;
        this.privileges = new RolePrivileges(privileges);
        this.password = password;
        this.userRoles = userRoles;
    }

    public String name() {
        return name;
    }

    @Nullable
    public SecureHash password() {
        return password;
    }

    public boolean isUser() {
        return isUser;
    }

    public boolean isSuperUser() {
        return userRoles.contains(UserRole.SUPERUSER);
    }

    public Iterable<Privilege> privileges() {
        return privileges;
    }

    /**
     * Checks if the user has a privilege that matches the given class, type, ident and
     * default schema. Currently only the type is checked since Class is always
     * CLUSTER and ident null.
     * @param type           privilege type
     * @param clazz          privilege class (ie. CLUSTER, TABLE, etc)
     * @param ident          ident of the object
     */
    public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, @Nullable String ident) {
        return isSuperUser() || privileges.matchPrivilege(type, clazz, ident);
    }

    /**
     * Checks if the user has a schema privilege that matches the given type and ident OID.
     * @param type           privilege type
     * @param schemaOid      OID of the schema
     */
    public boolean hasSchemaPrivilege(Privilege.Type type, Integer schemaOid) {
        if (isSuperUser()) {
            return true;
        }
        for (Privilege privilege : privileges) {
            if (privilege.state() == Privilege.State.GRANT && privilege.ident().type() == type) {
                if (privilege.ident().clazz() == Privilege.Clazz.CLUSTER) {
                    return true;
                }
                if (privilege.ident().clazz() == Privilege.Clazz.SCHEMA &&
                    OidHash.schemaOid(privilege.ident().ident()) == schemaOid) {
                    return true;
                }
            }
        }
        return false;

    }

    /**
     * Checks if the user has any privilege that matches the given class, type and ident
     * currently we check for any privilege, since Class is always CLUSTER and ident null.
     *
     * @param clazz privilege class (ie. CLUSTER, TABLE, etc)
     * @param ident ident of the object
     */
    public boolean hasAnyPrivilege(Privilege.Clazz clazz, @Nullable String ident) {
        return isSuperUser() || privileges.matchPrivilegeOfAnyType(clazz, ident);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role that = (Role) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(privileges, that.privileges) &&
               Objects.equals(password, that.password) &&
               Objects.equals(userRoles, that.userRoles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, privileges, password, userRoles);
    }

    @Override
    public String toString() {
        return (isUser ? "User{" : "Role{") + name + ", " + (password() == null ? "null" : "*****") + '}';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("is_user", isUser);
        if (password != null) {
            password.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static Role fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException(
                "failed to parse a role, expecting the current token to be a field name, got " + parser.currentToken()
            );
        }

        String roleName = parser.currentName();
        boolean isUser = false;
        SecureHash secureHash = null;

        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "is_user":
                        parser.nextToken();
                        isUser = parser.booleanValue();
                        break;
                    case "secure_hash":
                        secureHash = SecureHash.fromXContent(parser);
                        break;
                    default:
                        throw new ElasticsearchParseException(
                            "failed to parse a role, unexpected field name: " + parser.currentName()
                        );
                }
            }
            if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                throw new ElasticsearchParseException(
                    "failed to parse a role, expected an object token at the end, got: " + parser.currentToken()
                );
            }
        }
        return new Role(roleName, isUser, Set.of(), secureHash, Set.of());
    }
}
