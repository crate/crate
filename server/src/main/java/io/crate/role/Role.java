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

public class Role implements ToXContent {

    public static final Role CRATE_USER = new Role("crate",
        true,
        Set.of(),
        null,
        EnumSet.of(UserRole.SUPERUSER));

    public enum UserRole {
        SUPERUSER
    }

    public record Properties(boolean login, @Nullable SecureHash password) implements ToXContent {

        public static Properties fromXContent(XContentParser parser) throws IOException {
            boolean login = false;
            SecureHash secureHash = null;
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "login":
                        parser.nextToken();
                        login = parser.booleanValue();
                        break;
                    case "secure_hash":
                        secureHash = SecureHash.fromXContent(parser);
                        break;
                    default:
                        throw new ElasticsearchParseException(
                            "failed to parse role properties, unexpected field name: " + parser.currentName()
                        );
                }
            }
            return new Properties(login, secureHash);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("login", login);
            if (password != null) {
                password.toXContent(builder, params);
            }
            return builder;
        }
    }

    private final String name;
    private final RolePrivileges privileges;
    private final Set<UserRole> userRoles;

    private final Properties properties;

    public Role(String name,
                boolean login,
                Set<Privilege> privileges,
                @Nullable SecureHash password,
                Set<UserRole> userRoles) {
        this(name, privileges, userRoles, new Properties(login, password));
    }

    public Role(String name, Set<Privilege> privileges, Set<UserRole> userRoles, Properties properties) {
        if (properties.login == false) {
            assert properties.password == null : "Cannot create a Role with password";
            assert userRoles.isEmpty() : "Cannot create a Role with UserRoles";
        }
        this.name = name;
        this.privileges = new RolePrivileges(privileges);
        this.userRoles = userRoles;
        this.properties = properties;
    }

    public String name() {
        return name;
    }

    @Nullable
    public SecureHash password() {
        return properties.password();
    }

    public boolean isUser() {
        return properties.login();
    }

    public boolean isSuperUser() {
        return userRoles.contains(UserRole.SUPERUSER);
    }

    public RolePrivileges privileges() {
        return privileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role that = (Role) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(privileges, that.privileges) &&
               Objects.equals(userRoles, that.userRoles) &&
               Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, privileges, properties, userRoles);
    }

    @Override
    public String toString() {
        return (isUser() ? "User{" : "Role{") + name + ", " + (password() == null ? "null" : "*****") + '}';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startObject("properties");
        properties.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * A role is stored in the form of:
     * <p>
     *   "role1": {
     *     "properties" {
     *       "login" : true,
     *       "secure_hash": {
     *         "iterations": INT,
     *         "hash": BYTE[],
     *         "salt": BYTE[]
     *       }
     *     }
     *   }
     */
    public static Role fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException(
                "failed to parse a role, expecting the current token to be a field name, got " + parser.currentToken()
            );
        }

        String roleName = parser.currentName();
        Properties properties = null;

        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                if (parser.currentName().equals("properties")) {
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException(
                            "failed to parse a role, expected an start object token but got " + parser.currentToken()
                        );
                    }
                    properties = Properties.fromXContent(parser);
                    if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                        throw new ElasticsearchParseException(
                            "failed to parse a role, expected an end object token but got " + parser.currentToken()
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                            "failed to parse a Role, unexpected field name: " + parser.currentName()
                    );
                }
            }
            if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                throw new ElasticsearchParseException(
                    "failed to parse a role, expected an object token at the end, got: " + parser.currentToken()
                );
            }
        }
        if (properties == null) {
            throw new ElasticsearchParseException("failed to parse role properties, not found");
        }
        return new Role(roleName, Set.of(), Set.of(), properties);
    }
}
