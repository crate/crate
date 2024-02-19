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

package io.crate.sql.tree;

import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

public class CreateUserMapping extends Statement {

    private final boolean ifNotExists;

    @Nullable
    private final String userName;

    private final String server;
    private final Map<String, Expression> options;

    public CreateUserMapping(boolean ifNotExists,
                             @Nullable String userName,
                             String server,
                             Map<String, Expression> options) {
        this.ifNotExists = ifNotExists;
        this.userName = userName;
        this.server = server;
        this.options = options;
    }

    /**
     * Optional user name. Use current user/role if null
     */
    @Nullable
    public String userName() {
        return userName;
    }

    public String server() {
        return server;
    }

    public Map<String, Expression> options() {
        return options;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateUserMapping(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ifNotExists, userName, server, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CreateUserMapping other = (CreateUserMapping) obj;
        return ifNotExists == other.ifNotExists
            && Objects.equals(userName, other.userName)
            && Objects.equals(server, other.server)
            && Objects.equals(options, other.options);
    }

    @Override
    public String toString() {
        return "CreateUserMapping{"
            + "ifNotExists=" + ifNotExists
            + ", userName=" + userName
            + ", server=" + server + "}";
    }
}
