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

import java.util.Objects;

import org.jetbrains.annotations.Nullable;

public class DropUserMapping extends Statement {

    @Nullable
    private final String userName;
    private final boolean ifExists;
    private final String server;

    public DropUserMapping(@Nullable String userName, boolean ifExists, String server) {
        this.userName = userName;
        this.ifExists = ifExists;
        this.server = server;
    }

    public String userName() {
        return userName;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public String server() {
        return server;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropUserMapping(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, ifExists, server);
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
        DropUserMapping other = (DropUserMapping) obj;
        return Objects.equals(userName, other.userName)
            && ifExists == other.ifExists
            && Objects.equals(server, other.server);
    }

    @Override
    public String toString() {
        return "DropUserMapping{userName=" + userName
            + ", ifExists=" + ifExists
            + ", serverName=" + server + "}";
    }
}
