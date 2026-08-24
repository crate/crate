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

import org.jspecify.annotations.Nullable;

public record SetSessionAuthorizationStatement(@Nullable String user,
                                               Scope scope) implements Statement {

    public enum Scope {
        SESSION, LOCAL
    }

    public SetSessionAuthorizationStatement(Scope scope) {
        this(null, scope);
    }

    /**
     * user is null for the following statements::
     * <p>
     * SET [ SESSION | LOCAL ] SESSION AUTHORIZATION DEFAULT
     * and
     * RESET SESSION AUTHORIZATION
     */
    @Nullable
    public String user() {
        return user;
    }

    public Scope scope() {
        return scope;
    }

    @Override
    public String toString() {
        return "SetSessionAuthorizationStatement{" +
               "user='" + user + '\'' +
               ", scope=" + scope +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetSessionAuthorizationStatement(this, context);
    }
}
