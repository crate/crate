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

import java.util.List;

public class DropSchema extends Statement {

    private final List<String> names;
    private final boolean ifExists;

    public DropSchema(List<String> names, boolean ifExists) {
        this.names = names;
        this.ifExists = ifExists;
    }

    public List<String> names() {
        return names;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public int hashCode() {
        return 31 * names.hashCode() + (ifExists ? 1231 : 1237);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DropSchema other
            && names.equals(other.names)
            && ifExists == other.ifExists;
    }

    @Override
    public String toString() {
        return "DropSchema{names=" + names + ", ifExists=" + ifExists + "}";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropSchema(this, context);
    }
}
