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

public class CreateSchema extends Statement {

    public final String name;
    public final boolean ifNotExists;

    public CreateSchema(String name, boolean ifNotExists) {
        this.name = name;
        this.ifNotExists = ifNotExists;
    }

    public String name() {
        return name;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + (ifNotExists ? 1231 : 1237);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CreateSchema other
            && name.equals(other.name)
            && ifNotExists == other.ifNotExists;
    }

    @Override
    public String toString() {
        return "CreateSchema{" + name + ", ifNotExists=" + ifNotExists + "}";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateSchema(this, context);
    }
}
