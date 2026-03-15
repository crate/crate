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
import java.util.Set;

public final class CreateTableLike<T> extends Statement {

    private final Table<T> name;
    private final QualifiedName likeTableName;
    private final boolean ifNotExists;
    private final Set<LikeOption> includedOptions;

    public CreateTableLike(Table<T> name,
                           QualifiedName likeTableName,
                           boolean ifNotExists,
                           Set<LikeOption> includedOptions) {
        this.name = name;
        this.likeTableName = likeTableName;
        this.ifNotExists = ifNotExists;
        this.includedOptions = includedOptions;
    }

    public Table<T> name() {
        return this.name;
    }

    public QualifiedName likeTableName() {
        return this.likeTableName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Set<LikeOption> includedOptions() {
        return includedOptions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableLike(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CreateTableLike)) {
            return false;
        }
        CreateTableLike<?> that = (CreateTableLike<?>) o;
        return name.equals(that.name) &&
            likeTableName.equals(that.likeTableName) &&
            ifNotExists == that.ifNotExists &&
            includedOptions.equals(that.includedOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, likeTableName, ifNotExists, includedOptions);
    }

    @Override
    public String toString() {
        return "CreateTableLike{" +
               "name=" + name +
               ", likeTableName=" + likeTableName +
               ", includedOptions=" + includedOptions +
               '}';
    }
}
