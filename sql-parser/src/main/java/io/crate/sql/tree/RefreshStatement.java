/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class RefreshStatement extends Statement {

    private final Table table;
    private final Optional<String> partitionIdent;

    public RefreshStatement(Table table, @Nullable Expression partitionIdent) {
        this.table = table;

        if (partitionIdent != null) {
            Preconditions.checkArgument(partitionIdent instanceof StringLiteral,
                    "invalid partition ident. must be string.");
            this.partitionIdent = Optional.of(((StringLiteral) partitionIdent).getValue());
        } else {
            this.partitionIdent = Optional.absent();
        }

    }

    public Table table() {
        return table;
    }

    public Optional<String> partitionIdent() {
        return partitionIdent;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, partitionIdent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RefreshStatement that = (RefreshStatement) o;

        if (!partitionIdent.equals(that.partitionIdent)) return false;
        if (!table.equals(that.table)) return false;

        return true;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("partitionIdent", partitionIdent)
                .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshStatement(this, context);
    }
}
