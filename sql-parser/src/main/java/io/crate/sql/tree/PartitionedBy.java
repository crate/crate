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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;

public class PartitionedBy extends CrateTableOption {

    private final List<Expression> columns;

    public PartitionedBy(List<Expression> columns) {
        this.columns = columns;
    }

    public List<Expression> columns() {
        return columns;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionedBy that = (PartitionedBy) o;

        if (columns != null ? !columns.equals(that.columns) : that.columns != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("columns", columns).toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPartitionedBy(this, context);
    }
}
