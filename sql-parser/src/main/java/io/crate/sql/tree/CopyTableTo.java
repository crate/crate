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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;

public class CopyTableTo extends CopyTo {

    private final Table table;
    private final List<Expression> columns;

    public CopyTableTo(Table table,
                       @Nullable List<Expression> columns,
                       boolean directoryUri,
                       Expression targetUri,
                       @Nullable GenericProperties genericProperties) {
        super(directoryUri, targetUri, genericProperties);
        this.table = table;
        this.columns = MoreObjects.firstNonNull(columns, ImmutableList.<Expression>of());
    }

    public Table table() {
        return table;
    }

    public List<Expression> columns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CopyTableTo copyTableTo = (CopyTableTo) o;

        if (directoryUri != copyTableTo.directoryUri) return false;
        if (!columns.equals(copyTableTo.columns)) return false;
        if (!genericProperties.equals(copyTableTo.genericProperties)) return false;
        if (!table.equals(copyTableTo.table)) return false;
        if (!targetUri.equals(copyTableTo.targetUri)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + (directoryUri ? 1 : 0);
        result = 31 * result + targetUri.hashCode();
        result = 31 * result + genericProperties.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table", table)
                .add("columns", columns)
                .add("directoryUri", directoryUri)
                .add("targetUri", targetUri)
                .add("genericProperties", genericProperties)
                .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCopyTableTo(this, context);
    }
}
