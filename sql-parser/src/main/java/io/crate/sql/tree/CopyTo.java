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

import java.util.List;
import java.util.Optional;

public class CopyTo extends Statement {

    private final Table table;
    private final boolean directoryUri;
    private final Expression targetUri;

    private final GenericProperties genericProperties;
    private final List<Expression> columns;
    private final Optional<Expression> whereClause;

    public CopyTo(Table table,
                  List<Expression> columns,
                  Optional<Expression> whereClause,
                  boolean directoryUri,
                  Expression targetUri,
                  GenericProperties genericProperties) {

        this.table = table;
        this.directoryUri = directoryUri;
        this.targetUri = targetUri;
        this.genericProperties = genericProperties;
        this.columns = columns;
        this.whereClause = whereClause;
    }

    public Table table() {
        return table;
    }

    public boolean directoryUri() {
        return directoryUri;
    }

    public Expression targetUri() {
        return targetUri;
    }

    public List<Expression> columns() {
        return columns;
    }

    public GenericProperties genericProperties() {
        return genericProperties;
    }

    public Optional<Expression> whereClause() {
        return whereClause;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CopyTo copyTo = (CopyTo) o;

        if (directoryUri != copyTo.directoryUri) return false;
        if (!columns.equals(copyTo.columns)) return false;
        if (!genericProperties.equals(copyTo.genericProperties)) return false;
        if (!table.equals(copyTo.table)) return false;
        if (!targetUri.equals(copyTo.targetUri)) return false;
        if (!whereClause.equals(copyTo.whereClause)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + (directoryUri ? 1 : 0);
        result = 31 * result + targetUri.hashCode();
        result = 31 * result + genericProperties.hashCode();
        result = 31 * result + columns.hashCode();
        result = 31 * result + whereClause.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("table", table)
            .add("columns", columns)
            .add("whereClause", whereClause)
            .add("directoryUri", directoryUri)
            .add("targetUri", targetUri)
            .add("genericProperties", genericProperties)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCopyTo(this, context);
    }
}
