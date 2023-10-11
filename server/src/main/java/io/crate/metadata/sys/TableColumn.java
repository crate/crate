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

package io.crate.metadata.sys;

import java.util.Map;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;

/**
 * A virtual column that is pointing to other columns.
 * <p>
 * E.g.
 * _doc['foo'] is pointing to 'foo'
 * <p>
 */
public class TableColumn {

    private final ColumnIdent column;
    private final Map<ColumnIdent, Reference> columns;

    /**
     * @param column  the virtual column
     * @param columns the columns this virtual column has underneath
     */
    public TableColumn(ColumnIdent column, Map<ColumnIdent, Reference> columns) {
        this.column = column;
        this.columns = columns;
    }

    public Reference getReference(RelationName relationName, ColumnIdent columnIdent) {
        if (!columnIdent.isChildOf(column)) {
            return null;
        }
        Reference info = columns.get(columnIdent.shiftRight());
        if (info == null) {
            return null;
        }
        return info.withReferenceIdent(new ReferenceIdent(relationName, columnIdent));
    }
}
