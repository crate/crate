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

package io.crate.analyze;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.util.List;

public class BoundCreateTable {

    private final RelationName relationName;
    private final AnalyzedTableElements<Object> analyzedTableElements;
    private final TableParameter tableParameter;
    private final ColumnIdent routingColumn;
    private final boolean noOp;
    private final boolean ifNotExists;

    public BoundCreateTable(RelationName relationName,
                            AnalyzedTableElements<Object> tableElements,
                            TableParameter tableParameter,
                            @Nullable ColumnIdent routingColumn,
                            boolean ifNotExists,
                            Schemas schemas) {
        relationName.ensureValidForRelationCreation();
        boolean tableExists = schemas.tableExists(relationName);
        boolean viewExists = schemas.viewExists(relationName);
        if (ifNotExists && !viewExists) {
            noOp = tableExists;
        } else if (tableExists || viewExists) {
            throw new RelationAlreadyExists(relationName);
        } else {
            noOp = false;
        }
        this.ifNotExists = ifNotExists;
        this.relationName = relationName;
        this.analyzedTableElements = tableElements;
        this.tableParameter = tableParameter;

        if (routingColumn != null && routingColumn.name().equalsIgnoreCase("_id") == false) {
            this.routingColumn = routingColumn;
        } else {
            this.routingColumn = null;
        }
    }

    public boolean noOp() {
        return noOp;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @NotNull
    public List<List<String>> partitionedBy() {
        return analyzedTableElements().partitionedBy();
    }

    public boolean isPartitioned() {
        return !analyzedTableElements().partitionedByColumns.isEmpty();
    }

    /**
     * name of the template to create
     *
     * @return the name of the template to create or <code>null</code>
     * if no template is created
     */
    @Nullable
    public String templateName() {
        if (isPartitioned()) {
            return PartitionName.templateName(tableIdent().schema(), tableIdent().name());
        }
        return null;
    }

    @Nullable
    public String templatePrefix() {
        if (isPartitioned()) {
            return PartitionName.templatePrefix(tableIdent().schema(), tableIdent().name());
        }
        return null;
    }

    public RelationName tableIdent() {
        return relationName;
    }

    @Nullable
    public String routingColumn() {
        return routingColumn != null ? routingColumn.fqn() : null;
    }

    /**
     * return true if a columnDefinition with name <code>columnIdent</code> exists
     */
    boolean hasColumnDefinition(ColumnIdent columnIdent) {
        return (analyzedTableElements().columnIdents().contains(columnIdent) ||
                columnIdent.name().equalsIgnoreCase("_id"));
    }

    public AnalyzedTableElements<Object> analyzedTableElements() {
        return analyzedTableElements;
    }

    public TableParameter tableParameter() {
        return tableParameter;
    }
}
