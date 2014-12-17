/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.Table;

import java.util.ArrayList;

public abstract class AbstractInsertAnalyzer<T extends AbstractInsertAnalyzedStatement> extends DataStatementAnalyzer<T> {


    protected void handleInsertColumns(Insert node, int maxInsertValues, T context) {
        // allocate columnsLists
        int numColumns;

        if (node.columns().size() == 0) { // no columns given in statement
            numColumns = context.table().columns().size();
            if (maxInsertValues > numColumns) {
                throw new IllegalArgumentException("too many values");
            }
            context.columns(new ArrayList<Reference>(numColumns));

            int i = 0;
            for (ReferenceInfo columnInfo : context.table().columns()) {
                if (i >= maxInsertValues) { break; }
                addColumn(columnInfo.ident().columnIdent().name(), context, i);
                i++;
            }

        } else {
            numColumns = node.columns().size();
            context.columns(new ArrayList<Reference>(numColumns));
            if (maxInsertValues > node.columns().size()) {
                throw new IllegalArgumentException("too few values");
            }
            for (int i = 0; i < node.columns().size(); i++) {
                addColumn(node.columns().get(i), context, i);
            }
        }

        if (!context.table().hasAutoGeneratedPrimaryKey() && context.primaryKeyColumnIndices().size() == 0) {
            throw new IllegalArgumentException("Primary key is required but is missing from the insert statement");
        }
        ColumnIdent clusteredBy = context.table().clusteredBy();
        if (clusteredBy != null && !clusteredBy.name().equalsIgnoreCase("_id") && context.routingColumnIndex() < 0) {
            throw new IllegalArgumentException("Clustered by value is required but is missing from the insert statement");
        }


    }

    @Override
    protected Symbol visitTable(Table node, T context) {
        Preconditions.checkState(context.table() == null, "inserting into multiple tables is not supported");

        context.editableTable(TableIdent.of(node));

        if (context.table().isAlias() && !context.table().isPartitioned()) {
            throw new IllegalArgumentException("Table alias not allowed in INSERT statement.");
        }
        return null;
    }

    /**
     * validates the column and sets primary key / partitioned by / routing information as well as a
     * column Reference to the context.
     *
     * the created column reference is returned
     */
    protected Reference addColumn(String column, T context, int i) {
        assert context.table() != null;
        return addColumn(new ReferenceIdent(context.table().ident(), column), context, i);
    }

    /**
     * validates the column and sets primary key / partitioned by / routing information as well as a
     * column Reference to the context.
     *
     * the created column reference is returned
     */
    protected Reference addColumn(ReferenceIdent ident, T context, int i) {
        final ColumnIdent columnIdent = ident.columnIdent();
        Preconditions.checkArgument(!columnIdent.name().startsWith("_"), "Inserting system columns is not allowed");
        Preconditions.checkArgument(!columnIdent.name().contains("["), "Column ident must not contain '[' ");

        // set primary key column if found
        for (ColumnIdent pkIdent : context.table().primaryKey()) {
            if (pkIdent.getRoot().equals(columnIdent)) {
                context.addPrimaryKeyColumnIdx(i);
            }
        }

        // set partitioned column if found
        for (ColumnIdent partitionIdent : context.table().partitionedBy()) {
            if (partitionIdent.getRoot().equals(columnIdent)) {
                context.addPartitionedByIndex(i);
            }
        }

        // set routing if found
        ColumnIdent routing = context.table().clusteredBy();
        if (routing != null && (routing.equals(columnIdent) || routing.isChildOf(columnIdent))) {
            context.routingColumnIndex(i);
        }

        // ensure that every column is only listed once
        Reference columnReference = context.allocateUniqueReference(ident, true);
        context.columns().add(columnReference);
        return columnReference;
    }
}
