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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.Update;
import io.crate.types.DataTypes;

import java.util.Map;

public class UpdateStatementAnalyzer extends DataStatementAnalyzer<UpdateAnalysis> {

    @Override
    public Symbol visitUpdate(Update node, UpdateAnalysis context) {
        process(node.relation(), context);

        for (Assignment assignment : node.assignements()) {
            process(assignment, context);
        }
        if (node.whereClause().isPresent()) {
            processWhereClause(node.whereClause().get(), context);
        }
        return null;
    }

    @Override
    protected Symbol visitTable(Table node, UpdateAnalysis context) {
        Preconditions.checkState(context.table() == null, "updating multiple tables is not supported");
        context.editableTable(TableIdent.of(node));
        return null;
    }

    @Override
    public Symbol visitAssignment(Assignment node, UpdateAnalysis context) {
        // unknown columns in strict objects handled in here
        Reference reference = (Reference)process(node.columnName(), context);
        final ColumnIdent ident = reference.info().ident().columnIdent();
        if (ident.name().startsWith("_")) {
            throw new IllegalArgumentException("Updating system columns is not allowed");
        }

        ColumnIdent clusteredBy = context.table().clusteredBy();
        if (clusteredBy != null && clusteredBy.equals(ident)) {
            throw new IllegalArgumentException("Updating a clustered-by column is currently not supported");
        }

        if (context.hasMatchingParent(reference.info(), UpdateAnalysis.HAS_OBJECT_ARRAY_PARENT)) {
            // cannot update fields of object arrays
            throw new IllegalArgumentException("Updating fields of object arrays is not supported");
        }

        // it's something that we can normalize to a literal
        Symbol value = process(node.expression(), context);
        Literal updateValue;
        try {
            updateValue = context.normalizeInputForReference(value, reference);
        } catch(IllegalArgumentException|UnsupportedOperationException e) {
            throw new ColumnValidationException(ident.fqn(), e);
        }

        for (ColumnIdent pkIdent : context.table().primaryKey()) {
            ensureNotUpdated(ident, updateValue, pkIdent, "Updating a primary key is not supported");
        }
        for (ColumnIdent partitionIdent : context.table.partitionedBy()) {
            ensureNotUpdated(ident, updateValue, partitionIdent, "Updating a partitioned-by column is not supported");
        }

        context.addAssignement(reference, updateValue);
        return null;
    }

    private void ensureNotUpdated(ColumnIdent columnUpdated,
                                  Literal newValue,
                                  ColumnIdent protectedColumnIdent,
                                  String errorMessage) {
        if (columnUpdated.equals(protectedColumnIdent)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (columnUpdated.isChildOf(protectedColumnIdent) &&
                !(newValue.valueType().equals(DataTypes.OBJECT)
                        && StringObjectMaps.fromMapByPath((Map) newValue.value(), protectedColumnIdent.path()) == null)) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

}
