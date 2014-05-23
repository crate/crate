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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.Update;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
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
        Literal updateValue = context.normalizeInputForReference(value, reference);

        // check for primary keys
        for (ColumnIdent primaryKeyIdent : Iterables.filter(context.table().primaryKey(), new Predicate<ColumnIdent>() {
            @Override
            public boolean apply(@Nullable ColumnIdent input) {
                return input != null && (input.equals(ident) || ident.isChildOf(input));
            }
        })) {
            if (!ident.equals(primaryKeyIdent) && updateValue.valueType().equals(DataTypes.OBJECT)) {
                // we might have a nested primary key column
                if (StringObjectMaps.getByPath(((Map)updateValue.value()),
                        StringUtils.PATH_JOINER.join(primaryKeyIdent.path())) == null) {
                    // no primary key column found
                    continue;
                }
            }
            throw new IllegalArgumentException("Updating a primary key is currently not supported");
        }

        // check for partitioned columns
        for (ColumnIdent partitionedIdent : Iterables.filter(context.table().partitionedBy(), new Predicate<ColumnIdent>() {
            @Override
            public boolean apply(@Nullable ColumnIdent input) {
                return input != null && (input.equals(ident) || ident.isChildOf(input));
            }
        })) {
            if (!ident.equals(partitionedIdent) && updateValue.valueType().equals(DataTypes.OBJECT)) {
                // we might have a nested partitioned by column
                if (StringObjectMaps.getByPath((Map)updateValue.value(), StringUtils.PATH_JOINER.join(partitionedIdent.path())) == null) {
                    // no partitioned column found
                    continue;
                }
            }
            throw new IllegalArgumentException("Updating a partitioned-by column is currently not supported");
        }
        context.addAssignement(reference, updateValue);
        return null;
    }

}
