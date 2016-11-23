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

package io.crate.analyze.relations;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Path;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;

import javax.annotation.Nullable;
import java.util.List;

public class DocTableRelation extends AbstractTableRelation<DocTableInfo> {

    private static final ImmutableSet<ColumnIdent> HIDDEN_COLUMNS = ImmutableSet.of(DocSysColumns.FETCHID);
    private final static SortValidator SORT_VALIDATOR = new SortValidator();

    private static class SortValidator extends SymbolVisitor<DocTableRelation, Void> {

        @Override
        public Void visitFunction(Function symbol, DocTableRelation context) {
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, DocTableRelation context) {
            if (context.tableInfo.partitionedBy().contains(symbol.ident().columnIdent())) {
                throw new UnsupportedOperationException(
                    SymbolFormatter.format(
                        "cannot use partitioned column %s in ORDER BY clause", symbol));
            } else if (symbol.indexType() == Reference.IndexType.ANALYZED) {
                throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot ORDER BY '%s': sorting on analyzed/fulltext columns is not possible", symbol));
            } else if (symbol.indexType() == Reference.IndexType.NO) {
                throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot ORDER BY '%s': sorting on non-indexed columns is not possible", symbol));
            }
            return null;
        }
    }


    public DocTableRelation(DocTableInfo tableInfo) {
        super(tableInfo);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitDocTableRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        return getField(path, Operation.READ);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        ColumnIdent ci = toColumnIdent(path);
        if (HIDDEN_COLUMNS.contains(ci)) {
            return null;
        }
        if (operation == Operation.UPDATE) {
            ensureColumnCanBeUpdated(ci);
        }
        Reference reference = tableInfo.getReference(ci);
        if (reference == null) {
            reference = tableInfo.indexColumn(ci);
            if (reference == null) {
                DynamicReference dynamic = tableInfo.getDynamic(ci,
                    operation == Operation.INSERT || operation == Operation.UPDATE);
                if (dynamic == null) {
                    return null;
                } else {
                    return allocate(ci, dynamic);
                }
            }
        }
        reference = checkNestedArray(ci, reference);
        // TODO: check allocated fields first?
        return allocate(ci, reference);
    }

    /**
     * @throws io.crate.exceptions.ColumnValidationException if the column cannot be updated
     */
    private void ensureColumnCanBeUpdated(ColumnIdent ci) {
        if (ci.isSystemColumn()) {
            throw new ColumnValidationException(ci.toString(), "Updating a system column is not supported");
        }
        if (tableInfo.clusteredBy() != null) {
            ensureNotUpdated(ci, tableInfo.clusteredBy(), "Updating a clustered-by column is not supported");
        }
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            ensureNotUpdated(ci, pkIdent, "Updating a primary key is not supported");
        }
        List<GeneratedReference> generatedReferences = tableInfo.generatedColumns();

        for (ColumnIdent partitionIdent : tableInfo.partitionedBy()) {
            ensureNotUpdated(ci, partitionIdent, "Updating a partitioned-by column is not supported");
            int idx = generatedReferences.indexOf(tableInfo.getReference(partitionIdent));
            if (idx >= 0) {
                GeneratedReference generatedReference = generatedReferences.get(idx);
                for (Reference reference : generatedReference.referencedReferences()) {
                    ensureNotUpdated(ci, reference.ident().columnIdent(),
                        "Updating a column which is referenced in a partitioned by generated column expression is not supported");
                }
            }
        }
    }

    private static void ensureNotUpdated(ColumnIdent columnUpdated, ColumnIdent protectedColumnIdent, String errorMessage) {
        if (columnUpdated.equals(protectedColumnIdent) || protectedColumnIdent.isChildOf(columnUpdated)) {
            throw new ColumnValidationException(columnUpdated.toString(), errorMessage);
        }
    }

    public void validateOrderBy(Optional<OrderBy> orderBy) {
        if (orderBy.isPresent()) {
            for (Symbol symbol : orderBy.get().orderBySymbols()) {
                SORT_VALIDATOR.process(symbol, this);
            }
        }
    }


}
