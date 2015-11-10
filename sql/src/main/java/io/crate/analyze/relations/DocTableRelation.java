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
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

import javax.annotation.Nullable;

public class DocTableRelation extends AbstractTableRelation<DocTableInfo> {

    private static final ImmutableSet<ColumnIdent> HIDDEN_COLUMNS = ImmutableSet.of(DocSysColumns.DOCID);
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
            if (context.tableInfo.partitionedBy().contains(symbol.info().ident().columnIdent())) {
                throw new UnsupportedOperationException(
                        SymbolFormatter.format(
                                "cannot use partitioned column %s in ORDER BY clause", symbol));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
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
        return getField(path, false);
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        return getField(path, true);
    }

    private Field getField(Path path, boolean forWrite) {
        ColumnIdent ci = getColumnIdentSafe(path);
        if (HIDDEN_COLUMNS.contains(ci)) {
            return null;
        }
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(ci);
        if (referenceInfo == null) {
            referenceInfo = tableInfo.indexColumn(ci);
            if (referenceInfo == null) {
                DynamicReference dynamic = tableInfo.getDynamic(ci, forWrite);
                if (dynamic == null) {
                    return null;
                } else {
                    return allocate(ci, dynamic);
                }
            }
        }
        referenceInfo = checkNestedArray(ci, referenceInfo);
        // TODO: check allocated fields first?
        return allocate(ci, new Reference(referenceInfo));
    }

    public void validateOrderBy(Optional<OrderBy> orderBy) {
        if (orderBy.isPresent()) {
            for (Symbol symbol : orderBy.get().orderBySymbols()) {
                SORT_VALIDATOR.process(symbol, this);
            }
        }
    }



}
