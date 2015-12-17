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

package io.crate.metadata;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Visitor to change regular column references into references using the DOC sys column.
 * <p/>
 * e.g.   s.t.colname -&gt; s.t._DOC['colname']
 */
public class DocReferenceConverter {

    private final static Visitor VISITOR = new Visitor();

    private final static Predicate<Reference> DEFAULT_PREDICATE = new Predicate<Reference>() {
        @Override
        public boolean apply(@Nullable Reference input) {
            assert input != null;

            ReferenceIdent ident = input.info().ident();
            String schema = ident.tableIdent().schema();
            return ReferenceInfos.isDefaultOrCustomSchema(schema);
        }
    };

    /**
     * re-writes any references to source lookup ( n -&gt; _doc['n'] )
     * <p/>
     * won't be converted: partition columns or non-doc-schema columns
     */
    public static Symbol convertIfPossible(Symbol symbol, DocTableInfo tableInfo) {
        if (tableInfo.isPartitioned()) {
            return convertIf(symbol, getNotPartitionPredicate(tableInfo));
        } else {
            return convertIf(symbol);
        }
    }

    public static Predicate<Reference> getNotPartitionPredicate(DocTableInfo tableInfo) {
        if (!tableInfo.isPartitioned()) {
            return Predicates.alwaysTrue();
        }
        final List<ReferenceInfo> partitionedByColumns = tableInfo.partitionedByColumns();
        return new Predicate<Reference>() {
            @Override
            public boolean apply(@Nullable Reference input) {
                assert input != null;
                return !partitionedByColumns.contains(input.info());
            }
        };
    }

    /**
     * will convert any references that are analyzed or not indexed to doc-references
     */
    public static Symbol convertIf(Symbol symbol, @Nullable Predicate<Reference> predicate) {
        if (predicate == null) {
            return convertIf(symbol);
        }
        return VISITOR.process(symbol, Predicates.and(DEFAULT_PREDICATE, predicate));
    }

    /**
     * will convert any references that are analyzed or not indexed to doc-references
     */
    public static Symbol convertIf(Symbol symbol) {
        return VISITOR.process(symbol, DEFAULT_PREDICATE);
    }

    public static Reference toSourceLookup(Reference reference) {
        ReferenceIdent ident = reference.info().ident();
        if (ident.columnIdent().isSystemColumn()) {
            return reference;
        }
        return new Reference(reference.info().getRelocated(
                new ReferenceIdent(ident.tableIdent(), ident.columnIdent().prepend(DocSysColumns.DOC.name()))));
    }

    private static class Visitor extends ReplacingSymbolVisitor<Predicate<Reference>> {

        public Visitor() {
            super(false);
        }

        @Override
        public Symbol visitReference(Reference symbol, Predicate<Reference> predicate) {
            if (predicate.apply(symbol)) {
                return toSourceLookup(symbol);
            }
            return symbol;
        }
    }

    private DocReferenceConverter() {
    }
}
