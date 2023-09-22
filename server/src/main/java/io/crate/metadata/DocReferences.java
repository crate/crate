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

package io.crate.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocSysColumns;

/**
 * Visitor to change a _doc reference into a regular column reference.
 * <p/>
 * e.g.   s.t._doc['col'] -&gt; s.t.col
 */
public final class DocReferences {

    private DocReferences() {
    }

    /**
     * Convert any _doc references to regular column references
     * <pre>
     *     _doc['x'] -> x
     * </pre>
     */
    public static Symbol inverseSourceLookup(Symbol symbol) {
        return RefReplacer.replaceRefs(symbol, DocReferences::docRefToRegularRef);
    }

    /**
     * Replace any suitable references within tree with _doc references.
     * Suitable references are any non-system columns from user tables with DOC granularity.
     * <pre>
     *     x -> _doc['x']
     * </pre>
     */
    public static Symbol toSourceLookup(Symbol tree) {
        return RefReplacer.replaceRefs(tree, DocReferences::toSourceLookup);
    }

    /**
     * Replace any suitable references within tree with _doc references if the given condition matches.
     * Suitable references are any non-system columns from user tables with DOC granularity.
     * <pre>
     *     x -> _doc['x']
     * </pre>
     */
    public static Symbol toSourceLookup(Symbol tree, Predicate<Reference> condition) {
        return RefReplacer.replaceRefs(tree, r -> toSourceLookup(r, condition));
    }

    /**
     * Rewrite the reference to do a source lookup if possible.
     * @see #toSourceLookup(Symbol)
     *
     * <pre>
     *     x -> _doc['x']
     * </pre>
     */
    public static Reference toSourceLookup(Reference reference) {
        return toSourceLookup(reference, r -> true);
    }

    private static Reference toSourceLookup(Reference reference, Predicate<Reference> condition) {
        ReferenceIdent ident = reference.ident();
        if (ident.columnIdent().isSystemColumn()) {
            return reference;
        }
        if (reference.granularity() == RowGranularity.DOC && Schemas.isDefaultOrCustomSchema(ident.tableIdent().schema())
            && condition.test(reference)) {
            return reference.getRelocated(
                new ReferenceIdent(ident.tableIdent(), ident.columnIdent().prepend(DocSysColumns.Names.DOC)));
        }
        return reference;
    }

    public static Reference docRefToRegularRef(Reference ref) {
        ColumnIdent column = ref.column();
        if (!column.isRoot() && column.name().equals(DocSysColumns.Names.DOC)) {
            return ref.getRelocated(new ReferenceIdent(ref.ident().tableIdent(), column.shiftRight()));
        }
        return ref;
    }

    public static List<Reference> applyOid(Collection<Reference> sourceReferences,
                                           LongSupplier columnOidSupplier) {
        List<Reference> references = new ArrayList<>(sourceReferences.size());
        Map<ColumnIdent, Reference> referencesMap = new HashMap<>(sourceReferences.size());
        for (var ref : sourceReferences) {
            var newRef = ref.applyColumnOid(columnOidSupplier);
            references.add(newRef);
            referencesMap.put(newRef.column(), newRef);
        }

        for (var i = 0; i < references.size(); i++) {
            var ref = references.get(i);
            if (ref instanceof IndexReference indexReference) {
                List<Reference> newSources = new ArrayList<>(indexReference.columns().size());
                for (var sourceRef : indexReference.columns()) {
                    newSources.add(referencesMap.get(sourceRef.column()));
                }
                references.set(i, indexReference.updateColumns(newSources));
            }
        }
        return references;
    }
}
