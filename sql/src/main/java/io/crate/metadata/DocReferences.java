/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
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
     * Rewrite the reference to do a source lookup if possible.
     * @see #toSourceLookup(Symbol)
     *
     * <pre>
     *     x -> _doc['x']
     * </pre>
     */
    public static Reference toSourceLookup(Reference reference) {
        ReferenceIdent ident = reference.ident();
        if (ident.columnIdent().isSystemColumn()) {
            return reference;
        }
        if (reference.granularity() == RowGranularity.DOC && Schemas.isDefaultOrCustomSchema(ident.tableIdent().schema())) {
            return reference.getRelocated(
                new ReferenceIdent(ident.tableIdent(), ident.columnIdent().prepend(DocSysColumns.Names.DOC)));
        }
        return reference;
    }

    private static Reference docRefToRegularRef(Reference reference) {
        ReferenceIdent ident = reference.ident();
        if (!ident.isColumn() && ident.columnIdent().name().equals(DocSysColumns.Names.DOC)) {
            return reference.getRelocated(new ReferenceIdent(ident.tableIdent(), ident.columnIdent().shiftRight()));
        }
        return reference;
    }
}
