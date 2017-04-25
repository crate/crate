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

import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.doc.DocSysColumns;

/**
 * Visitor to change regular column references into references using the DOC sys column.
 * <p/>
 * e.g.   s.t.colname -&gt; s.t._DOC['colname']
 */
public final class DocReferenceConverter {

    /**
     * will convert any references that are analyzed or not indexed to doc-references
     */
    public static Symbol toSourceLookup(Symbol tree) {
        return RefReplacer.replaceRefs(tree, DocReferenceConverter::toSourceLookup);
    }

    public static Reference toSourceLookup(Reference reference) {
        ReferenceIdent ident = reference.ident();
        if (ident.columnIdent().isSystemColumn()) {
            return reference;
        }
        if (reference.granularity() == RowGranularity.DOC && Schemas.isDefaultOrCustomSchema(ident.tableIdent().schema())) {
            return reference.getRelocated(
                new ReferenceIdent(ident.tableIdent(), ident.columnIdent().prepend(DocSysColumns.DOC.name())));
        }
        return reference;
    }

    private DocReferenceConverter() {
    }
}
