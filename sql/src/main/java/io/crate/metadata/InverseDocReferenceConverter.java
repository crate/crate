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

import io.crate.analyze.symbol.Symbol;

import java.util.function.Predicate;

import static org.elasticsearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;

/**
 * Visitor to change a _doc reference into a regular column reference.
 * <p/>
 * e.g.   s.t._doc['col'] -&gt; s.t.col
 */
public class InverseDocReferenceConverter {

    private final static Visitor VISITOR = new Visitor();

    private final static Predicate<Reference> DEFAULT_PREDICATE = input -> {
        assert input != null : "input must not be null";
        ReferenceIdent ident = input.ident();
        String schema = ident.tableIdent().schema();
        return Schemas.isDefaultOrCustomSchema(schema);
    };

    /**
     * convert _doc references to regular column references
     */
    public static Symbol convertIf(Symbol symbol) {
        return VISITOR.process(symbol, DEFAULT_PREDICATE);
    }

    public static Reference toColumnReference(Reference reference) {
        ReferenceIdent ident = reference.ident();
        if (!ident.isColumn() && ident.columnIdent().name().startsWith(DOC_FIELD_NAME)) {
            return reference.getRelocated(
                new ReferenceIdent(ident.tableIdent(), ident.columnIdent().shiftRight())
            );
        }
        return reference;
    }

    private static class Visitor extends ReplacingSymbolVisitor<Predicate<Reference>> {

        public Visitor() {
            super(ReplaceMode.COPY);
        }

        @Override
        public Symbol visitReference(Reference symbol, Predicate<Reference> predicate) {
            if (predicate.test(symbol)) {
                return toColumnReference(symbol);
            }
            return symbol;
        }
    }

    private InverseDocReferenceConverter() {
    }
}
