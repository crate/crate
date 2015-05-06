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
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Visitor to change regular column references into references using the DOC sys column.
 *
 * e.g.   s.t.colname -&gt; s.t._DOC['colname']
 */
public class DocReferenceConverter {

    private final static Visitor VISITOR = new Visitor();

    private final static Predicate<Reference> DEFAULT_PREDICATE = new Predicate<Reference>() {
        @Override
        public boolean apply(@Nullable Reference input) {
            assert input != null;

            ReferenceIdent ident = input.info().ident();
            if (ident.columnIdent().name().startsWith("_")) {
                return false;
            }

            String schema = ident.tableIdent().schema();
            if (schema != null && !ReferenceInfos.isDefaultOrCustomSchema(schema)) {
                return false;
            }
            return true;
        }
    };

    /**
     * re-writes any references to source lookup ( n -&gt; _doc['n'] )
     *
     * won't be converted: partition columns or non-doc-schema columns
     */
    public static Symbol convertIfPossible(Symbol symbol, TableInfo tableInfo) {
        final List<ReferenceInfo> partitionedByColumns = tableInfo.partitionedByColumns();
        Predicate<Reference> predicate = new Predicate<Reference>() {
            @Override
            public boolean apply(@Nullable Reference input) {
                assert input != null;
                return !partitionedByColumns.contains(input.info());
            }
        };
        return convertIf(symbol, predicate);
    }

    /**
     * will convert any references that are analyzed or not indexed to doc-references
     */
    public static Symbol convertIf(Symbol symbol, Predicate<Reference> predicate) {
        return VISITOR.process(symbol, Predicates.and(DEFAULT_PREDICATE, predicate));
    }

    private static class Visitor extends SymbolVisitor<Predicate<Reference>, Symbol> {

        private static Reference toSourceLookup(Reference reference) {
            ReferenceIdent ident = reference.info().ident();
            if (ident.columnIdent().name().equals(DocSysColumns.DOC.name())) {
                // already converted
                // symbols might be shared and visited twice.. prevent rewriting _doc[x] to _doc._doc[x]
                return reference;
            }
            List<String> path = new ArrayList<>(ident.columnIdent().path());
            if (path.isEmpty()) { // if it's empty it might be an empty immutableList
                path = Arrays.asList(ident.columnIdent().name());
            } else {
                path.add(0, ident.columnIdent().name());
            }
            return new Reference(
                    new ReferenceInfo(
                            new ReferenceIdent(ident.tableIdent(), DocSysColumns.DOC.name(), path),
                            reference.info().granularity(),
                            reference.valueType()
                    )
            );
        }

        @Override
        public Symbol visitFunction(Function symbol, Predicate<Reference> predicate) {
            List<Symbol> arguments = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                arguments.add(process(argument, predicate));
            }
            return new Function(symbol.info(), arguments);
        }

        @Override
        public Symbol visitDynamicReference(DynamicReference symbol, Predicate<Reference> predicate) {
            return visitReference(symbol, predicate);
        }

        @Override
        public Symbol visitReference(Reference symbol, Predicate<Reference> predicate) {
            if (predicate.apply(symbol)) {
                return toSourceLookup(symbol);
            }
            return symbol;
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Predicate<Reference> predicate) {
            return symbol;
        }
    }

    private DocReferenceConverter() {}
}
