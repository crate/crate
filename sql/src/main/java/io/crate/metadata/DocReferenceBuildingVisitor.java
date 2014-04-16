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

import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor to change regular column references into references using the DOC sys column.
 *
 * e.g.   s.t.colname -> s.t._DOC['colname']
 */
public class DocReferenceBuildingVisitor extends SymbolVisitor<Void, Symbol> {

    public final static DocReferenceBuildingVisitor INSTANCE = new DocReferenceBuildingVisitor();

    @Override
    public Symbol visitFunction(Function symbol, Void context) {
        int idx = 0;
        for (Symbol argument : symbol.arguments()) {
            symbol.setArgument(idx, process(argument, null));
            idx++;
        }
        return symbol;
    }

    @Override
    public Symbol visitReference(Reference symbol, Void context) {
        List<String> path = new ArrayList<>(symbol.info().ident().columnIdent().path());
        path.add(0, symbol.info().ident().columnIdent().name());
        return new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(symbol.info().ident().tableIdent(), DocSysColumns.DOC.name(), path),
                        symbol.info().granularity(),
                        symbol.valueType()
                )
        );
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, Void context) {
        return super.visitReference(symbol, context);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        return symbol;
    }

    private DocReferenceBuildingVisitor() {}
}
