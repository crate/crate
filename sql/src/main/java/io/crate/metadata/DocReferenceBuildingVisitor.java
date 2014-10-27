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

import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor to change regular column references into references using the DOC sys column.
 *
 * e.g.   s.t.colname -> s.t._DOC['colname']
 */
public class DocReferenceBuildingVisitor extends SymbolVisitor<TableInfo, Symbol> {

    private final static DocReferenceBuildingVisitor INSTANCE = new DocReferenceBuildingVisitor();

    public static Symbol convert(Symbol symbol, TableInfo tableInfo) {
        return INSTANCE.process(symbol, tableInfo);
    }

    @Override
    public Symbol visitFunction(Function symbol, TableInfo tableInfo) {
        int idx = 0;
        for (Symbol argument : symbol.arguments()) {
            symbol.setArgument(idx, process(argument, tableInfo));
            idx++;
        }
        return symbol;
    }

    @Override
    public Symbol visitReference(Reference symbol, TableInfo tableInfo) {
        ReferenceIdent ident = symbol.info().ident();
        if (ident.columnIdent().name().startsWith("_")) {
            return symbol;
        }
        String schema = ident.tableIdent().schema();
        if ((schema != null && !schema.equals(DocSchemaInfo.NAME)
            || tableInfo.partitionedByColumns().contains(symbol.info()))) {
            return symbol;
        }
        List<String> path = new ArrayList<>(ident.columnIdent().path());
        path.add(0, ident.columnIdent().name());
        return new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(ident.tableIdent(), DocSysColumns.DOC.name(), path),
                        symbol.info().granularity(),
                        symbol.valueType()
                )
        );
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, TableInfo tableInfo) {
        return visitReference(symbol, tableInfo);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, TableInfo tableInfo) {
        return symbol;
    }

    private DocReferenceBuildingVisitor() {}
}
