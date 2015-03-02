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

package io.crate.analyze.where;

import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;

import javax.annotation.Nullable;

public class HasColumn {

    private static final Visitor VISITOR = new Visitor();

    public static boolean appliesTo(@Nullable Symbol input, ColumnIdent columnIdent) {
        if (input != null){
            return VISITOR.process(input, columnIdent);
        }
        return false;
    }

    private static class Visitor extends SymbolVisitor<ColumnIdent, Boolean> {

        @Override
        protected Boolean visitSymbol(Symbol symbol, ColumnIdent context) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, ColumnIdent context) {
            for (Symbol arg : symbol.arguments()) {
                if (process(arg, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitReference(Reference symbol, ColumnIdent context) {
            return context.equals(symbol.ident().columnIdent());
        }
    }
}
