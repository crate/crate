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

package io.crate.testing;

import com.google.common.base.Joiner;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.planner.symbol.*;

import java.util.List;
import java.util.Locale;

public class QueryPrinter extends SymbolVisitor<Void, String> {

    private static final Joiner argJoiner = Joiner.on(", ");
    private static final QueryPrinter INSTANCE = new QueryPrinter();

    private QueryPrinter() {}


    public static String print(Symbol query) {
        return INSTANCE.process(query, null);
    }

    @Override
    public String visitFunction(Function symbol, Void context) {
        String[] args = new String[symbol.arguments().size()];

        List<Symbol> arguments = symbol.arguments();
        for (int i = 0; i < arguments.size(); i++) {
            Symbol arg = arguments.get(i);
            args[i] = arg.accept(this, context);
        }
        return format("%s(%s)",
                symbol.info().ident().name(),
                argJoiner.join(args));
    }

    private static String format(String tmpl, Object ... args) {
        return String.format(Locale.ENGLISH, tmpl, args);
    }

    @Override
    protected String visitSymbol(Symbol symbol, Void context) {
        return symbol.toString();
    }

    @Override
    public String visitLiteral(Literal symbol, Void context) {
        return SymbolFormatter.format(symbol);
    }

    @Override
    public String visitParameter(Parameter symbol, Void context) {
        return SymbolFormatter.format(symbol);
    }

    @Override
    public String visitAggregation(Aggregation symbol, Void context) {
        String[] args = new String[symbol.inputs().size()];

        List<Symbol> arguments = symbol.inputs();
        for (int i = 0; i < arguments.size(); i++) {
            Symbol arg = arguments.get(i);
            args[i] = arg.accept(this, context);
        }
        return format("%s(%s)",
                symbol.functionIdent().name(),
                argJoiner.join(args));
    }

    @Override
    public String visitReference(Reference symbol, Void context) {
        StringBuilder builder = new StringBuilder();
        String schema = symbol.info().ident().tableIdent().schema();
        if (schema != null && !schema.equals(DocSchemaInfo.NAME)) {
            builder.append(symbol.info().ident().tableIdent().schema()).append(".");
        }
        return builder.append(symbol.info().ident().tableIdent().name())
                .append(".")
                .append(symbol.info().ident().columnIdent().sqlFqn()).toString();
    }
}
