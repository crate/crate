/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.symbol;

import org.elasticsearch.common.lucene.BytesRefs;

public class StringValueSymbolVisitor extends SymbolVisitor<Void, String> {

    public static final StringValueSymbolVisitor INSTANCE = new StringValueSymbolVisitor();

    private StringValueSymbolVisitor() {
    }

    public String process(Symbol symbol) {
        return process(symbol, null);
    }

    @Override
    protected String visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException(
                SymbolFormatter.format("Unable to get string value from symbol: %s", symbol));
    }



    @Override
    public String visitLiteral(Literal symbol, Void context) {
        return BytesRefs.toString(symbol.value());
    }
}
