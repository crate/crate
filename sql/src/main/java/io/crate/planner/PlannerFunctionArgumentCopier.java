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

package io.crate.planner;

import io.crate.planner.symbol.*;

import java.io.IOException;
import java.util.List;

/**
 * Visitor which deepcopies functions if their arguments are references,
 * used in {@linkplain io.crate.planner.Planner}.
 */
public class PlannerFunctionArgumentCopier extends SymbolVisitor<Void, Symbol> {

    /**
     * process given symbol and return possibly changed one
     * @param symbol
     * @return
     */
    public Symbol process(Symbol symbol) {
        return symbol.accept(this, null);
    }

    /**
     * process list of Symbols and change in place
     */
    public void process(List<Symbol> symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            symbols.set(i, process(symbols.get(i), null));
        }
    }

    @Override
    public Symbol visitFunction(Function symbol, Void context) {
        boolean needsCopy = false;
        List<Symbol> arguments = symbol.arguments();
        for (int i = 0; i < arguments.size(); i++) {
            Symbol argument = arguments.get(i);
            arguments.set(i, process(argument, null));
            if (argument.symbolType() == SymbolType.REFERENCE) {
                needsCopy = true;
            }
        }
        try {
            return needsCopy ? symbol.deepCopy() : symbol;
        } catch (IOException e) {
            throw new RuntimeException(SymbolFormatter.format("Error copying %s", symbol), e);
        }
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        return symbol;
    }
}
