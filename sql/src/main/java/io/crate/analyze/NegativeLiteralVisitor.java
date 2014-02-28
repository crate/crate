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
package io.crate.analyze;

import io.crate.planner.symbol.*;

public class NegativeLiteralVisitor extends SymbolVisitor<Void, Literal> {

    @Override
    public Literal visitBooleanLiteral(BooleanLiteral symbol, Void context) {
        return new BooleanLiteral(!symbol.value());
    }

    @Override
    public Literal visitByteLiteral(ByteLiteral symbol, Void context) {
        return new ByteLiteral(symbol.value()* -1);
    }

    @Override
    public Literal visitShortLiteral(ShortLiteral symbol, Void context) {
        return new ShortLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteral symbol, Void context) {
        return new IntegerLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitLongLiteral(LongLiteral symbol, Void context) {
        return new LongLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitFloatLiteral(FloatLiteral symbol, Void context) {
        return new FloatLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitDoubleLiteral(DoubleLiteral symbol, Void context) {
        return new DoubleLiteral(symbol.value() * -1);
    }

    @Override
    protected Literal visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException(SymbolFormatter.format("Cannot negate symbol %s", symbol));
    }
}
