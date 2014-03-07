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

// PRESTOBORROW


import org.elasticsearch.common.Nullable;

public class SymbolVisitor<C, R> {

    public R process(Symbol symbol, @Nullable C context) {
        return symbol.accept(this, context);
    }

    protected R visitSymbol(Symbol symbol, C context) {
        return null;
    }

    public R visitAggregation(Aggregation symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitValue(Value symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitReference(Reference symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitDynamicReference(DynamicReference symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitFunction(Function symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitLiteral(Literal symbol, C context) {
        return visitSymbol(symbol, context);
    }

    public R visitStringLiteral(StringLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitDoubleLiteral(DoubleLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitBooleanLiteral(BooleanLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitIntegerLiteral(IntegerLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitInputColumn(InputColumn inputColumn, C context) {
        return visitSymbol(inputColumn, context);
    }

    public R visitNullLiteral(Null symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitLongLiteral(LongLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitFloatLiteral(FloatLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitSetLiteral(SetLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitArrayLiteral(ArrayLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitObjectLiteral(ObjectLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitTimestampLiteral(TimestampLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitByteLiteral(ByteLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitShortLiteral(ShortLiteral symbol, C context) {
        return visitLiteral(symbol, context);
    }

    public R visitParameter(Parameter symbol, C context) {
        return visitSymbol(symbol, context);
    }
}

