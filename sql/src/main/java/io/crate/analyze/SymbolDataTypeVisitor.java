/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.cratedb.DataType;

public class SymbolDataTypeVisitor extends SymbolVisitor<Analysis, DataType> {

    @Override
    public DataType visitBooleanLiteral(BooleanLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitDoubleLiteral(DoubleLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitFloatLiteral(FloatLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitFunction(Function symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitIntegerLiteral(IntegerLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitLongLiteral(LongLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitNullLiteral(Null symbol, Analysis context) {
        return DataType.NULL;
    }

    @Override
    public DataType visitReference(Reference symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitStringLiteral(StringLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitObjectLiteral(ObjectLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitTimestampLiteral(TimestampLiteral symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitValue(Value symbol, Analysis context) {
        return symbol.valueType();
    }

    @Override
    protected DataType visitSymbol(Symbol symbol, Analysis context) {
        throw new UnsupportedOperationException("Unsupported symbol type " + symbol.symbolType().toString());
    }
}
