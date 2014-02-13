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

public enum SymbolType {

    AGGREGATION(Aggregation.FACTORY),
    REFERENCE(Reference.FACTORY),
    VALUE(Value.FACTORY),
    FUNCTION(Function.FACTORY),
    STRING_LITERAL(StringLiteral.FACTORY),
    DOUBLE_LITERAL(DoubleLiteral.FACTORY),
    FLOAT_LITERAL(FloatLiteral.FACTORY),
    BOOLEAN_LITERAL(BooleanLiteral.FACTORY),
    INTEGER_LITERAL(IntegerLiteral.FACTORY),
    LONG_LITERAL(LongLiteral.FACTORY),
    NULL_LITERAL(Null.FACTORY),
    INPUT_COLUMN(InputColumn.FACTORY),
    SET_LITERAL(SetLiteral.FACTORY),
    ;

    private final Symbol.SymbolFactory factory;

    SymbolType(Symbol.SymbolFactory factory) {
        this.factory = factory;
    }

    public Symbol newInstance() {
        return factory.newInstance();
    }

    public boolean isLiteral() {
        return ordinal() > FUNCTION.ordinal() && ordinal() < INPUT_COLUMN.ordinal();
    }

}
