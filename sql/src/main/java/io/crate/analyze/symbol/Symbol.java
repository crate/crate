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

package io.crate.analyze.symbol;

import io.crate.planner.ExplainLeaf;
import io.crate.types.DataType;
import io.crate.types.UndefinedType;
import org.elasticsearch.common.io.stream.Writeable;

public abstract class Symbol implements FuncArg, Writeable, ExplainLeaf {

    public static boolean isLiteral(Symbol symbol, DataType expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL
               && symbol.valueType().equals(expectedType);
    }

    public abstract SymbolType symbolType();

    public abstract <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    public abstract DataType valueType();

    @Override
    public String toString() {
        return representation();
    }

    /**
     * Typically, we only allow casting of Literals and undefined types.
     */
    @Override
    public boolean canBeCasted() {
        return valueType().id() == UndefinedType.INSTANCE.id();
    }
}
