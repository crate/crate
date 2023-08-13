/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.symbol;

import io.crate.analyze.SubscriptContext;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;
import io.crate.types.UndefinedType;

public class ObjectKeyFinder extends SymbolVisitor<Symbol, Boolean> {

    //private static final ObjectKeyFinder INSTANCE = new ObjectKeyFinder();
    private final SubscriptContext subscriptContext;

    /**
     * Returns true if the object key is found.
     */
    public boolean find(Symbol base, Symbol key) {
        return base.accept(this, key);
    }

    @Override
    public Boolean visitFunction(Function symbol, Symbol key) {
        if (symbol.valueType().id() != ObjectType.ID) {
            return false;
        }
        if (MapFunction.NAME.equals(symbol.name())) {
            return key.equals(symbol.arguments().get(0));
        }
        return false;
    }

    @Override
    public Boolean visitReference(Reference base, Symbol index) {
        var baseType = ArrayType.unnest(base.valueType());
        if (baseType instanceof ObjectType objectType) {
            return objectType.resolveInnerType(subscriptContext.parts()).id() != UndefinedType.ID;
        }
        return false;
    }

    @Override
    protected Boolean visitSymbol(Symbol symbol, Symbol key) {
        return false;
    }

    public ObjectKeyFinder(SubscriptContext context) {
        this.subscriptContext = context;
    }
}
