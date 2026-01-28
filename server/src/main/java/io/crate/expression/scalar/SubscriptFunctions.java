/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import io.crate.types.RowType;

public final class SubscriptFunctions {

    public static Function makeObjectSubscript(Symbol base, List<String> path) {
        assert base.valueType().id() == ObjectType.ID
            : "makeObjectSubscript only works on base symbols of type `object`, got `" + base.valueType().getName() + '`';
        List<Symbol> arguments = Lists.mapTail(base, path, Literal::of);
        DataType<?> returnType = ((ObjectType) base.valueType()).innerType(path);
        return new Function(
            SubscriptObjectFunction.SIGNATURE,
            arguments,
            returnType
        );
    }

    public static Function makeObjectSubscript(Symbol base, ColumnIdent column) {
        return makeObjectSubscript(base, column.path());
    }

    @Nullable
    public static Function tryCreateSubscript(Symbol baseSymbol, List<String> path) {
        assert !path.isEmpty() : "Path must not be empty to create subscript function";

        var baseType = baseSymbol.valueType();
        switch (baseType.id()) {
            case ObjectType.ID: {
                List<Symbol> arguments = Lists.mapTail(baseSymbol, path, Literal::of);
                DataType<?> returnType = ((ObjectType) baseType).innerType(path);
                return new Function(
                    SubscriptObjectFunction.SIGNATURE,
                    arguments,
                    returnType
                );
            }

            case RowType.ID: {
                String child = path.get(0);
                RowType rowType = (RowType) baseType;
                int idx = rowType.fieldNames().indexOf(child);
                if (idx < 0) {
                    return null;
                }
                Function recordSubscript = new Function(
                    SubscriptRecordFunction.SIGNATURE,
                    List.of(baseSymbol, Literal.of(child)),
                    rowType.getFieldType(idx)
                );
                if (path.size() > 1) {
                    return tryCreateSubscript(recordSubscript, path.subList(1, path.size()));
                }
                return recordSubscript;
            }

            case ArrayType.ID:
                DataType<?> innerType = ArrayType.unnest(baseType);
                assert innerType instanceof ObjectType
                    : "SubscriptFunctions.tryCreateSubscript only supports array of objects for nested object subscript, got `"
                      + innerType.getName() + '`';
                String child = path.get(0);
                DataType<?> returnType = ((ObjectType) innerType).innerType(child);

                // The arrayOfObjects subscript function only supports 1 level of child access,
                // so we need to wrap multiple path elements in multiple function calls.
                Function subscript = new Function(
                    SubscriptFunction.SIGNATURE_ARRAY_OF_OBJECTS,
                    List.of(baseSymbol, Literal.of(child)),
                    new ArrayType<>(returnType)
                );

                for (int i = 1; i < path.size(); i++) {
                    Function innerSubscript = tryCreateSubscript(subscript, List.of(path.get(i)));
                    if (innerSubscript == null) {
                        return null;
                    }
                    subscript = innerSubscript;
                }

                return subscript;

            default:
                return null;
        }
    }
}
