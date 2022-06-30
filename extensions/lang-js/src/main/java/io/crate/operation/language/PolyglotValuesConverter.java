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

package io.crate.operation.language;

import io.crate.data.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import org.graalvm.polyglot.TypeLiteral;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class PolyglotValuesConverter {

    private static final TypeLiteral<Number> NUMBER_TYPE_LITERAL = new TypeLiteral<>() {
    };
    private static final TypeLiteral<Map> MAP_TYPE_LITERAL = new TypeLiteral<>() {
    };

    static Object toCrateObject(Value value, DataType<?> type) {
        if (value == null) {
            return null;
        }
        switch (type.id()) {
            case ArrayType.ID:
                ArrayList<Object> items = new ArrayList<>((int) value.getArraySize());
                for (int idx = 0; idx < value.getArraySize(); idx++) {
                    var item = toCrateObject(value.getArrayElement(idx), ((ArrayType<?>) type).innerType());
                    items.add(idx, item);
                }
                return type.implicitCast(items);
            case ObjectType.ID:
                return type.implicitCast(value.as(MAP_TYPE_LITERAL));
            case GeoPointType.ID:
                if (value.hasArrayElements()) {
                    return type.implicitCast(toCrateObject(value, DataTypes.DOUBLE_ARRAY));
                } else {
                    return type.implicitCast(value.asString());
                }
            case GeoShapeType.ID:
                if (value.isString()) {
                    return type.implicitCast(value.asString());
                } else {
                    return type.implicitCast(value.as(MAP_TYPE_LITERAL));
                }
            default:
                final Object polyglotValue;
                if (value.isNumber()) {
                    polyglotValue = value.as(NUMBER_TYPE_LITERAL);
                } else if (value.isString()) {
                    polyglotValue = value.asString();
                } else if (value.isBoolean()) {
                    polyglotValue = value.asBoolean();
                } else {
                    polyglotValue = value.asString();
                }
                return type.implicitCast(polyglotValue);
        }
    }

    static Object[] toPolyglotValues(Input<Object>[] inputs, List<DataType<?>> dataTypes) {
        Object[] args = new Object[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            switch (dataTypes.get(i).id()) {
                case ObjectType.ID, GeoShapeType.ID ->
                    //noinspection unchecked
                    args[i] = ProxyObject.fromMap((Map<String, Object>) inputs[i].value());
                default -> args[i] = Value.asValue(inputs[i].value());
            }
        }
        return args;
    }
}
