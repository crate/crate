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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.graalvm.polyglot.TypeLiteral;
import org.graalvm.polyglot.Value;

import io.crate.data.Input;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;

class PolyglotValues {

    private static final TypeLiteral<Number> NUMBER_TYPE_LITERAL = new TypeLiteral<>() {
    };
    private static final TypeLiteral<Object> OBJECT_LITERAL = new TypeLiteral<>() {
    };

    private PolyglotValues() {
    }

    private static Object toCrateObject(Value value) {
        if (value.isNull()) {
            return null;
        }
        if (value.isHostObject()) {
            return value.asHostObject();
        }
        if (value.hasIterator()) {
            ArrayList<Object> items = new ArrayList<>();
            Value iterator = value.getIterator();
            while (iterator.hasIteratorNextElement()) {
                items.add(toCrateObject(iterator.getIteratorNextElement()));
            }
            return items;
        }
        if (value.hasMembers()) {
            Set<String> memberKeys = value.getMemberKeys();
            HashMap<String, Object> result = HashMap.newHashMap(memberKeys.size());
            for (String key : memberKeys) {
                result.put(key, toCrateObject(value.getMember(key)));
            }
            return result;
        }
        return value.as(OBJECT_LITERAL);
    }

    static Object toCrateObject(Value value, DataType<?> type) {
        // function needs to do deep copies because the proxy objects graalvm uses
        // are not safe for concurrent read access
        if (value == null) {
            return null;
        }
        if (value.isHostObject()) {
            return type.implicitCast(value.asHostObject());
        }
        return switch (type) {
            case ArrayType<?> arrayType -> {
                DataType<?> innerType = arrayType.innerType();
                int size = (int) value.getArraySize();
                ArrayList<Object> items = new ArrayList<>(size);
                for (int idx = 0; idx < size; idx++) {
                    var item = toCrateObject(value.getArrayElement(idx), innerType);
                    items.add(idx, item);
                }
                yield type.implicitCast(items);
            }
            case ObjectType objectType -> {
                Set<String> memberKeys = value.getMemberKeys();
                HashMap<String, Object> shape = HashMap.newHashMap(memberKeys.size());
                for (String key : memberKeys) {
                    shape.put(key, toCrateObject(value.getMember(key)));
                }
                yield type.implicitCast(shape);
            }
            case GeoPointType geoPointType -> {
                if (value.hasArrayElements()) {
                    yield type.implicitCast(toCrateObject(value, DataTypes.DOUBLE_ARRAY));
                } else {
                    yield type.implicitCast(value.asString());
                }
            }
            case GeoShapeType geoShapeType -> {
                if (value.isString()) {
                    yield type.implicitCast(value.asString());
                }
                Set<String> memberKeys = value.getMemberKeys();
                HashMap<String, Object> shape = HashMap.newHashMap(memberKeys.size());
                for (String key : memberKeys) {
                    shape.put(key, toCrateObject(value.getMember(key)));
                }
                yield type.implicitCast(shape);
            }
            case FloatVectorType floatVectorType -> {
                long size = value.getArraySize();
                assert size == floatVectorType.characterMaximumLength()
                    : "Length of array returned from UDF must match float vector type dimensions";
                float[] result = new float[floatVectorType.characterMaximumLength()];
                for (int i = 0; i < size; i++) {
                    result[i] = value.getArrayElement(i).as(NUMBER_TYPE_LITERAL).floatValue();
                }
                yield result;
            }
            case BitStringType bitStringType -> bitStringType.implicitCast(value.asString());
            default -> {
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
                yield type.implicitCast(polyglotValue);
            }
        };
    }

    static Value[] toPolyglotValues(Input<Object>[] inputs) {
        Value[] result = new Value[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            result[i] = Value.asValue(inputs[i].value());
        }
        return result;
    }
}
