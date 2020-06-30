/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
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

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;

class PolyglotValuesConverter {

    private static final TypeLiteral<Number> NUMBER_TYPE_LITERAL = new TypeLiteral<>() {};
    private static final TypeLiteral<Map> MAP_TYPE_LITERAL = new TypeLiteral<>() {};

    static Object toCrateObject(Value value, DataType<?> type) {
        if (value == null) {
            return null;
        }
        Function<Object, ?> convert = type::implicitCast;
        switch (type.id()) {
            case ArrayType.ID:
                ArrayList<Object> items = new ArrayList<>((int) value.getArraySize());
                for (int idx = 0; idx < value.getArraySize(); idx++) {
                    var item = toCrateObject(value.getArrayElement(idx), ((ArrayType<?>) type).innerType());
                    items.add(idx, item);
                }
                return convert.apply(items);
            case ObjectType.ID:
                return convert.apply(value.as(MAP_TYPE_LITERAL));
            case GeoPointType.ID:
                if (value.hasArrayElements()) {
                    return convert.apply(toCrateObject(value, DataTypes.DOUBLE_ARRAY));
                } else {
                    return convert.apply(value.asString());
                }
            case GeoShapeType.ID:
                if (value.isString()) {
                    return convert.apply(value.asString());
                } else {
                    return convert.apply(value.as(MAP_TYPE_LITERAL));
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
                return convert.apply(polyglotValue);
        }
    }

    static Object[] toPolyglotValues(Input<Object>[] inputs) {
        Object[] args = new Object[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            args[i] = Value.asValue(inputs[i].value());
        }
        return args;
    }
}
