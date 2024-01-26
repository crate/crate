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

package io.crate.expression.scalar.bitwise;

import io.crate.common.TriConsumer;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.BinaryScalar;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;
import java.util.Locale;
import java.util.function.BinaryOperator;

import static io.crate.types.DataTypes.BYTE;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.SHORT;

public class BitwiseFunctions {

    private static final TriConsumer<String, BitString, BitString> LENGTH_VALIDATOR = (op, bs1, bs2) -> {
        if (bs1.length() != bs2.length()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot %s bit strings of different sizes", op));
        }
    };

    private static <T> void register(ScalarFunctionModule module,
                                     String name,
                                     DataType<T> type,
                                     BinaryOperator<T> operator) {
        TypeSignature typeSignature = type.getTypeSignature();
        Signature scalar = Signature.scalar(
                name.toLowerCase(Locale.ENGLISH),
                typeSignature,
                typeSignature,
                typeSignature
            )
            .withFeatures(Scalar.DETERMINISTIC_ONLY)
            .withFeature(Scalar.Feature.NULLABLE);
        module.register(scalar, (signature, boundSignature) -> new BinaryScalar<>(operator, signature, boundSignature, type));
    }

    public static void register(ScalarFunctionModule module) {
        register(module, "AND", LONG, (a, b) -> a & b);
        register(module, "AND", INTEGER, (a, b) -> a & b);
        register(module, "AND", SHORT, (a, b) -> (short) (a & b)); // Bitwise operations on short and byte types are auto-casted to int, need to cast back.
        register(module, "AND", BYTE, (a, b) -> (byte) (a & b));
        register(module, "AND", BitStringType.INSTANCE_ONE, (a, b) -> {
            LENGTH_VALIDATOR.accept("AND", a, b);
            a.bitSet().and(b.bitSet());
            return a;
        });

        register(module, "OR", LONG, (a, b) -> a | b);
        register(module, "OR", INTEGER, (a, b) -> a | b);
        register(module, "OR", SHORT, (a, b) -> (short) (a | b));
        register(module, "OR", BYTE, (a, b) -> (byte) (a | b));
        register(module, "OR", BitStringType.INSTANCE_ONE, (a, b) -> {
            LENGTH_VALIDATOR.accept("OR", a, b);
            a.bitSet().or(b.bitSet());
            return a;
        });

        register(module, "XOR", LONG, (a, b) -> a ^ b);
        register(module, "XOR", INTEGER, (a, b) -> a ^ b);
        register(module, "XOR", SHORT, (a, b) -> (short) (a ^ b));
        register(module, "XOR", BYTE, (a, b) -> (byte) (a ^ b));
        register(module, "XOR", BitStringType.INSTANCE_ONE, (a, b) -> {
            LENGTH_VALIDATOR.accept("XOR", a, b);
            a.bitSet().xor(b.bitSet());
            return a;
        });
    }
}
