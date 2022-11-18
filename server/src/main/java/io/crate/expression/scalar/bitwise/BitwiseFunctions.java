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
import io.crate.expression.scalar.arithmetic.BinaryScalar;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BinaryOperator;

import static io.crate.types.DataTypes.BYTE;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.SHORT;

public class BitwiseFunctions {

    public record TypedOperator<T>(DataType<T> type, BinaryOperator<T> operator) {}

    private static final TriConsumer<String, BitString, BitString> LENGTH_VALIDATOR = (op, bs1, bs2) -> {
        if (bs1.length() != bs2.length()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot %s bit strings of different sizes", op));
        }
    };

    private enum Operations {
        AND(
            new TypedOperator<>(LONG, (a, b) -> a & b),
            new TypedOperator<>(INTEGER, (a, b) -> a & b),
            new TypedOperator<>(SHORT, (a, b) -> (short) (a & b)), // Bitwise operations on short and byte types are auto-casted to int, need to cast back.
            new TypedOperator<>(BYTE, (a, b) -> (byte) (a & b)),
            new TypedOperator<>(BitStringType.INSTANCE_ONE, (a, b) -> {
                LENGTH_VALIDATOR.accept("AND", a, b);
                a.bitSet().and(b.bitSet()); // Mutates 'a'
                return a;
            })
        ),
        OR(
            new TypedOperator<>(LONG, (a, b) -> a | b),
            new TypedOperator<>(INTEGER, (a, b) -> a | b),
            new TypedOperator<>(SHORT, (a, b) -> (short) (a | b)),
            new TypedOperator<>(BYTE, (a, b) -> (byte) (a | b)),
            new TypedOperator<>(BitStringType.INSTANCE_ONE, (a, b) -> {
                LENGTH_VALIDATOR.accept("OR", a, b);
                a.bitSet().or(b.bitSet());
                return a;
            })
        ),
        XOR(
            new TypedOperator<>(LONG, (a, b) -> a ^ b),
            new TypedOperator<>(INTEGER, (a, b) -> a ^ b),
            new TypedOperator<>(SHORT, (a, b) -> (short) (a ^ b)),
            new TypedOperator<>(BYTE, (a, b) -> (byte) (a ^ b)),
            new TypedOperator<>(BitStringType.INSTANCE_ONE, (a, b) -> {
                LENGTH_VALIDATOR.accept("XOR", a, b);
                a.bitSet().xor(b.bitSet());
                return a;
            })
        );

        private final List<TypedOperator> typedOperators = new ArrayList<>();

        Operations(TypedOperator<Long> longOperator,
                   TypedOperator<Integer> intOperator,
                   TypedOperator<Short> shortOperator,
                   TypedOperator<Byte> byteOperator,
                   TypedOperator<BitString> bitStringOperator) {
            typedOperators.add(longOperator);
            typedOperators.add(intOperator);
            typedOperators.add(shortOperator);
            typedOperators.add(byteOperator);
            typedOperators.add(bitStringOperator);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }

    public static void register(ScalarFunctionModule module) {
        for (var operation : Operations.values()) {
            for (TypedOperator typedOperator : operation.typedOperators) {
                TypeSignature typeSignature = typedOperator.type.getTypeSignature();
                module.register(
                    Signature.scalar(
                        operation.toString(),
                        typeSignature,
                        typeSignature,
                        typeSignature
                    ).withFeatures(Scalar.DETERMINISTIC_ONLY),
                    (signature, boundSignature) ->
                        new BinaryScalar<>(typedOperator.operator, signature, boundSignature, typedOperator.type)
                );
            }
        }
    }
}
