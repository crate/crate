/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import static io.crate.types.TypeSignature.parseTypeSignature;

public final class NegateFunctions {

    public static final String NAME = "_negate";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("double precision"),
                parseTypeSignature("double precision")
            )
                .withForbiddenCoercion(),
            (signature, argumentTypes) -> {
                DataType<?> dataType = argumentTypes.get(0);
                return new UnaryScalar<Double, Double>(NAME, signature, dataType, dataType, x -> x * -1);
            }
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("float"),
                parseTypeSignature("float")
            )
                .withForbiddenCoercion(),
            (signature, argumentTypes) -> {
                DataType<?> dataType = argumentTypes.get(0);
                return new UnaryScalar<Float, Float>(NAME, signature, dataType, dataType, x -> x * -1);
            }
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("integer"),
                parseTypeSignature("integer")
            )
                .withForbiddenCoercion(),
            (signature, argumentTypes) -> {
                DataType<?> dataType = argumentTypes.get(0);
                return new UnaryScalar<Integer, Integer>(NAME, signature, dataType, dataType, x -> x * -1);
            }
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("bigint"),
                parseTypeSignature("bigint")
            )
                .withForbiddenCoercion(),
            (signature, argumentTypes) -> {
                DataType<?> dataType = argumentTypes.get(0);
                return new UnaryScalar<Long, Long>(NAME, signature, dataType, dataType, x -> x * -1);
            }
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("smallint"),
                parseTypeSignature("smallint")
            )
                .withForbiddenCoercion(),
            (signature, argumentTypes) -> {
                DataType<?> dataType = argumentTypes.get(0);
                return new UnaryScalar<Short, Short>(NAME, signature, dataType, dataType, x -> (short) (x * -1));
            }
        );
    }
}
