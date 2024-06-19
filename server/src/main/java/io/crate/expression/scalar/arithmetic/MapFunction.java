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

package io.crate.expression.scalar.arithmetic;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;

import java.util.LinkedHashMap;
import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.TypeSignature;

/**
 * _map(k, v, [...]) -> object
 *
 * args must be a multiple of 2
 *
 * k: must be a string
 *
 * Note that keys will be returned as String while values of type String will be BytesRef
 */
public class MapFunction extends Scalar<Object, Object> {

    public static final String NAME = "_map";

    public static final Signature SIGNATURE =
        Signature.builder()
            .name(new FunctionName(null, NAME))
            .kind(FunctionType.SCALAR)
            .typeVariableConstraints(List.of(typeVariableOfAnyType("V")))
            .argumentTypes(TypeSignature.parse("text"), TypeSignature.parse("V"))
            // This is not 100% correct because each variadic `V` is type independent, resulting in a return type
            // of e.g. `object(text, int, text, geo_point, ...)`.
            // This is *ok* as the returnType is currently not used directly, only for function description.
            .returnType(TypeSignature.parse("object(text, V)"))
            .variableArityGroup(List.of(TypeSignature.parse("text"), TypeSignature.parse("V")))
            .feature(Feature.DETERMINISTIC)
            .build();


    public static void register(Functions.Builder module) {
        module.add(
            SIGNATURE,
            MapFunction::new
        );
    }

    private MapFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SafeVarargs
    @Override
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>(args.length / 2, 1.0f);
        for (int i = 0; i < args.length - 1; i += 2) {
            m.put((String) args[i].value(), args[i + 1].value());
        }
        return m;
    }
}
