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

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;
import static io.crate.types.TypeSignature.parseTypeSignature;

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
            .argumentTypes(parseTypeSignature("text"), parseTypeSignature("V"))
            // This is not 100% correct because each variadic `V` is type independent, resulting in a return type
            // of e.g. `object(text, int, text, geo_point, ...)`.
            // This is *ok* as the returnType is currently not used directly, only for function description.
            .returnType(parseTypeSignature("object(text, V)"))
            .variableArityGroup(List.of(parseTypeSignature("text"), parseTypeSignature("V")))
            .build();


    public static void register(ScalarFunctionModule module) {
        module.register(
            SIGNATURE,
            MapFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private MapFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @SafeVarargs
    @Override
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        Map<String, Object> m = new HashMap<>(args.length / 2, 1.0f);
        for (int i = 0; i < args.length - 1; i += 2) {
            m.put((String) args[i].value(), args[i + 1].value());
        }
        return m;
    }
}
