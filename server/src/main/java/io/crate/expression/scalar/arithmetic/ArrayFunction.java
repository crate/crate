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

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;

import java.util.ArrayList;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class ArrayFunction extends Scalar<Object, Object> {

    public static final String NAME = "_array";
    public static final Signature SIGNATURE =
        Signature.builder()
            .name(NAME)
            .kind(FunctionType.SCALAR)
            .typeVariableConstraints(typeVariable("E"))
            .argumentTypes(parseTypeSignature("E"))
            .returnType(parseTypeSignature("array(E)"))
            .setVariableArity(true)
            .build();


    public static void register(ScalarFunctionModule module) {
        module.register(
            SIGNATURE,
            ArrayFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private ArrayFunction(Signature signature, Signature boundSignature) {
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
        ArrayList<Object> values = new ArrayList<>(args.length);
        for (Input<Object> arg : args) {
            values.add(arg.value());
        }
        return values;
    }
}
