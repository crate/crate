/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.conditional;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

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
 * The CaseFunction evaluates a case statement.
 * <p>
 * The type signature is:
 * </p>
 * <pre>
 * case(true, default T, condition Boolean, value1 T ...) -> T
 * </pre>
 *  The following sql case example:
 *<pre>
 * case
 *  when a <= 10 then 0
 *  when a >= 100 then 1
 *  else 2
 * end
 *</pre>
 * will result in the following argument:
 *<pre>
 *(true, 2, (a <= 10), 0, (a >= 100), 1)
 *</pre>
 *
 *
 **/

public class CaseFunction extends Scalar<Object, Object> {

    public static final String NAME = "case";

    public static void register(Functions.Builder module) {
        TypeSignature t = TypeSignature.parse("T");
        TypeSignature bool = TypeSignature.parse("boolean");
        module.add(
            Signature.builder(new FunctionName(null, NAME), FunctionType.SCALAR)
                .typeVariableConstraints(typeVariable("T"))
                .argumentTypes(bool, t)
                .returnType(t)
                .variableArityGroup(List.of(bool, t))
                .feature(Feature.DETERMINISTIC)
                .build(),
            CaseFunction::new
        );
    }

    private CaseFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        // args structure is [true, default T, condition1 Boolean, value T, condition2 Boolean, value T ...]
        // Therefore skip first pair which represents the default value
        for (int i = 2; i < args.length; i = i + 2) {
            var condition = (Boolean) args[i].value();
            if (condition != null && condition) {
                return args[i + 1].value();
            }
        }
        // Fallback to default value which is the first pair
        return args[1].value();
    }

}
