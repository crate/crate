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

package io.crate.expression.scalar;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.List;
import java.util.StringJoiner;

import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

class ArrayToStringFunction extends Scalar<String, Object> {

    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, "array_to_string");

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(FQN, FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("array(E)"),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .typeVariableConstraints(typeVariable("E"))
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            ArrayToStringFunction::new
        );
        module.add(
            Signature.builder(FQN, FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("array(E)"),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .typeVariableConstraints(typeVariable("E"))
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            ArrayToStringFunction::new
        );
    }

    private ArrayToStringFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), FQN.name());
    }

    @Override
    @SafeVarargs
    public final String evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) args[0].value();
        if (values == null) {
            return null;
        }
        String separator = (String) args[1].value();
        if (separator == null) {
            return null;
        }

        String nullString = null;
        if (args.length == 3) {
            nullString = (String) args[2].value();
        }

        StringJoiner joiner = new StringJoiner(separator);
        for (Object value : values) {
            if (value != null) {
                joiner.add(DataTypes.STRING.implicitCast(value));
            } else if (nullString != null) {
                joiner.add(nullString);
            }
        }

        return joiner.toString();
    }
}
