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

package io.crate.expression.scalar.string;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.types.DataTypes;

public final class EndsWithFunction extends Scalar<Boolean, String> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("ends_with", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            EndsWithFunction::new
        );
    }

    public EndsWithFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
        assert args.length == 2 : "ends_with takes exactly two arguments";
        var text = args[0].value();
        var suffix = args[1].value();
        if (text == null || suffix == null) {
            return null;
        }

        return text.endsWith(suffix);
    }
}
