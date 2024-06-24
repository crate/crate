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

package io.crate.expression.scalar.conditional;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

/**
 * Conditional If/Else function, CASE expressions can be converted to chain of {@link IfFunction}s.
 * It takes at most 3 arguments: condition, result, default.
 * The 3rd argument is optional. If left out, default result will be NULL.
 *
 * <pre>
 *
 *      A CASE expression like this:
 *
 *      CASE WHEN id = 0 THEN 'zero' WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END
 *
 *      can result in:
 *
 *      if(id = 0, 'zero', if(id % 2 = 0, 'even', 'odd'))
 *
 * </pre>
 */
public class IfFunction extends Scalar<Object, Object> {

    public static void register(Functions.Builder module) {
        // if (condition, result)
        module.add(
            Signature.scalar(
                    NAME,
                    DataTypes.BOOLEAN.getTypeSignature(),
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E")
                ).withFeature(Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            IfFunction::new
        );
        // if (condition, result, default)
        module.add(
            Signature.scalar(
                    NAME,
                    DataTypes.BOOLEAN.getTypeSignature(),
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E")
                ).withFeature(Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            IfFunction::new
        );
    }

    public static final String NAME = "if";


    private IfFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        Boolean condition = (Boolean) args[0].value();
        if (condition != null && condition) {
            return args[1].value();
        }
        if (args.length == 3) {
            return args[2].value();
        }

        return null;
    }


}
