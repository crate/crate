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

import io.crate.metadata.Functions;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public class GreatestFunction extends ConditionalCompareFunction {

    private static final String NAME = "greatest";

    public static void register(Functions.Builder module) {
        module.add(
            Signature
                .scalar(
                    NAME,
                    Feature.CONDITIONAL,
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E"))
                .withFeature(Feature.DETERMINISTIC)
                .withVariableArity()
                .withTypeVariableConstraints(typeVariable("E")),
            GreatestFunction::new
        );
    }

    public GreatestFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public int compare(Object o1, Object o2) {
        DataType dataType = boundSignature().returnType();
        return dataType.compare(o2, o1);
    }
}
