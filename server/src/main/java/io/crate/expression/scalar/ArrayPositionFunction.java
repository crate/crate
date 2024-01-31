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
import java.util.Objects;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class ArrayPositionFunction extends Scalar<Integer, List<Object>> {

    public static final String NAME = "array_position";

    public ArrayPositionFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(
            Signature.scalar(NAME,
                    TypeSignature.parse("array(T)"),
                    TypeSignature.parse("T"),
                    DataTypes.INTEGER.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("T"))
                .withFeature(Feature.NULLABLE),
            ArrayPositionFunction::new);

        scalarFunctionModule.register(
            Signature.scalar(NAME,
                    TypeSignature.parse("array(T)"),
                    TypeSignature.parse("T"),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("T"))
                .withFeature(Feature.NULLABLE),
            ArrayPositionFunction::new);
    }

    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input[] args) {

        List<Object> elements = (List<Object>) args[0].value();
        if (elements == null || elements.isEmpty()) {
            return null;
        }

        Object targetValue = args[1].value();

        //Start iteration with 0 if optional parameter not supplied
        Integer beginIndex = 0;
        if (args.length > 2) {
            beginIndex = getBeginPosition(args[2].value(), elements.size());
        }

        if (beginIndex == null) {
            return null;
        }

        Object element = null;
        for (int i = beginIndex; i < elements.size(); i++) {
            element = elements.get(i);
            if (Objects.equals(targetValue, element)) {
                return i + 1; //Return position.
            }
        }

        return null;
    }

    private Integer getBeginPosition(Object position, int elementsSize) {

        if (position == null) {
            return 0;
        }

        Integer beginPosition = (Integer) position;
        if (beginPosition < 1 || beginPosition > elementsSize) {
            return null;
        }

        return beginPosition - 1;
    }
}
