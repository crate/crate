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

package io.crate.expression.operator;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public final class BitwiseAndOperator extends Operator<Object> {

    private final Signature signature;
    private final Signature boundSignature;

    public static final String NAME = "op_&";

    public BitwiseAndOperator(Signature signature, Signature boundSignature) {
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

    public static void register(OperatorModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ), BitwiseAndOperator::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.LONG.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ), BitwiseAndOperator::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.SHORT.getTypeSignature(),
                DataTypes.SHORT.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ), BitwiseAndOperator::new
        );
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        long leftValue, rightValue;
        if (left instanceof Long) {
            leftValue = (Long) left;
            rightValue = (Long) right;
        } else if (left instanceof Integer) {
            leftValue = ((Integer) left).longValue();
            rightValue = ((Integer) right).longValue();
        } else if (left instanceof Short) {
            leftValue = ((Short) left).longValue();
            rightValue = ((Short) right).longValue();
        } else {
            return null;
        }

        return (leftValue & rightValue) == leftValue;
    }
}
