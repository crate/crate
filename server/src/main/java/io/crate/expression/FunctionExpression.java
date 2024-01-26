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

package io.crate.expression;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.expression.scalar.Scalar;

import java.util.Arrays;

public final class FunctionExpression<ReturnType, InputType> implements Input<ReturnType> {

    private final Input<InputType>[] arguments;
    private final Scalar<ReturnType, InputType> scalar;
    private final TransactionContext txnCtx;
    private final NodeContext nodeCtx;

    public FunctionExpression(TransactionContext txnCtx,
                              NodeContext nodeCtx,
                              Scalar<ReturnType, InputType> scalar,
                              Input<InputType>[] arguments) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.scalar = scalar;
        this.arguments = arguments;
    }

    @Override
    public ReturnType value() {
        return scalar.evaluate(txnCtx, nodeCtx, arguments);
    }

    @Override
    public String toString() {
        return "FuncExpr{" +
               scalar.signature().getName() +
               ", args=" + Arrays.toString(arguments) + '}';
    }
}
