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

package io.crate.expression.operator.any;

import io.crate.data.Input;
import io.crate.expression.operator.Operator;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import java.util.function.IntPredicate;

import static io.crate.expression.operator.any.AnyOperators.collectionValueToIterable;

public final class AnyOperator extends Operator<Object> {

    public static final String OPERATOR_PREFIX = "any_";

    private final Signature signature;
    private final Signature boundSignature;
    private final IntPredicate cmpIsMatch;
    private final DataType leftType;

    /**
     * @param cmpIsMatch predicate to test if a comparison (-1, 0, 1) should be considered a match
     */
    AnyOperator(Signature signature, Signature boundSignature, IntPredicate cmpIsMatch) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.cmpIsMatch = cmpIsMatch;
        this.leftType = boundSignature.getArgumentDataTypes().get(0);
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @SuppressWarnings("unchecked")
    private Boolean doEvaluate(Object left, Iterable<?> rightValues) {
        boolean anyNulls = false;
        for (Object rightValue : rightValues) {
            if (rightValue == null) {
                anyNulls = true;
                continue;
            }
            if (cmpIsMatch.test(leftType.compare(left, rightValue))) {
                return true;
            }
        }
        return anyNulls ? null : false;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null : "1st argument must not be null";

        Object item = args[0].value();
        Object items = args[1].value();
        if (items == null || item == null) {
            return null;
        }
        return doEvaluate(item, collectionValueToIterable(items));
    }
}
