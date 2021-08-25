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
import io.crate.expression.operator.TriPredicate;
import io.crate.expression.operator.LikeOperators.CaseSensitivity;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import static io.crate.expression.operator.any.AnyOperators.collectionValueToIterable;

public class AnyLikeOperator extends Operator<Object> {

    private final Signature signature;
    private final Signature boundSignature;
    private final TriPredicate<String, String, CaseSensitivity> matcher;
    private final CaseSensitivity caseSensitivity;

    public AnyLikeOperator(Signature signature,
                           Signature boundSignature,
                           TriPredicate<String, String, CaseSensitivity> matcher,
                           CaseSensitivity caseSensitivity) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.matcher = matcher;
        this.caseSensitivity = caseSensitivity;
        DataType<?> innerType = ((ArrayType<?>) boundSignature.getArgumentDataTypes().get(1)).innerType();
        if (innerType.id() == ObjectType.ID) {
            throw new IllegalArgumentException("ANY on object arrays is not supported");
        }
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    private Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        String pattern = (String) left;
        boolean hasNull = false;
        for (Object elem : rightIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert elem instanceof String : "elem must be a String";
            String elemValue = (String) elem;
            if (matcher.test(elemValue, pattern, caseSensitivity)) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        Object value = args[0].value();
        Object collectionReference = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        return doEvaluate(value, collectionValueToIterable(collectionReference));
    }
}
