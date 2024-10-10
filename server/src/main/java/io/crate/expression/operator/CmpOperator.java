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

import java.util.function.IntPredicate;

import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.EqQuery;
import io.crate.types.StorageSupport;

public final class CmpOperator extends Operator<Object> {

    private final IntPredicate isMatch;
    private final DataType<Object> type;

    @SuppressWarnings("unchecked")
    public CmpOperator(Signature signature, BoundSignature boundSignature, IntPredicate cmpResultIsMatch) {
        super(signature, boundSignature);
        this.type = (DataType<Object>) boundSignature.argTypes().get(0);
        this.isMatch = cmpResultIsMatch;
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        return isMatch.test(type.compare(left, right));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Query toQuery(String functionName, Reference ref, Object value) {
        StorageSupport<?> storageSupport = ref.valueType().storageSupport();
        if (storageSupport == null) {
            return null;
        }
        EqQuery eqQuery = storageSupport.eqQuery();
        String field = ref.storageIdent();
        return switch (functionName) {
            case GtOperator.NAME -> eqQuery.rangeQuery(
                field,
                value,
                null,
                false,
                false,
                ref.hasDocValues(),
                ref.indexType() != IndexType.NONE);
            case GteOperator.NAME -> eqQuery.rangeQuery(
                field,
                value,
                null,
                true,
                false,
                ref.hasDocValues(),
                ref.indexType() != IndexType.NONE);
            case LtOperator.NAME -> eqQuery.rangeQuery(
                field,
                null,
                value,
                false,
                false,
                ref.hasDocValues(),
                ref.indexType() != IndexType.NONE);
            case LteOperator.NAME -> eqQuery.rangeQuery(
                field,
                null,
                value,
                false,
                true,
                ref.hasDocValues(),
                ref.indexType() != IndexType.NONE);
            default -> throw new IllegalArgumentException(functionName + " is not a supported comparison operator");
        };
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) {
        return CmpOperator.toQuery(signature.getName().name(), ref, literal.value());
    }
}
