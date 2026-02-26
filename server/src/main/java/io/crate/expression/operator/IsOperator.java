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

package io.crate.expression.operator;

import org.apache.lucene.search.Query;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;
import io.crate.types.StorageSupport;

public class IsOperator extends Scalar<Boolean, Object> {

    public static final String NAME = "op_is";

    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(DataTypes.BOOLEAN.getTypeSignature(), DataTypes.BOOLEAN.getTypeSignature())
        .returnType(DataTypes.BOOLEAN.getTypeSignature())
        .features(Feature.DETERMINISTIC, Feature.NOTNULL)
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, IsOperator::new);
    }

    private IsOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 2 : "number of args must be 2";
        Object left = args[0].value();
        Object right = args[1].value();

        if (right == null) {
            return left == null;
        }

        if (left == null) {
            return false;
        }
        return left.equals(right);
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) {
        Object value = literal.value();

        assert value != null : "IS operator always has TRUE/FALSE literal values due to parser/expression analyzer";

        return fromPrimitive(
            ref.valueType(),
            ref.storageIdent(),
            value,
            ref.hasDocValues(),
            ref.indexType()
        );
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private Query fromPrimitive(DataType<?> type, String column, Object value, boolean hasDocValues, IndexType indexType) {
        StorageSupport<?> storageSupport = type.storageSupport();
        EqQuery<?> eqQuery = storageSupport == null ? null : storageSupport.eqQuery();
        if (eqQuery == null) {
            return null;
        }
        return ((EqQuery<Object>) eqQuery).termQuery(column, value, hasDocValues, indexType != IndexType.NONE);
    }
}
