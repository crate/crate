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

import java.util.List;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.TypeSignature;

public class KnnMatch extends Scalar<Boolean, Object> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("knn_match", FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse(FloatVectorType.NAME), TypeSignature.parse(FloatVectorType.NAME), DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            KnnMatch::new
        );
    }

    public KnnMatch(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx,
                                  NodeContext nodeContext,
                                  Input<Object>... args) {
        throw new UnsupportedOperationException("knn_match can only be used in WHERE clause for tables as it needs an index to operate on");
    }

    @Override
    @Nullable
    public Query toQuery(Function function, Context context) {
        List<Symbol> args = function.arguments();
        if (args.get(0) instanceof Reference ref
                && args.get(1) instanceof Literal<?> targetLiteral
                && args.get(2) instanceof Literal<?> kLiteral) {

            Object target = targetLiteral.value();
            Object k = kLiteral.value();
            if (target instanceof float[] && k instanceof Integer) {
                return new KnnFloatVectorQuery(ref.storageIdent(), (float[]) target, (int) k);
            }
            return null;
        }
        return null;
    }

}
