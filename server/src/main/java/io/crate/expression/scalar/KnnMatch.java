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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.TypeSignature;

public class KnnMatch extends Scalar<Boolean, Object> {

    private static final float DEFAULT_MIN_SCORE = 0.50f;

    private DataType<float[]> type;

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                "knn_match",
                TypeSignature.parse(FloatVectorType.NAME),
                TypeSignature.parse(FloatVectorType.NAME),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature()
            ),
            KnnMatch::new
        );
    }

    @SuppressWarnings("unchecked")
    public KnnMatch(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.type = (DataType<float[]>) boundSignature.argTypes().get(0);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx,
                                  NodeContext nodeContext,
                                  Input<Object>... args) {
        Object field = args[0].value();
        if (field == null) {
            return null;
        }
        Object target = args[1].value();
        if (target == null) {
            return null;
        }
        Object k = args[2].value();
        if (k == null) {
            return null;
        }
        assert field instanceof float[] : "First parameter value must be a float array";
        assert target instanceof float[] : "Second parameter value must be a float array";
        assert k instanceof Integer : "Third parameter value must be an integer";

        try {
            return evaluate((float[]) field, (float[]) target, (int) k);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean evaluate(float[] field, float[] target, int k) throws IOException {
        if (field.length != target.length) {
            throw new IllegalArgumentException("Field dimensions must match target dimensions for knn_query");
        }
        if (field.length != type.characterMaximumLength()) {
            throw new IllegalArgumentException("Field dimensions must match type dimensions for knn_query");
        }
        return FloatVectorType.SIMILARITY_FUNC.compare(field, target) >= DEFAULT_MIN_SCORE;
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
                return new KnnFloatVectorQuery(ref.column().fqn(), (float[]) target, (int) k);
            }
            return null;
        }
        return null;
    }

}
