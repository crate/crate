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

package io.crate.window;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.function.IntBinaryOperator;


public class RankFunctions implements WindowFunction {

    private static final String RANK_NAME = "rank";
    private static final String DENSE_RANK_NAME = "dense_rank";

    private final Signature signature;
    private final Signature boundSignature;
    private int seenLastUpperBound = -1;
    private int rank;
    private final IntBinaryOperator rankIncrementor;

    private RankFunctions(Signature signature, Signature boundSignature, IntBinaryOperator rankIncrementor) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.rankIncrementor = rankIncrementor;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Object execute(int idxInPartition,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          Input... args) {
        if (idxInPartition == 0) {
            rank = 1;
            seenLastUpperBound = currentFrame.upperBoundExclusive();
        }

        if (currentFrame.upperBoundExclusive() != seenLastUpperBound) {
            rank = rankIncrementor.applyAsInt(rank, seenLastUpperBound);
            seenLastUpperBound = currentFrame.upperBoundExclusive();
        }

        return rank;

    }

    public static void register(ExtraFunctionsModule module) {
        module.register(
            Signature.window(
                RANK_NAME,
                DataTypes.INTEGER.getTypeSignature()
                ),
            (signature, boundSignature) ->
                new RankFunctions(
                    signature,
                    boundSignature,
                    (rank, upperBound) -> upperBound + 1
                )
        );

        module.register(
            Signature.window(
                DENSE_RANK_NAME,
                DataTypes.INTEGER.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new RankFunctions(
                    signature,
                    boundSignature,
                    (rank, upperBound) -> rank + 1
                )
        );
    }
}
