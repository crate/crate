/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.window;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.functions.Signature;
import io.crate.module.EnterpriseFunctionsModule;
import io.crate.types.DataTypes;

import java.util.List;


public class RankFunctions implements WindowFunction {

    private static final String RANK_NAME = "rank";

    private final Signature signature;
    private final Signature boundSignature;
    private int seenLastUpperBound;
    private int rank;

    private RankFunctions(Signature signature, Signature boundSignature) {
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
            rank = seenLastUpperBound + 1;
            seenLastUpperBound = currentFrame.upperBoundExclusive();
        }

        return rank;

    }

    public static void register(EnterpriseFunctionsModule module) {
        module.register(
            Signature.window(
                RANK_NAME,
                DataTypes.INTEGER.getTypeSignature()
                ),
            RankFunctions::new
        );
    }
}
