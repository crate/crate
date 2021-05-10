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
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.function.BiFunction;

import static io.crate.execution.engine.window.WindowFrameState.isLowerBoundIncreasing;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class NthValueFunctions implements WindowFunction {

    public static final String LAST_VALUE_NAME = "last_value";
    private static final String FIRST_VALUE_NAME = "first_value";
    private static final String NTH_VALUE = "nth_value";

    public static void register(ExtraFunctionsModule module) {
        module.register(
            Signature.window(
                FIRST_VALUE_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    (frame, inputs) -> 0
                )
        );

        module.register(
            Signature.window(
                LAST_VALUE_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    (frame, inputs) -> frame.size() - 1
                )
        );

        module.register(
            Signature.window(
                NTH_VALUE,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    (frame, inputs) -> {
                        Number position = (Number) inputs[1].value();
                        if (position == null) {
                            // treating a null position as an out-of-bounds position
                            return -1;
                        }
                        return position.intValue() - 1;
                    }
                )
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final BiFunction<WindowFrameState, Input[], Integer> frameIndexSupplier;
    private int seenFrameLowerBound = -1;
    private int seenFrameUpperBound = -1;
    private Object resultForCurrentFrame = null;

    private NthValueFunctions(Signature signature,
                              Signature boundSignature,
                              BiFunction<WindowFrameState, Input[], Integer> frameIndexSupplier) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.frameIndexSupplier = frameIndexSupplier;
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
        boolean shrinkingWindow = isLowerBoundIncreasing(currentFrame, seenFrameLowerBound);
        if (idxInPartition == 0 || currentFrame.upperBoundExclusive() > seenFrameUpperBound || shrinkingWindow) {
            seenFrameLowerBound = currentFrame.lowerBound();
            seenFrameUpperBound = currentFrame.upperBoundExclusive();

            int index = frameIndexSupplier.apply(currentFrame, args);
            if (shrinkingWindow) {
                // consecutive shrinking frames (lower bound increments) will can have the following format :
                //         frame 1: 1 2 3 with lower bound 0
                //          frame 2:   2 3 with lower bound 1
                // We represent the frames as a view over the rows in a partition (for frame 2 the element "1" is not
                // present by virtue of the frame's lower bound being 1 and "hiding"/excluding it)
                // If we want the 2nd value (index = 1) in every frame we have to request the index _after_  the frame's
                // lower bound (in our example, to get the 2nd value in the second frame, namely "3", the requested
                // index needs to be 2)
                index = currentFrame.lowerBound() + index;
            }

            Object[] nthRowCells = currentFrame.getRowInFrameAtIndexOrNull(index);
            if (nthRowCells == null) {
                resultForCurrentFrame = null;
                return null;
            }

            Row nthRowInFrame = new RowN(nthRowCells);
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(nthRowInFrame);
            }

            resultForCurrentFrame = args[0].value();
        }

        return resultForCurrentFrame;
    }
}
