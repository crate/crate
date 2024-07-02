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

import static io.crate.execution.engine.window.WindowFrameState.isLowerBoundIncreasing;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.List;
import java.util.function.LongConsumer;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class NthValueFunctions implements WindowFunction {

    private enum Implementation {
        FIRST_VALUE {
            @Override
            public Object execute(int adjustForShrinkingWindow,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  boolean ignoreNulls,
                                  Input<?> ... args) {
                if (ignoreNulls) {
                    for (int i = adjustForShrinkingWindow; i < currentFrame.upperBoundExclusive(); i++) {
                        Object[] nthRowCells = currentFrame.getRowInFrameAtIndexOrNull(i);
                        if (nthRowCells != null) {
                            Object value = extractValueFromRow(nthRowCells, expressions, args);
                            if (value != null) {
                                return value;
                            }
                        }
                    }
                } else {
                    return extractValueAtIndex(adjustForShrinkingWindow,
                                               currentFrame,
                                               expressions,
                                               args);
                }
                return null;
            }
        },
        LAST_VALUE {
            @Override
            public Object execute(int adjustForShrinkingWindow,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  boolean ignoreNulls,
                                  Input<?> ... args) {
                if (ignoreNulls) {
                    for (int i = adjustForShrinkingWindow + currentFrame.size() - 1; i >= 0; i--) {
                        Object[] nthRowCells = currentFrame.getRowInFrameAtIndexOrNull(i);
                        if (nthRowCells != null) {
                            Object value = extractValueFromRow(nthRowCells, expressions, args);
                            if (value != null) {
                                return value;
                            }
                        }
                    }
                } else {
                    return extractValueAtIndex(adjustForShrinkingWindow + currentFrame.size() - 1,
                                               currentFrame,
                                               expressions,
                                               args);
                }
                return null;
            }
        },
        NTH_VALUE {
            @Override
            public Object execute(int adjustForShrinkingWindow,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  boolean ignoreNulls,
                                  Input<?> ... args) {
                Number position = (Number) args[1].value();
                if (position == null) {
                    return null;
                }
                int iPosition = position.intValue();
                if (ignoreNulls) {
                    for (int i = adjustForShrinkingWindow, counter = 0; i < currentFrame.upperBoundExclusive(); i++) {
                        Object[] nthRowCells = currentFrame.getRowInFrameAtIndexOrNull(i);
                        if (nthRowCells != null) {
                            Object value = extractValueFromRow(nthRowCells, expressions, args);
                            if (value != null) {
                                counter++;
                                if (counter == iPosition) {
                                    return value;
                                }
                            }
                        }
                    }
                } else {
                    return extractValueAtIndex(adjustForShrinkingWindow + iPosition - 1,
                                               currentFrame,
                                               expressions,
                                               args);
                }
                return null;
            }
        };

        public abstract Object execute(int adjustForShrinkingWindow,
                                       WindowFrameState currentFrame,
                                       List<? extends CollectExpression<Row, ?>> expressions,
                                       boolean ignoreNulls,
                                       Input<?> ... args);

        protected static Object extractValueFromRow(Object[] nthRowCells,
                                   List<? extends CollectExpression<Row, ?>> expressions,
                                   Input<?> ... args) {
            Row nthRowInFrame = new RowN(nthRowCells);
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(nthRowInFrame);
            }
            return args[0].value();
        }

        protected static Object extractValueAtIndex(int index,
                                             WindowFrameState currentFrame,
                                             List<? extends CollectExpression<Row, ?>> expressions,
                                             Input<?> ... args) {
            Object[] nthRowCells = currentFrame.getRowInFrameAtIndexOrNull(index);
            if (nthRowCells == null) {
                return null;
            }
            return extractValueFromRow(nthRowCells, expressions, args);
        }
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.window(
                    FIRST_VALUE_NAME,
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    Implementation.FIRST_VALUE
                )
        );

        builder.add(
            Signature.window(
                    LAST_VALUE_NAME,
                    TypeSignature.parse("E"),
                    TypeSignature.parse("E")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    Implementation.LAST_VALUE
                )
        );

        builder.add(
            Signature.window(
                    NTH_VALUE_NAME,
                    TypeSignature.parse("E"),
                    DataTypes.INTEGER.getTypeSignature(),
                    TypeSignature.parse("E")
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new NthValueFunctions(
                    signature,
                    boundSignature,
                    Implementation.NTH_VALUE
                )
        );
    }

    public static final String FIRST_VALUE_NAME = "first_value";
    public static final String LAST_VALUE_NAME = "last_value";
    public static final String NTH_VALUE_NAME = "nth_value";

    private final Implementation implementation;
    private final Signature signature;
    private final BoundSignature boundSignature;
    private int seenFrameLowerBound = -1;
    private int seenFrameUpperBound = -1;
    private Object resultForCurrentFrame = null;

    private NthValueFunctions(Signature signature,
                              BoundSignature boundSignature,
                              Implementation implementation) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.implementation = implementation;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public Object execute(LongConsumer allocateBytes,
                          int idxInPartition,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          @Nullable Boolean ignoreNulls,
                          Input<?> ... args) {
        boolean ignoreNullsOrFalse = ignoreNulls != null && ignoreNulls;
        boolean shrinkingWindow = isLowerBoundIncreasing(currentFrame, seenFrameLowerBound);
        if (idxInPartition == 0 || currentFrame.upperBoundExclusive() > seenFrameUpperBound || shrinkingWindow) {

            int adjustForShrinkingWindow = 0;
            if (shrinkingWindow) {
                // consecutive shrinking frames (lower bound increments) will can have the following format :
                //         frame 1: 1 2 3 with lower bound 0
                //          frame 2:   2 3 with lower bound 1
                // We represent the frames as a view over the rows in a partition (for frame 2 the element "1" is not
                // present by virtue of the frame's lower bound being 1 and "hiding"/excluding it)
                // If we want the 2nd value (index = 1) in every frame we have to request the index _after_  the frame's
                // lower bound (in our example, to get the 2nd value in the second frame, namely "3", the requested
                // index needs to be 2)
                adjustForShrinkingWindow = currentFrame.lowerBound();
            }

            resultForCurrentFrame = implementation.execute(adjustForShrinkingWindow,
                                                           currentFrame,
                                                           expressions,
                                                           ignoreNullsOrFalse,
                                                           args);
            seenFrameLowerBound = currentFrame.lowerBound();
            seenFrameUpperBound = currentFrame.upperBoundExclusive();
        }

        return resultForCurrentFrame;
    }
}
