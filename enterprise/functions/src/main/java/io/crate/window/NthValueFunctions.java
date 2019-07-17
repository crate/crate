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
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.module.EnterpriseFunctionsModule;
import io.crate.types.DataType;

import java.util.List;
import java.util.function.BiFunction;

import static io.crate.execution.engine.window.WindowFrameState.isLowerBoundIncreasing;

public class NthValueFunctions implements WindowFunction {

    public static final String LAST_VALUE_NAME = "last_value";
    private static final String FIRST_VALUE_NAME = "first_value";
    private static final String NTH_VALUE = "nth_value";

    private final FunctionInfo info;
    private final BiFunction<WindowFrameState, Input[], Integer> frameIndexSupplier;
    private int seenFrameLowerBound = -1;
    private int seenFrameUpperBound = -1;
    private Object resultForCurrentFrame = null;

    private NthValueFunctions(FunctionInfo info, BiFunction<WindowFrameState, Input[], Integer> frameIndexSupplier) {
        this.info = info;
        this.frameIndexSupplier = frameIndexSupplier;
    }

    @Override
    public FunctionInfo info() {
        return info;
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

    public static void register(EnterpriseFunctionsModule module) {
        module.register(FIRST_VALUE_NAME, new BaseFunctionResolver(FuncParams.SINGLE_ANY) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new NthValueFunctions(
                    new FunctionInfo(
                        new FunctionIdent(FIRST_VALUE_NAME, dataTypes), dataTypes.get(0), FunctionInfo.Type.WINDOW),
                    (frame, inputs) -> 0
                );
            }
        });
        module.register(LAST_VALUE_NAME, new BaseFunctionResolver(FuncParams.SINGLE_ANY) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new NthValueFunctions(
                    new FunctionInfo(
                        new FunctionIdent(LAST_VALUE_NAME, dataTypes), dataTypes.get(0), FunctionInfo.Type.WINDOW),
                    (frame, inputs) -> frame.size() - 1
                );
            }
        });
        module.register(NTH_VALUE, new BaseFunctionResolver(FuncParams.builder(Param.ANY, Param.NUMERIC).build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new NthValueFunctions(
                    new FunctionInfo(new FunctionIdent(NTH_VALUE, dataTypes), dataTypes.get(0), FunctionInfo.Type.WINDOW),
                    (frame, inputs) -> {
                        Number position = (Number) inputs[1].value();
                        if (position == null) {
                            // treating a null position as an out-of-bounds position
                            return -1;
                        }
                        return position.intValue() - 1;
                    }
                );
            }
        });
    }
}
