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
import io.crate.module.EnterpriseFunctionsModule;
import io.crate.types.DataType;

import java.util.List;

public class LastValueFunction implements WindowFunction {

    private static final String NAME = "last_value";

    private final FunctionInfo info;
    private int seenFrameUpperBound = -1;
    private Object resultForCurrentFrame = null;

    private LastValueFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Object execute(int rowIdx,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          Input... args) {
        if (isNewFrame(seenFrameUpperBound, currentFrame)) {
            seenFrameUpperBound = currentFrame.upperBoundExclusive();
            int frameSize = currentFrame.size();
            Object[] lastRowCells = currentFrame.getRowAtIndexOrNull(frameSize - 1);
            assert lastRowCells != null : "Last row must exist";
            Row lastRowInFrame = new RowN(lastRowCells);
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(lastRowInFrame);
            }

            resultForCurrentFrame = args[0].value();
        }

        return resultForCurrentFrame;
    }

    public static void register(EnterpriseFunctionsModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams.SINGLE_ANY) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new LastValueFunction(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), dataTypes.get(0), FunctionInfo.Type.WINDOW));
            }
        });
    }
}
