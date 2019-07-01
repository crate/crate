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

/**
 * The offset functions return the evaluated value at a row that
 * precedes or follows the current row by offset within its partition.
 * <p>
 * Synopsis:
 *      lag/lead(argument any [, offset integer [, default any ] ])
 */
public class OffsetValueFunctions implements WindowFunction {


    private enum OffsetDirection {
        FORWARD {
            @Override
            int getTargetIndex(int idxInPartition, int offset) {
                return idxInPartition + offset;
            }
        },
        BACKWARD {
            @Override
            int getTargetIndex(int idxInPartition, int offset) {
                return idxInPartition - offset;
            }
        };
        abstract int getTargetIndex(int idxInPartition, int offset);
    }

    private static final String LAG_NAME = "lag";
    private static final String LEAD_NAME = "lead";

    private final OffsetDirection offsetDirection;
    private final FunctionInfo info;

    private OffsetValueFunctions(FunctionInfo info, OffsetDirection offsetDirection) {
        this.info = info;
        this.offsetDirection = offsetDirection;
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
        final int offset;
        if (args.length > 1) {
            Object offsetValue = args[1].value();
            if (offsetValue == null) {
                return null;
            } else {
                offset = ((Number) offsetValue).intValue();
            }
        } else {
            offset = 1;
        }

        var lagRowCells = currentFrame
            .getRowInPartitionAtIndexOrNull(offsetDirection.getTargetIndex(idxInPartition, offset));
        if (lagRowCells != null) {
            var lagRow = new RowN(lagRowCells);
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(lagRow);
            }

            return args[0].value();
        } else {
            return getDefaultOrNull(args);
        }
    }

    private static Object getDefaultOrNull(Input[] args) {
        if (args.length == 3) {
            return args[2].value();
        } else {
            return null;
        }
    }

    public static void register(EnterpriseFunctionsModule module) {
        module.register(LEAD_NAME, new BaseFunctionResolver(
            FuncParams.builder(Param.ANY)
                .withVarArgs(Param.INTEGER)
                .withVarArgs(Param.ANY)
                .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new OffsetValueFunctions(
                    new FunctionInfo(
                        new FunctionIdent(LEAD_NAME, dataTypes),
                        dataTypes.get(0),
                        FunctionInfo.Type.WINDOW),
                    OffsetDirection.FORWARD
                );
            }
        });

        module.register(LAG_NAME, new BaseFunctionResolver(
            FuncParams.builder(Param.ANY)
                .withVarArgs(Param.INTEGER)
                .withVarArgs(Param.ANY)
                .build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new OffsetValueFunctions(
                    new FunctionInfo(
                        new FunctionIdent(LAG_NAME, dataTypes),
                        dataTypes.get(0),
                        FunctionInfo.Type.WINDOW),
                    OffsetDirection.BACKWARD
                );
            }
        });
    }
}
