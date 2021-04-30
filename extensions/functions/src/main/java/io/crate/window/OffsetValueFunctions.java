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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

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
    private final Signature signature;
    private final Signature boundSignature;

    private OffsetValueFunctions(Signature signature, Signature boundSignature, OffsetDirection offsetDirection) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.offsetDirection = offsetDirection;
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

    public static void register(ExtraFunctionsModule module) {
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.FORWARD
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.FORWARD
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.FORWARD
                )
        );

        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.BACKWARD
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.BACKWARD
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    OffsetDirection.BACKWARD
                )
        );
    }
}
