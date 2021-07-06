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
    private Integer offset;
    private Integer cache; // holds a null or an index to 'offset' number of non-null elements away from idxInPartition

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
                          boolean ignoreNulls,
                          Input[] args) {
        calculateOffsetIfNull(args);
        if (offset == null) {
            return null;
        }
        try {
            if (ignoreNulls == false) {
                return getValueFromTargetIndex(offsetDirection.getTargetIndex(idxInPartition, offset),
                                               currentFrame,
                                               expressions,
                                               args);
            }
            if (offsetDirection == OffsetDirection.FORWARD) {
                return executeLeadIgnoreNulls(idxInPartition, currentFrame, expressions, args);
            } else {
                return executeLagIgnoreNulls(idxInPartition, currentFrame, expressions, args);
            }
        } catch (IndexOutOfBoundsException e) {
            return getDefaultOrNull(args);
        }
    }

    private Object executeLeadIgnoreNulls(int idxInPartition,
                                          WindowFrameState currentFrame,
                                          List<? extends CollectExpression<Row, ?>> expressions,
                                          Input[] args) {
        return null;
    }

    private Object executeLagIgnoreNulls(int idxInPartition,
                                         WindowFrameState currentFrame,
                                         List<? extends CollectExpression<Row, ?>> expressions,
                                         Input[] args) {
        // cache and idxInPartition forms a window that holds 'offset' number of non-null elements and any number of nulls.
        // However, if idxInPartition has not moved deeper into the frame, cache will remain null

        // idxInPartition is exclusive so when counting 'offset' number of non-nulls, previous value is used (idxInPartition - 1)
        var prevValue = getValueFromTargetIndex(idxInPartition - 1, currentFrame, expressions, args);
        if (prevValue != null) {
            // idxInPartition is incremented at every iteration and if the prevValue is not null,
            // cache need to find the idx of the next not null element
            moveCacheToNextNotNull(idxInPartition, currentFrame, expressions, args);
        }
        return getValueFromTargetIndex(cache, currentFrame, expressions, args);
    }

    private void moveCacheToNextNotNull(int idxInPartition,
                                        WindowFrameState currentFrame,
                                        List<? extends CollectExpression<Row, ?>> expressions,
                                        Input[] args) {
        if (cache != null) {
            // from cached index, search in forward direction for a non-null element
            for (int i = 1; true; i++) {
                Object lagValue = getValueFromTargetIndex(cache + i, currentFrame, expressions, args);
                if (lagValue != null) {
                    cache = cache + i;
                    break;
                }
            }
        } else {
            // if cache is null, from the current index, search for 'offset' number of non-null elements in 'offsetDirection' and cache the index
            cache = findNonNullOffsetFromCurrentIndex(idxInPartition, currentFrame, expressions, args);
        }
    }

    private Integer findNonNullOffsetFromCurrentIndex(int idxInPartition,
                                                      WindowFrameState currentFrame,
                                                      List<? extends CollectExpression<Row, ?>> expressions,
                                                      Input[] args) {
        // search for 'offset' number of non-null elements in 'offsetDirection' and return the index
        for (int i = 1, counter = 0; true; i++) {
            Object value = getValueFromTargetIndex(offsetDirection.getTargetIndex(idxInPartition, i),
                                                      currentFrame,
                                                      expressions,
                                                      args);
            if (value != null) {
                counter++;
                if (counter == offset) {
                    return offsetDirection.getTargetIndex(idxInPartition, i);
                }
            }
        }
    }

    private Object getValueFromTargetIndex(Integer targetIndex,
                                           WindowFrameState currentFrame,
                                           List<? extends CollectExpression<Row, ?>> expressions,
                                           Input[] args) {
        if (targetIndex == null) {
            throw new IndexOutOfBoundsException();
        }
        Object[] lagRowCells = currentFrame.getRowInPartitionAtIndexOrNull(targetIndex);
        if (lagRowCells == null) {
            throw new IndexOutOfBoundsException();
        }
        var lagRow = new RowN(lagRowCells);
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(lagRow);
        }
        return args[0].value();
    }

    private void calculateOffsetIfNull(Input[] args) {
        if (offset == null) {
            if (args.length > 1) {
                Object offsetValue = args[1].value();
                if (offsetValue != null) {
                    offset = ((Number) offsetValue).intValue();
                }
            } else {
                offset = 1;
            }
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
