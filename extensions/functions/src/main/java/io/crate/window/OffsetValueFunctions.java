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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.window.WindowFrameState;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

/**
 * The offset functions return the evaluated value at a row that
 * precedes or follows the current row by offset within its partition.
 * <p>
 * Synopsis:
 *      lag/lead(argument any [, offset integer [, default any ] ])
 */
public class OffsetValueFunctions implements WindowFunction {

    private Object getValueAtOffsetIgnoringNulls(int idxInPartition,
                                                 int offset,
                                                 WindowFrameState currentFrame,
                                                 List<? extends CollectExpression<Row, ?>> expressions,
                                                 Input<?> ... args) {
        if (cachedNonNullIndex == null) {
            cachedNonNullIndex = findNonNullOffsetFromCurrentIndex(idxInPartition,
                                                                   offset,
                                                                   currentFrame,
                                                                   expressions,
                                                                   args);
        } else {
            if (resolvedDirection > 0) {
                var curValue = getValueAtTargetIndex(idxInPartition, currentFrame, expressions, args);
                if (curValue != null) {
                    moveCacheToNextNonNull(currentFrame, expressions, args);
                }
            } else {
                var prevValue = getValueAtTargetIndex(idxInPartition - 1, currentFrame, expressions, args);
                if (prevValue != null) {
                    moveCacheToNextNonNull(currentFrame, expressions, args);
                }
            }
        }
        return getValueAtTargetIndex(cachedNonNullIndex, currentFrame, expressions, args);
    }

    private void moveCacheToNextNonNull(WindowFrameState currentFrame,
                                        List<? extends CollectExpression<Row, ?>> expressions,
                                        Input<?> ... args) {
        if (cachedNonNullIndex == null) {
            return;
        }
        /* from cached index, search from left to right for a non-null element.
           for 'lag', cache index will never go past idxInPartition(current index).
           for 'lead', cache index may reach partitionEnd. */
        for (int i = cachedNonNullIndex + 1; i <= currentFrame.partitionEnd(); i++) {
            if (i == currentFrame.partitionEnd()) {
                cachedNonNullIndex = null;
                break;
            }
            Object value = getValueAtTargetIndex(i, currentFrame, expressions, args);
            if (value != null) {
                cachedNonNullIndex = i;
                break;
            }
        }
    }

    private Integer findNonNullOffsetFromCurrentIndex(int idxInPartition,
                                                      int offset,
                                                      WindowFrameState currentFrame,
                                                      List<? extends CollectExpression<Row, ?>> expressions,
                                                      Input<?> ... args) {
        /* Search for 'offset' number of non-null elements in 'resolvedDirection' and return the index if found.
           If index goes out of bound, an exception will be thrown and eventually invoke getDefaultOrNull(...) */
        for (int i = 1, counter = 0; ; i++) {
            Object value = getValueAtTargetIndex(getTargetIndex(idxInPartition, i), currentFrame, expressions, args);
            if (value != null) {
                counter++;
                if (counter == offset) {
                    return getTargetIndex(idxInPartition, i);
                }
            }
        }
    }

    private Object getValueAtOffset(int idxAtPartition,
                                    int offset,
                                    WindowFrameState currentFrame,
                                    List<? extends CollectExpression<Row, ?>> expressions,
                                    boolean ignoreNulls,
                                    Input<?> ... args) {
        if (ignoreNulls == false) {
            var targetIndex = getTargetIndex(idxAtPartition, offset);
            return getValueAtTargetIndex(targetIndex, currentFrame, expressions, args);
        } else {
            if (offset == 0) {
                throw new IllegalArgumentException("offset 0 is not a valid argument if ignore nulls flag is set");
            }
            return getValueAtOffsetIgnoringNulls(idxAtPartition, offset, currentFrame, expressions, args);
        }
    }

    private Object getValueAtTargetIndex(@Nullable Integer targetIndex,
                                         WindowFrameState currentFrame,
                                         List<? extends CollectExpression<Row, ?>> expressions,
                                         Input<?> ... args) {
        if (targetIndex == null) {
            throw new IndexOutOfBoundsException();
        }
        Row row;
        if (indexToRow.containsKey(targetIndex)) {
            row = indexToRow.get(targetIndex);
        } else {
            Object[] rowCells = currentFrame.getRowInPartitionAtIndexOrNull(targetIndex);
            if (rowCells == null) {
                throw new IndexOutOfBoundsException();
            }
            row = new RowN(rowCells);
            indexToRow.putIfAbsent(targetIndex, row);
        }
        for (CollectExpression<Row, ?> expression : expressions) {
            expression.setNextRow(row);
        }
        return args[0].value();
    }

    private int getTargetIndex(int idxInPartition, int offset) {
        return idxInPartition + offset * resolvedDirection;
    }

    private static final String LAG_NAME = "lag";
    private static final String LEAD_NAME = "lead";
    private static final int LAG_DEFAULT_OFFSET = -1;
    private static final int LEAD_DEFAULT_OFFSET = 1;
    private final int directionMultiplier;

    private final Signature signature;
    private final BoundSignature boundSignature;
    private Integer cachedOffset;
    private int resolvedDirection;
    /* cachedNonNullIndex and idxInPartition forms an offset window containing 'offset' number of non-null elements and any number of nulls.
       The main perf. benefits will be seen when series of nulls are present.
       In the cases where idxInPartition is near the bounds that the cache cannot point to the valid index, it will hold a null. */
    private Integer cachedNonNullIndex;
    private Map<Integer, Row> indexToRow;

    private OffsetValueFunctions(Signature signature, BoundSignature boundSignature, int directionMultiplier) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.directionMultiplier = directionMultiplier;
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
    public Object execute(int idxInPartition,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          @Nullable Boolean ignoreNulls,
                          Input<?> ... args) {
        boolean ignoreNullsOrFalse = ignoreNulls != null && ignoreNulls;
        final int offset;
        if (args.length > 1) {
            Object offsetValue = args[1].value();
            if (offsetValue != null) {
                offset = ((Number) offsetValue).intValue();
            } else {
                return null;
            }
        } else {
            offset = 1;
        }
        if (idxInPartition == 0) {
            indexToRow = new HashMap<>(currentFrame.size());
        }
        // if the offset is changed compared to the previous iteration, cache will be cleared
        if (idxInPartition == 0 || (cachedOffset != null && cachedOffset != offset)) {
            if (offset != 0) {
                resolvedDirection = offset / Math.abs(offset) * directionMultiplier;
            }
            cachedNonNullIndex = null;
        }
        cachedOffset = offset;
        final int cachedOffsetMagnitude = Math.abs(cachedOffset);
        try {
            return getValueAtOffset(idxInPartition,
                                    cachedOffsetMagnitude,
                                    currentFrame,
                                    expressions,
                                    ignoreNullsOrFalse,
                                    args);
        } catch (IndexOutOfBoundsException e) {
            return getDefaultOrNull(args);
        }
    }

    private static Object getDefaultOrNull(Input<?> ... args) {
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
                TypeSignature.parse("E"),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                TypeSignature.parse("E"),
                DataTypes.INTEGER.getTypeSignature(),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET
                )
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                TypeSignature.parse("E"),
                DataTypes.INTEGER.getTypeSignature(),
                TypeSignature.parse("E"),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LEAD_DEFAULT_OFFSET
                )
        );

        module.register(
            Signature.window(
                LAG_NAME,
                TypeSignature.parse("E"),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                TypeSignature.parse("E"),
                DataTypes.INTEGER.getTypeSignature(),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET
                )
        );
        module.register(
            Signature.window(
                LAG_NAME,
                TypeSignature.parse("E"),
                DataTypes.INTEGER.getTypeSignature(),
                TypeSignature.parse("E"),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new OffsetValueFunctions(
                    signature,
                    boundSignature,
                    LAG_DEFAULT_OFFSET
                )
        );
    }
}
