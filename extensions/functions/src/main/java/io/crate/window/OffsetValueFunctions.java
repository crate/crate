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
import java.util.function.Supplier;

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

    private abstract class OffsetDirection {
        /* cachedNonNullIndex and idxInPartition forms an offset window containing 'offset' number of non-null elements and any number of nulls.
           The main perf. benefits will be seen when series of nulls are present.
           In the cases where idxInPartition is near the bounds that the cache cannot point to the valid index, it will hold a null. */
        protected Integer cachedNonNullIndex;

        abstract int getTargetIndex(int idxInPartition, int offset);

        abstract Object getValueAtOffsetIgnoringNulls(int idxInPartition,
                                                      int offset,
                                                      WindowFrameState currentFrame,
                                                      List<? extends CollectExpression<Row, ?>> expressions,
                                                      Input[] args);

        public void moveCacheToNextNonNull(WindowFrameState currentFrame,
                                           List<? extends CollectExpression<Row, ?>> expressions,
                                           Input[] args) {
            if (cachedNonNullIndex == null) {
                return;
            }
            /* from cached index, search in 'forward' direction for a non-null element.
               for 'lag', cache index will never go past idxInPartition(current index).
               for 'lead', cache index may reach upperBoundExclusive. */
            for (int i = cachedNonNullIndex + 1; i <= currentFrame.upperBoundExclusive(); i++) {
                if (i == currentFrame.upperBoundExclusive()) {
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

        public Integer findNonNullOffsetFromCurrentIndex(int idxInPartition,
                                                         int offset,
                                                         WindowFrameState currentFrame,
                                                         List<? extends CollectExpression<Row, ?>> expressions,
                                                         Input[] args) {
            /* Search for 'offset' number of non-null elements in 'offsetDirection' and return the index if found.
               If index goes out of bound, an exception will be thrown and eventually invoke getDefaultOrNull(...) */
            for (int i = 1, counter = 0; ; i++) {
                Object value = getValueAtTargetIndex(getTargetIndex(idxInPartition, i),
                                                     currentFrame,
                                                     expressions,
                                                     args);
                if (value != null) {
                    counter++;
                    if (counter == offset) {
                        return getTargetIndex(idxInPartition, i);
                    }
                }
            }
        }

        public Object getValueAtOffset(int idxAtPartition,
                                       int offset,
                                       WindowFrameState currentFrame,
                                       List<? extends CollectExpression<Row, ?>> expressions,
                                       boolean ignoreNulls,
                                       Input[] args) {
            if (ignoreNulls == false) {
                var targetIndex = getTargetIndex(idxAtPartition, offset);
                return getValueAtTargetIndex(targetIndex, currentFrame, expressions, args);
            } else {
                return getValueAtOffsetIgnoringNulls(idxAtPartition, offset, currentFrame, expressions, args);
            }
        }

        static Object getValueAtTargetIndex(Integer targetIndex,
                                            WindowFrameState currentFrame,
                                            List<? extends CollectExpression<Row, ?>> expressions,
                                            Input[] args) {
            if (targetIndex == null) {
                throw new IndexOutOfBoundsException();
            }
            Object[] rowCells = currentFrame.getRowInPartitionAtIndexOrNull(targetIndex);
            if (rowCells == null) {
                throw new IndexOutOfBoundsException();
            }
            var row = new RowN(rowCells);
            for (CollectExpression<Row, ?> expression : expressions) {
                expression.setNextRow(row);
            }
            return args[0].value();
        }
    }

    private final Supplier<OffsetDirection> FORWARD = () -> new OffsetDirection() {

        @Override
        int getTargetIndex(int idxInPartition, int offset) {
            return idxInPartition + offset;
        }

        @Override
        Object getValueAtOffsetIgnoringNulls(int idxInPartition,
                                             int offset,
                                             WindowFrameState currentFrame,
                                             List<? extends CollectExpression<Row, ?>> expressions,
                                             Input[] args) {
            if (cachedNonNullIndex == null) {
                cachedNonNullIndex = findNonNullOffsetFromCurrentIndex(idxInPartition, offset, currentFrame, expressions, args);
            } else {
                var curValue = getValueAtTargetIndex(idxInPartition, currentFrame, expressions, args);
                if (curValue != null) {
                    moveCacheToNextNonNull(currentFrame, expressions, args);
                }
            }
            return getValueAtTargetIndex(cachedNonNullIndex, currentFrame, expressions, args);
        }
    };

    private final Supplier<OffsetDirection> BACKWARD = () -> new OffsetDirection() {

        @Override
        int getTargetIndex(int idxInPartition, int offset) {
            return idxInPartition - offset;
        }

        @Override
        Object getValueAtOffsetIgnoringNulls(int idxInPartition,
                                             int offset,
                                             WindowFrameState currentFrame,
                                             List<? extends CollectExpression<Row, ?>> expressions,
                                             Input[] args) {
            if (cachedNonNullIndex == null) {
                cachedNonNullIndex = findNonNullOffsetFromCurrentIndex(idxInPartition, offset, currentFrame, expressions, args);
            } else {
                var prevValue = getValueAtTargetIndex(idxInPartition - 1, currentFrame, expressions, args);
                if (prevValue != null) {
                    moveCacheToNextNonNull(currentFrame, expressions, args);
                }
            }
            return getValueAtTargetIndex(cachedNonNullIndex, currentFrame, expressions, args);
        }
    };

    private OffsetDirection resolveOffsetDirection(int offset) {
        if (LAG_NAME.equals(signature.getName().name())) {
            if (offset >= 0) {
                return BACKWARD.get();
            } else {
                return FORWARD.get();
            }
        } else {
            if (offset >= 0) {
                return FORWARD.get();
            } else {
                return BACKWARD.get();
            }
        }
    }

    private static final String LAG_NAME = "lag";
    private static final String LEAD_NAME = "lead";

    private OffsetDirection offsetDirection;
    private final Signature signature;
    private final Signature boundSignature;
    private Integer cachedOffset;

    private OffsetValueFunctions(Signature signature, Signature boundSignature) {
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
                          boolean ignoreNulls,
                          Input[] args) {
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
        if (offset == 0) {
            ignoreNulls = false;
        }
        // if offset is not constant but varying per iteration, OffsetDirection.cachedNonNullIndex cannot be utilized
        if (idxInPartition == 0 || (cachedOffset != null && cachedOffset != offset)) {
            offsetDirection = resolveOffsetDirection(offset);
        }
        cachedOffset = offset;
        final int cachedOffsetMagnitude = Math.abs(cachedOffset);
        try {
            return offsetDirection.getValueAtOffset(idxInPartition,
                                                    cachedOffsetMagnitude,
                                                    currentFrame,
                                                    expressions,
                                                    ignoreNulls,
                                                    args);
        } catch (IndexOutOfBoundsException e) {
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
            OffsetValueFunctions::new
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            OffsetValueFunctions::new
        );
        module.register(
            Signature.window(
                LEAD_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            OffsetValueFunctions::new
        );

        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            OffsetValueFunctions::new
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            OffsetValueFunctions::new
        );
        module.register(
            Signature.window(
                LAG_NAME,
                parseTypeSignature("E"),
                DataTypes.INTEGER.getTypeSignature(),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            OffsetValueFunctions::new
        );
    }
}
