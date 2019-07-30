/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.tree;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.crate.common.collections.Lists2.findFirstGTEProbeValue;
import static io.crate.common.collections.Lists2.findFirstLTEProbeValue;
import static io.crate.common.collections.Lists2.findFirstNonPeer;
import static io.crate.common.collections.Lists2.findFirstPreviousPeer;
import static io.crate.sql.tree.WindowFrame.Mode.ROWS;

public class FrameBound extends Node {

    public enum Type {
        UNBOUNDED_PRECEDING {
            @Override
            public <T> int getStart(WindowFrame.Mode mode,
                                    int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Object offset,
                                    @Nullable T offsetProbeValue,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                return pStart;
            }

            @Override
            public <T> int getEnd(WindowFrame.Mode mode,
                                  int pStart,
                                  int pEnd,
                                  int currentRowIdx,
                                  @Nullable Object offset,
                                  @Nullable T offsetProbeValue,
                                  @Nullable Comparator<T> cmp,
                                  List<T> rows) {
                throw new IllegalStateException("UNBOUNDED PRECEDING cannot be the start of a frame");
            }
        },
        /**
         * <pre>
         * {@code
         * { ROWS | RANGE } <offset> PRECEDING
         *
         * ROWS mode:
         *   ROWS <offset> PRECEDING
         *   the start of the frame is *literally* <offset> number of rows before the current row
         *
         * RANGE MODE:
         *   ORDER BY x RANGE <offset> PRECEDING
         *   Every row before the current row where the value for `x` is >= `x - <offset>` is within the frame
         * }
         * </pre>
         */
        PRECEDING {
            @Override
            public <T> int getStart(WindowFrame.Mode mode,
                                    int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Object offset,
                                    @Nullable T offsetProbeValue,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                if (mode == ROWS) {
                    assert offset instanceof Long : "In ROWS mode the offset must be a non-null, non-negative number";
                    return Math.max(pStart, currentRowIdx - ((Long) offset).intValue());
                } else {
                    int firstGTEProbeValue = findFirstGTEProbeValue(rows, currentRowIdx, offsetProbeValue, cmp);
                    if (firstGTEProbeValue == -1) {
                        return currentRowIdx;
                    } else {
                        return Math.max(pStart, firstGTEProbeValue);
                    }
                }
            }

            @Override
            public <T> int getEnd(WindowFrame.Mode mode,
                                  int pStart,
                                  int pEnd,
                                  int currentRowIdx,
                                  @Nullable Object offset,
                                  @Nullable T offsetProbeValue,
                                  @Nullable Comparator<T> cmp,
                                  List<T> rows) {
                throw new UnsupportedOperationException("Custom PRECEDING frames are not supported");
            }
        },
        /*
         * In RANGE mode:
         *    - Frame starts with the current row's first peer (Row that is equal based on the ORDER BY clause)
         *    - Frame ends with the current row's first peer
         * In ROWS mode:
         *    - The current row
         */
        CURRENT_ROW {
            @Override
            public <T> int getStart(WindowFrame.Mode mode,
                                    int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Object offset,
                                    @Nullable T offsetProbeValue,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                if (mode == ROWS) {
                    return currentRowIdx;
                }

                if (pStart == currentRowIdx) {
                    return pStart;
                } else {
                    if (cmp != null) {
                        return Math.max(pStart, findFirstPreviousPeer(rows, currentRowIdx, cmp));
                    } else {
                        return currentRowIdx;
                    }
                }
            }

            @Override
            public <T> int getEnd(WindowFrame.Mode mode,
                                  int pStart,
                                  int pEnd,
                                  int currentRowIdx,
                                  @Nullable Object offset,
                                  @Nullable T offsetProbeValue,
                                  @Nullable Comparator<T> cmp,
                                  List<T> rows) {
                if (mode == ROWS) {
                    return currentRowIdx + 1;
                }

                return findFirstNonPeer(rows, currentRowIdx, pEnd, cmp);
            }
        },
        FOLLOWING {
            @Override
            public <T> int getStart(WindowFrame.Mode mode,
                                    int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Object offset,
                                    @Nullable T offsetProbeValue,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                throw new UnsupportedOperationException("Custom FOLLOWING frames are not supported");
            }

            @Override
            public <T> int getEnd(WindowFrame.Mode mode,
                                  int pStart,
                                  int pEnd,
                                  int currentRowIdx,
                                  @Nullable Object offset,
                                  @Nullable T offsetProbeValue,
                                  @Nullable Comparator<T> cmp,
                                  List<T> rows) {
                // end index is exclusive so we increment it by one when finding the interval end index
                if (mode == ROWS) {
                    assert offset instanceof Long : "In ROWS mode the offset must be a non-null, non-negative number";
                    return Math.min(pEnd, currentRowIdx + ((Long) offset).intValue() + 1);
                } else {
                    return Math.min(pEnd, findFirstLTEProbeValue(rows, currentRowIdx, offsetProbeValue, cmp) + 1);
                }
            }
        },
        UNBOUNDED_FOLLOWING {
            @Override
            public <T> int getStart(WindowFrame.Mode mode,
                                    int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Object offset,
                                    @Nullable T offsetProbeValue,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                throw new IllegalStateException("UNBOUNDED FOLLOWING cannot be the start of a frame");
            }

            @Override
            public <T> int getEnd(WindowFrame.Mode mode,
                                  int pStart,
                                  int pEnd,
                                  int currentRowIdx,
                                  @Nullable Object offset,
                                  @Nullable T offsetProbeValue,
                                  @Nullable Comparator<T> cmp,
                                  List<T> rows) {
                return pEnd;
            }
        };

        /**
         * @param computeOffset compute the offset value (`offset` PRECEDING or `offset` FOLLOWING) given a row
         * @param getOrderingValue retrieve the value of the `ORDER BY` column for a given row.
         *                         This is only provided for RANGE `offset` PRECEDING or FOLLOWING,
         *                         where the ORDER BY clause is restricted to 1 column
         * @param cmpCell a comparator for the column values
         * @param cmpRow a comparator for the full row, considering all columns included in a ORDER BY expression
         * @param <T> type of the row
         * @param <U> type of a single value
         */
        public abstract <T, U> int getStart(WindowFrame.Mode mode,
                                            int pStart,
                                            int pEnd,
                                            int currentRowIdx,
                                            Function<T, U> computeOffset,
                                            Function<T, U> getOrderingValue,
                                            @Nullable Comparator<U> cmpCell,
                                            @Nullable Comparator<T> cmpRow,
                                            List<T> rows);

        public abstract <T> int getEnd(WindowFrame.Mode mode,
                                       int pStart,
                                       int pEnd,
                                       int currentRowIdx,
                                       @Nullable Object offset,
                                       @Nullable T offsetProbeValue,
                                       @Nullable Comparator<T> cmp,
                                       List<T> rows);
    }

    private final Type type;

    @Nullable
    private final Expression value;

    public FrameBound(Type type) {
        this(type, null);
    }

    public FrameBound(Type type, @Nullable Expression value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    @Nullable
    public Expression getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FrameBound that = (FrameBound) o;

        if (type != that.type) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FrameBound{" +
               "type=" + type +
               ", value=" + value +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFrameBound(this, context);
    }
}
