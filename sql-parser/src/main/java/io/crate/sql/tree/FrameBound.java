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

import static io.crate.common.collections.Lists2.findFirstNonPeer;
import static io.crate.common.collections.Lists2.findFirstPreviousPeer;

public class FrameBound extends Node {

    public enum Type {
        UNBOUNDED_PRECEDING {
            @Override
            public <T> int getStart(int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                return pStart;
            }

            @Override
            public <T> int getEnd(int pStart, int pEnd, int currentRowIdx, @Nullable Comparator<T> cmp, List<T> rows) {
                throw new IllegalStateException("UNBOUNDED PRECEDING cannot be the start of a frame");
            }
        },
        PRECEDING {
            @Override
            public <T> int getStart(int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                throw new UnsupportedOperationException("Custom PRECEDING frames are not supported");
            }

            @Override
            public <T> int getEnd(int pStart, int pEnd, int currentRowIdx, @Nullable Comparator<T> cmp, List<T> rows) {
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
            public <T> int getStart(int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
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
            public <T> int getEnd(int pStart, int pEnd, int currentRowIdx, @Nullable Comparator<T> cmp, List<T> rows) {
                return findFirstNonPeer(rows, currentRowIdx, pEnd, cmp);
            }
        },
        FOLLOWING {
            @Override
            public <T> int getStart(int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                throw new UnsupportedOperationException("Custom FOLLOWING frames are not supported");
            }

            @Override
            public <T> int getEnd(int pStart, int pEnd, int currentRowIdx, @Nullable Comparator<T> cmp, List<T> rows) {
                throw new UnsupportedOperationException("Custom FOLLOWING frames are not supported");
            }
        },
        UNBOUNDED_FOLLOWING {
            @Override
            public <T> int getStart(int pStart,
                                    int pEnd,
                                    int currentRowIdx,
                                    @Nullable Comparator<T> cmp,
                                    List<T> rows) {
                throw new IllegalStateException("UNBOUNDED FOLLOWING cannot be the start of a frame");
            }

            @Override
            public <T> int getEnd(int pStart, int pEnd, int currentRowIdx, @Nullable Comparator<T> cmp, List<T> rows) {
                return pEnd;
            }
        };

        public abstract <T> int getStart(int pStart,
                                     int pEnd,
                                     int currentRowIdx,
                                     @Nullable Comparator<T> cmp,
                                     List<T> rows);

        public abstract <T> int getEnd(int pStart,
                                   int pEnd,
                                   int currentRowIdx,
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
