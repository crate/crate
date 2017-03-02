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

package io.crate.data;

import java.util.BitSet;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

public class NestedLoopBatchIterator implements BatchIterator {

    final CombinedColumn rowData;
    final BatchIterator left;
    final BatchIterator right;

    /**
     * points to the batchIterator which will be used on the next {@link #moveNext()} call
     */
    BatchIterator activeIt;

    public static BatchIterator fullOuterJoin(BatchIterator left,
                                              BatchIterator right,
                                              Function<Columns, BooleanSupplier> joinCondition) {
        return new FullOuterJoinBatchIterator(left, right, joinCondition);
    }

    public static BatchIterator crossJoin(BatchIterator left, BatchIterator right) {
        return new NestedLoopBatchIterator(left, right);
    }

    public static BatchIterator leftJoin(BatchIterator left,
                                         BatchIterator right,
                                         Function<Columns, BooleanSupplier> joinCondition) {
        return new LeftJoinBatchIterator(left, right, joinCondition);
    }

    public static BatchIterator rightJoin(BatchIterator left,
                                          BatchIterator right,
                                          Function<Columns, BooleanSupplier> joinCondition) {
        return new RightJoinBatchIterator(left, right, joinCondition);
    }

    NestedLoopBatchIterator(BatchIterator left, BatchIterator right) {
        this.left = left;
        this.right = right;
        this.activeIt = left;
        this.rowData = new CombinedColumn(left.rowData(), right.rowData());
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
    }

    @Override
    public boolean moveNext() {
        if (activeIt == left) {
            return tryAdvanceLeftAndRight();
        } else {
            return tryAdvanceRight();
        }
    }

    private boolean tryAdvanceRight() {
        if (right.moveNext()) {
            return true;
        }
        if (right.allLoaded() == false) {
            return false;
        }

        // new outer loop iteration
        right.moveToStart();
        if (left.moveNext()) {
            return right.moveNext();
        }
        activeIt = left; // left is either out of rows or needs to load more data - whatever the case,
                         // next moveNext must operate on left
        return false;
    }

    private boolean tryAdvanceLeftAndRight() {
        while (left.moveNext()) {
            activeIt = right;
            if (right.moveNext()) {
                return true;
            }
            if (right.allLoaded() == false) {
                return false;
            }
            right.moveToStart();
        }
        return false;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return activeIt.allLoaded();
    }

    private static class CombinedColumn implements Columns {

        private final static Input<Object> NULL_INPUT = () -> null;

        private final ProxyInput[] inputs;
        private final Columns left;
        private final Columns right;

        CombinedColumn(Columns left, Columns right) {
            this.left = left;
            this.right = right;
            inputs = new ProxyInput[left.size() + right.size()];
            for (int i = 0; i < left.size(); i++) {
                inputs[i] = new ProxyInput();
                inputs[i].input = left.get(i);
            }
            for (int i = left.size(); i < inputs.length; i++) {
                inputs[i] = new ProxyInput();
                inputs[i].input = right.get(i - left.size());
            }
        }

        @Override
        public Input<?> get(int index) {
            return inputs[index];
        }

        @Override
        public int size() {
            return inputs.length;
        }

        void nullRight() {
            for (int i = left.size(); i < inputs.length; i++) {
                inputs[i].input = NULL_INPUT;
            }
        }

        void resetRight() {
            for (int i = left.size(); i < inputs.length; i++) {
                inputs[i].input = right.get(i - left.size());
            }
        }

        void nullLeft() {
            for (int i = 0; i < left.size(); i++) {
                inputs[i].input = NULL_INPUT;
            }
        }

        void resetLeft() {
            for (int i = 0; i < left.size(); i++) {
                inputs[i].input = left.get(i);
            }
        }
    }

    /**
     * <pre>
     *     for (leftRow in left) {
     *         for (rightRow in right) {
     *             match?
     *                  onRow
     *         }
     *         if (noRightRowMatched) {
     *              onRow // with right side null
     *         }
     *     }
     * </pre>
     */
    private static class LeftJoinBatchIterator extends NestedLoopBatchIterator {

        private final BooleanSupplier joinCondition;
        boolean hadMatch = false;
        private boolean loading = false;

        LeftJoinBatchIterator(BatchIterator left,
                              BatchIterator right,
                              Function<Columns, BooleanSupplier> joinCondition) {
            super(left, right);
            this.joinCondition = joinCondition.apply(rowData());
        }

        @Override
        public boolean moveNext() {
            if (loading) {
                throw new IllegalStateException("BatchIterator is loading");
            }
            while (true) {
                rowData.resetRight();
                if (activeIt == left) {
                    return moveLeftSide();
                }

                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
                activeIt = left;
            }
        }

        private boolean moveLeftSide() {
            activeIt = right;
            while (left.moveNext()) {
                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
            }
            activeIt = left;
            return false;
        }

        @Override
        public CompletionStage<?> loadNextBatch() {
            loading = true;
            return super.loadNextBatch().whenComplete((result, failure) -> loading = false);
        }

        /**
         * try to move the right side
         * @return true  -> moved and matched
         *         false -> need to load more data
         *         null  -> reached it's end and moved back to start -> left side needs to continue
         */
        private Boolean tryAdvanceRight() {
            while (right.moveNext()) {
                if (joinCondition.getAsBoolean()) {
                    hadMatch = true;
                    return true;
                }
            }
            if (right.allLoaded() == false) {
                return false;
            }
            right.moveToStart();
            if (hadMatch == false) {
                activeIt = left;
                rowData.nullRight();
                return true;
            }
            hadMatch = false;
            return null;
        }
    }

    /**
     * Nested Loop + additional loop afterwards to emit any rows that had no matches
     *
     * <pre>
     *     for (leftRow in left) {
     *         for (rightRow in right) {
     *             if matched {
     *                 markPosition(pos)
     *                 onRow
     *             }
     *         }
     *     }
     *
     *     for (rightRow in right) {
     *         if (noMatch(position)) {
     *             onRow (left-side-null)
     *         }
     *     }
     * </pre>
     */
    private static class RightJoinBatchIterator extends NestedLoopBatchIterator {

        private final BitSet matchedRows = new BitSet();
        private final BooleanSupplier joinCondition;

        private boolean postNL = false;
        private int position = -1;

        RightJoinBatchIterator(BatchIterator left, BatchIterator right, Function<Columns, BooleanSupplier> joinCondition) {
            super(left, right);
            this.joinCondition = joinCondition.apply(rowData());
        }

        @Override
        public void moveToStart() {
            super.moveToStart();
            rowData.resetLeft();
            activeIt = left;
            postNL = false;
            position = -1;
        }

        @Override
        public boolean moveNext() {
            if (postNL) {
                return moveRightPostNL();
            }
            while (true) {
                if (activeIt == left) {
                    return moveLeft();
                }
                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
                activeIt = left;
            }
        }

        private boolean moveLeft() {
            while (left.moveNext()) {
                activeIt = right;
                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
            }
            activeIt = left;
            postNL = left.allLoaded();
            if (postNL) {
                position = -1;
                activeIt = right;
                rowData.nullLeft();
                return moveRightPostNL();
            }
            return false;
        }

        /**
         * @return true  -> right moved
         *         false -> need to load more data
         *         null  -> reached its end, need to continue on left
         */
        private Boolean tryAdvanceRight() {
            while (right.moveNext()) {
                position++;
                if (joinCondition.getAsBoolean()) {
                    matchedRows.set(position);
                    return true;
                }
            }
            if (right.allLoaded() == false) {
                return false;
            }
            position = -1;
            right.moveToStart();
            return null;
        }

        private boolean moveRightPostNL() {
            while (right.moveNext()) {
                position++;
                if (matchedRows.get(position) == false) {
                    return true;
                }
            }
            return false;
        }
    }


    /**
     * Combination of left + right join:
     *
     * <pre>
     *     for (leftRow in left) {
     *         for (rightRow in right) {
     *             match?
     *               onRow
     *         }
     *         if (noMatches) {
     *             onRow (right-side-null)
     *         }
     *     }
     *
     *     for (rightRow in right) {
     *         if (noMatches) {
     *              onRow (left-side-null)
     *         }
     *     }
     * </pre>
     */
    private static class FullOuterJoinBatchIterator extends NestedLoopBatchIterator {

        private final BitSet matchedRows = new BitSet();
        private final BooleanSupplier joinCondition;

        private boolean postNL = false;
        private boolean hadMatch = false;
        private int position = -1;

        FullOuterJoinBatchIterator(BatchIterator left, BatchIterator right, Function<Columns, BooleanSupplier> joinCondition) {
            super(left, right);
            this.joinCondition = joinCondition.apply(rowData());
        }

        @Override
        public void moveToStart() {
            postNL = false;
            hadMatch = false;
            position = -1;
            activeIt = left;
            rowData.resetRight();
            rowData.resetLeft();
            super.moveToStart();
        }

        @Override
        public boolean moveNext() {
            if (postNL) {
                return moveRightPostNL();
            }
            while (true) {
                rowData.resetRight();
                if (activeIt == left) {
                    return moveLeft();
                }
                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
                activeIt = left;
            }
        }

        private boolean moveLeft() {
            while (left.moveNext()) {
                activeIt = right;
                Boolean x = tryAdvanceRight();
                if (x != null) {
                    return x;
                }
            }
            activeIt = left;
            postNL = left.allLoaded();
            if (postNL) {
                position = -1;
                activeIt = right;
                rowData.nullLeft();
                return moveRightPostNL();
            }
            return false;
        }

        /**
         * @return true  -> right moved
         *         false -> need to load more data
         *         null  -> reached its end, need to continue on left
         */
        private Boolean tryAdvanceRight() {
            while (right.moveNext()) {
                position++;
                if (joinCondition.getAsBoolean()) {
                    hadMatch = true;
                    matchedRows.set(position);
                    return true;
                }
            }
            if (right.allLoaded() == false) {
                return false;
            }
            position = -1;
            right.moveToStart();
            if (hadMatch == false) {
                rowData.nullRight();
                activeIt = left;
                return true;
            }
            hadMatch = false;
            return null;
        }

        private boolean moveRightPostNL() {
            while (right.moveNext()) {
                position++;
                if (matchedRows.get(position) == false) {
                    return true;
                }
            }
            return false;
        }
    }
}
