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

package io.crate.execution;

import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.operation.join.CombinedRow;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class NLDataSource implements DataSource {


    private enum State {
        START,
        LEFT_DATA_MISSING,
        RIGHT_DATA_MISSING,
        FINISHED;

        State transition(boolean lastLeft, boolean lastRight) {
            switch (this) {
                case START:
                case RIGHT_DATA_MISSING:
                    if (lastRight) {
                        if (lastLeft) {
                            return FINISHED;
                        }
                        return LEFT_DATA_MISSING;
                    }
                    return RIGHT_DATA_MISSING;
                case LEFT_DATA_MISSING:
                    if (lastLeft) {
                        return FINISHED;
                    }
                    return LEFT_DATA_MISSING;
                case FINISHED:
                    return FINISHED;
            }
            throw new AssertionError("Invalid case: " + this);
        }
    }

    private final DataSource left;
    private final DataSource right;

    private State currentState = State.START;
    private Page currentLeftPage;
    private Page currentRightPage;
    private Bucket fullRightBucket;
    private Row leftRow;

    public NLDataSource(DataSource left, DataSource right) {
        this.left = left;
        this.right = right;
    }

    private Page createFirstPage(Page leftPage, Page rightPage) {
        currentLeftPage = leftPage;
        currentRightPage = rightPage;
        fullRightBucket = rightPage.bucket();
        currentState = currentState.transition(leftPage.isLast(), rightPage.isLast());

        Bucket bucket = leftPage.bucket();
        if (bucket.size() == 0) {
            return Page.LAST;
        }
        if (currentState == State.RIGHT_DATA_MISSING) {
            Iterator<Row> leftIt = bucket.iterator();
            leftRow = leftIt.next();
            bucket = new RightLoop(leftRow, rightPage.bucket());
        } else {
            bucket = new NestedLoop(bucket, rightPage.bucket());
        }
        return new StaticPage(bucket, currentState == State.FINISHED);
    }

    private Page nextLeft(Page page) {
        currentLeftPage = page;
        currentState = currentState.transition(page.isLast(), currentRightPage.isLast());

        return new StaticPage(
            new NestedLoop(page.bucket(), fullRightBucket),
            currentState == State.FINISHED
        );
    }

    private Page nextRight(Page page) {
        Objects.requireNonNull(leftRow, "leftRow must be present");
        currentRightPage = page;
        fullRightBucket = Buckets.concat(fullRightBucket, page.bucket());
        currentState = currentState.transition(currentLeftPage.isLast(), page.isLast());

        RightLoop rightLoop = new RightLoop(leftRow, page.bucket());
        if (currentState == State.RIGHT_DATA_MISSING) {
            return new StaticPage(rightLoop, false);
        }

        Bucket bucket = Buckets.concat(
            rightLoop,
            new NestedLoop(Buckets.skip(currentLeftPage.bucket(), 1), fullRightBucket)
        );
        return new StaticPage(bucket, currentState == State.FINISHED);
    }

    @Override
    public CompletableFuture<Page> fetch() {
        switch (currentState) {
            case START:
                CompletableFuture<Page> leftFuture = left.fetch();
                CompletableFuture<Page> rightFuture = right.fetch();
                return leftFuture.thenCombine(rightFuture, this::createFirstPage);
            case LEFT_DATA_MISSING:
                return left.fetch().thenApply(this::nextLeft);
            case RIGHT_DATA_MISSING:
                return right.fetch().thenApply(this::nextRight);
            case FINISHED:
                return CompletableFuture.completedFuture(Page.LAST);
        }
        throw new IllegalStateException("Unknown state: " + currentState);
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    private static class NestedLoop implements Bucket {

        private final Bucket left;
        private final Bucket right;

        private NestedLoop(Bucket left, Bucket right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int size() {
            return left.size() * right.size();
        }

        @Override
        public Iterator<Row> iterator() {
            if (size() == 0) {
                return Collections.emptyIterator();
            }
            return new Iterator<Row>() {

                private final Iterator<Row> leftIt = left.iterator();
                private final CombinedRow combinedRow = new CombinedRow();

                private Iterator<Row> rightIt = right.iterator();
                private Row leftRow;

                @Override
                public boolean hasNext() {
                    return leftIt.hasNext() || rightIt.hasNext();
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("Iterator has no more elements");
                    }

                    if (leftRow == null) {
                        leftRow = leftIt.next();
                        combinedRow.outerRow = leftRow;
                    }
                    combinedRow.innerRow = rightIt.next();
                    if (!rightIt.hasNext() && leftIt.hasNext()) {
                        rightIt = right.iterator();
                        leftRow = null;
                    }
                    return combinedRow;
                }
            };
        }
    }

    private static class RightLoop implements Bucket {

        private final Row leftRow;
        private final Bucket rightBucket;

        RightLoop(Row leftRow, Bucket rightBucket) {
            Objects.requireNonNull(leftRow, "leftRow must not be null");
            this.leftRow = leftRow;
            this.rightBucket = rightBucket;
        }

        @Override
        public int size() {
            return rightBucket.size();
        }

        @Override
        public Iterator<Row> iterator() {
            final CombinedRow row = new CombinedRow();
            row.outerRow = leftRow;
            return new Iterator<Row>() {

                private final Iterator<Row> rightIt = rightBucket.iterator();

                @Override
                public boolean hasNext() {
                    return rightIt.hasNext();
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("Iterator exhausted");
                    }
                    row.innerRow = rightIt.next();
                    return row;
                }
            };
        }
    }
}
