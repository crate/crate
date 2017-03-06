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

package io.crate.testing;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.operation.projectors.*;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectingRowReceiver implements RowReceiver {

    public final List<Object[]> rows = new ArrayList<>();
    final CompletableFuture<Bucket> resultFuture = new CompletableFuture<>();
    int numFailOrFinish = 0;
    private AtomicInteger numPauseProcessed = new AtomicInteger(0);
    private ResumeHandle resumeable;
    private RepeatHandle repeatHandle;

    public static CollectingRowReceiver withPauseAfter(int pauseAfter) {
        return new PausingReceiver(pauseAfter);
    }

    public static CollectingRowReceiver withLimit(int limit) {
        return new LimitingReceiver(limit);
    }

    public static CollectingRowReceiver withFailure() {
        return new FailingReceiver();
    }

    public static CollectingRowReceiver withFailureOnRepeat() {
        return new FailingOnRepeatReceiver();
    }

    public CollectingRowReceiver() {
    }

    @Override
    public CompletableFuture<Bucket> completionFuture() {
        return resultFuture;
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public Result setNextRow(Row row) {
        rows.add(row.materialize());
        return Result.CONTINUE;
    }

    @Override
    public void kill(Throwable throwable) {
        if (throwable == null) {
            completionFuture().cancel(false);
        } else {
            resultFuture.completeExceptionally(throwable);
        }
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        this.numPauseProcessed.incrementAndGet();
        this.resumeable = resumeable;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        this.repeatHandle = repeatHandle;
        numFailOrFinish++;
        resultFuture.complete(new CollectionBucket(rows));
    }

    public int getNumFailOrFinishCalls() {
        return numFailOrFinish;
    }

    public boolean isFinished() {
        return numFailOrFinish > 0;
    }

    @Override
    public void fail(@Nonnull Throwable throwable) {
        resultFuture.completeExceptionally(throwable);
        numFailOrFinish++;
    }

    public void resumeUpstream(boolean async) {
        resumeable.resume(async);
    }

    public Bucket result() throws Exception {
        return result(TimeValue.timeValueSeconds(10L));
    }

    public Bucket result(TimeValue timeout) throws Exception {
        // always timeout, don't want tests to get stuck
        try {
            return resultFuture.get(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                throw e;
            }
            throw Throwables.propagate(cause);
        } catch (TimeoutException e) {
            TimeoutException timeoutException = new TimeoutException("Didn't receive fail or finish.");
            timeoutException.initCause(e);
            throw timeoutException;
        }
    }

    public void repeatUpstream() {
        repeatHandle.repeat();
    }

    public int numPauseProcessed() {
        return numPauseProcessed.get();
    }

    private static class LimitingReceiver extends CollectingRowReceiver {

        private final int limit;
        private int numRows = 0;

        LimitingReceiver(int limit) {
            this.limit = limit;
        }

        @Override
        public Result setNextRow(Row row) {
            Result result = super.setNextRow(row);
            numRows++;
            //noinspection SimplifiableIfStatement
            if (numRows >= limit) {
                return Result.STOP;
            }
            return result;
        }
    }

    private static class PausingReceiver extends CollectingRowReceiver {

        private final int pauseAfter;
        private int numRows = 0;

        PausingReceiver(int pauseAfter) {
            this.pauseAfter = pauseAfter;
        }

        @Override
        public Result setNextRow(Row row) {
            Result result = super.setNextRow(row);
            numRows++;
            if (numRows == pauseAfter) {
                return Result.PAUSE;
            }
            return result;
        }
    }

    private static class FailingReceiver extends CollectingRowReceiver {

        @Override
        public Result setNextRow(Row row) {
            throw new IllegalStateException("dummy");
        }
    }

    private static class FailingOnRepeatReceiver extends CollectingRowReceiver {

        private boolean invokeFailure = false;

        @Override
        public Result setNextRow(Row row) {
            if (invokeFailure) {
                throw new IllegalStateException("dummy");
            }
            return super.setNextRow(row);
        }

        @Override
        public void repeatUpstream() {
            invokeFailure = true;
            super.repeatUpstream();
        }
    }
}
