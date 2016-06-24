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
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.action.sql.ResultReceiver;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CollectingRowReceiver implements RowReceiver, ResultReceiver {

    public final List<Object[]> rows = new ArrayList<>();
    protected final SettableFuture<Bucket> resultFuture = SettableFuture.create();
    protected int numFailOrFinish = 0;
    protected RowUpstream upstream;
    private CompletionListener listener = CompletionListener.NO_OP;

    public static CollectingRowReceiver withPauseAfter(int pauseAfter) {
        return new PausingReceiver(pauseAfter);
    }

    public static CollectingRowReceiver withLimit(int limit) {
        return new LimitingReceiver(limit);
    }

    public CollectingRowReceiver() {
    }

    @Override
    public void prepare() {
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
        this.upstream = rowUpstream;
    }

    @Override
    public boolean setNextRow(Row row) {
        rows.add(row.materialize());
        return true;
    }

    @Override
    public void kill(Throwable throwable) {
        resultFuture.setException(throwable);
        listener.onFailure(throwable);
    }

    @Override
    public void finish() {
        resultFuture.set(new CollectionBucket(rows));
        listener.onSuccess(null);
        numFailOrFinish++;
    }

    public int getNumFailOrFinishCalls() {
        return numFailOrFinish;
    }

    public boolean isFinished() {
        return numFailOrFinish > 0;
    }

    @Override
    public void fail(@Nonnull Throwable throwable) {
        resultFuture.setException(throwable);
        listener.onFailure(throwable);
        numFailOrFinish++;
    }

    public void resumeUpstream(boolean async) {
        upstream.resume(async);
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
            TimeoutException timeoutException = new TimeoutException(
                    "Didn't receive fail or finish. Upstream was \"" + upstream + "\"");
            timeoutException.initCause(e);
            throw timeoutException;
        }
    }

    public void repeatUpstream() {
        upstream.repeat();
    }

    @Override
    public void addListener(CompletionListener listener) {
        this.listener = CompletionMultiListener.merge(this.listener, listener);
    }

    private static class LimitingReceiver extends CollectingRowReceiver {

        private final int limit;
        private int numRows = 0;

        public LimitingReceiver(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean setNextRow(Row row) {
            boolean wantsMore = super.setNextRow(row);
            numRows++;
            //noinspection SimplifiableIfStatement
            if (numRows >= limit) {
                return false;
            }
            return wantsMore;
        }
    }

    private static class PausingReceiver extends CollectingRowReceiver {

        private final int pauseAfter;
        private int numRows = 0;

        public PausingReceiver(int pauseAfter) {
            this.pauseAfter = pauseAfter;
        }

        @Override
        public boolean setNextRow(Row row) {
            boolean wantsMore = super.setNextRow(row);
            numRows++;
            if (numRows == pauseAfter) {
                upstream.pause();
                return true;
            }
            return wantsMore;
        }
    }
}
