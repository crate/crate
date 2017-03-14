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

package io.crate.operation.projectors;

import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchConsumer;
import io.crate.data.Killable;
import io.crate.data.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface RowReceiver extends CompletionListenable, Killable {

    enum Result {
        CONTINUE,
        PAUSE,
        STOP
    }

    /**
     * Future that is triggered once a RowReceiver finishes execution.
     */
    @Override
    CompletableFuture<?> completionFuture();

    /**
     * Feed the downstream with the next input row.
     * <p>
     * If setNextRow returns PAUSE a upstream must call {@link #pauseProcessed(ResumeHandle)} and immediately return afterwards.
     * A Upstream MUST NOT make any other calls until it receives a resume call.
     * <p>
     * If setNextRow returns STOP a upstream has to call finish/fail
     *
     * @param row the next row - the row is usually a shared object and the instances content change after the
     *            setNextRow call.
     * @return false if the downstream does not need any more rows, true otherwise.
     */
    Result setNextRow(Row row);

    /**
     * Called by an upstream after it has received PAUSE from {@link #setNextRow(Row)}
     * The upstream suspends execution immediately afterwards
     *
     * @param resumeable can be used to resume the upstream
     */
    void pauseProcessed(ResumeHandle resumeable);

    /**
     * Called from the upstream to indicate that all rows are sent.
     * <p>
     * NOTE: This method must not throw any exceptions!
     */
    void finish(RepeatHandle repeatable);

    /**
     * Is called from the upstream in case of a failure.
     * This is the equivalent to finish and indicates that the upstream is finished
     *
     * @param throwable the cause of the fail
     *                  <p>
     *                  NOTE: This method must not throw any exceptions!
     */
    void fail(Throwable throwable);

    /**
     * kill a RowReceiver to stop it's execution.
     * kill can be called from a different thread and can be called after/during finish/fail operations
     * <p>
     * If a RowReceiver doesn't delegate the kill to another RowReceiver the rowReceiver has to return false on the
     * next setNextRow call in order to stop collect operations.
     */
    void kill(@Nonnull Throwable throwable);

    /**
     * specifies which requirements a downstream requires from an upstream in order to work correctly.
     * <p>
     * This can be used to switch to optimized implementations if something isn't/is requirement
     */
    Set<Requirement> requirements();

    /**
     * @return a BatchConsumer implementation of the RowReceiver. Null if none is available.
     *         This can be used for migration purposes until the RowReceiver interface is removed.
     */
    @Nullable
    default BatchConsumer asConsumer() {
        return null;
    }
}
