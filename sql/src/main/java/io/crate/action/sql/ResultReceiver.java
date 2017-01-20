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

package io.crate.action.sql;

import io.crate.concurrent.CompletionListenable;
import io.crate.core.collections.Row;
import io.crate.executor.Executor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.Plan;

import javax.annotation.Nonnull;

/**
 * A subset / simplified form of {@link io.crate.operation.projectors.RowReceiver}.
 * <p>
 * Used to receive the Result from {@link Executor#execute(Plan, RowReceiver, Row)}
 */
public interface ResultReceiver extends CompletionListenable {

    /**
     * Feed the downstream with the next input row. Note thate the row might be a mutable shared object
     * it is only guaranteed to stay valid as long this method does not return.
     *
     * @param row the row handle to be processed
     *
     */
    void setNextRow(Row row);

    /**
     * Called from the upstream in order to notify the receiver that a batch of data has finished. It is the responsibility
     * of the upstream to eventually call {@link #allFinished()} once it is getting resumed. How this resume is working
     * is up to the implementation and not part of this interface.
     */
    void batchFinished();

    /**
     * Called by an upstream to notify that there will be no more data to process. After calling this method it is no
     * longer valid to call any method on this interface.
     */
    void allFinished();

    /**
     * Is called from the upstream in case of an upstream failure to indicate that it aborts emitting data due to
     * a failure. After calling this method it is no longer valid to call any method on this interface.
     *
     * @param throwable the cause of the failure
     *                  <p>
     *                  NOTE: This method must not throw any exceptions!
     */
    void fail(@Nonnull Throwable throwable);
}
