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
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.Plan;

import javax.annotation.Nonnull;

/**
 * A subset / simplified form of {@link io.crate.operation.projectors.RowReceiver}.
 *
 * Used to receive the Result from {@link io.crate.executor.transport.TransportExecutor#execute(Plan, ResultReceiver)}
 */
public interface ResultReceiver extends CompletionListenable {

    /**
     * @return true if the receiver wants to receive more rows, otherwise false
     */
    RowReceiver.Result setNextRow(Row row);

    /**
     * Called once an upstream can't provide anymore rows.
     *
     * If this is called {@link #fail(Throwable)} must not be called.
     */
    void finish();

    /**
     * Called once an upstream can't provide anymore rows and exited with an error.
     *
     * If this is called {@link #finish()} must not be called.
     */
    void fail(@Nonnull Throwable t);
}
