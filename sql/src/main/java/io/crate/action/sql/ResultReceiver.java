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
import io.crate.data.Row;
import io.crate.executor.Executor;
import io.crate.planner.Plan;

import javax.annotation.Nonnull;

/**
 * A subset / simplified form of {@link io.crate.operation.projectors.RowReceiver}.
 * <p>
 * Used to receive the Result from {@link Executor#execute(Plan, RowReceiver, Row)}
 */
public interface ResultReceiver extends CompletionListenable {

    void setNextRow(Row row);

    void batchFinished();

    /**
     * Called when receiver finished.
     * @param interrupted indicates whether the receiver finished because all results were pushed (false)
     *                    or receiving results was prematurely interrupted (true).
     */
    void allFinished(boolean interrupted);

    void fail(@Nonnull Throwable t);
}
