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
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.Plan;

import javax.annotation.Nonnull;

/**
 * A subset / simplified form of {@link io.crate.operation.projectors.RowReceiver}.
 * <p>
 * Used to receive the Result from {@link io.crate.executor.transport.TransportExecutor#execute(Plan, RowReceiver)}
 */
public interface ResultReceiver extends CompletionListenable {

    ResultReceiver NO_OP = new ResultReceiver() {

        CompletionListener listener;

        @Override
        public void setNextRow(Row row) {
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished() {
            listener.onSuccess(null);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(t);
        }

        @Override
        public void addListener(CompletionListener listener) {
            this.listener = CompletionMultiListener.merge(this.listener, listener);
        }
    };

    void setNextRow(Row row);

    void batchFinished();

    void allFinished();

    void fail(@Nonnull Throwable t);
}
