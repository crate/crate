/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;

import io.crate.common.concurrent.RejectableRunnable;

public class PriorityRunnable {

    private PriorityRunnable() {}

    public static PrioritizedRunnable of(Priority priority, String source, Runnable runnable) {
        return runnable instanceof RejectableRunnable rejectableRunnable
            ? new RejectablePriorityRunnable(priority, source, rejectableRunnable)
            : new SimplePriorityRunnable(priority, source, runnable);
    }

    public static class RejectablePriorityRunnable extends PrioritizedRunnable implements RejectableRunnable, WrappedRunnable {

        private final RejectableRunnable runnable;

        private RejectablePriorityRunnable(Priority priority, String source, RejectableRunnable runnable) {
            super(priority, source);
            this.runnable = runnable;
        }

        @Override
        public void onFailure(Exception e) {
            runnable.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            runnable.onRejection(e);
        }

        @Override
        public boolean isForceExecution() {
            return runnable.isForceExecution();
        }

        @Override
        public void doRun() throws Exception {
            runnable.doRun();
        }

        @Override
        public Runnable unwrap() {
            return runnable;
        }
    }

    public static class SimplePriorityRunnable extends PrioritizedRunnable implements WrappedRunnable {

        private final Runnable runnable;

        private SimplePriorityRunnable(Priority priority, String source, Runnable runnable) {
            super(priority, source);
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.run();
        }

        @Override
        public Runnable unwrap() {
            return runnable;
        }
    }
}
