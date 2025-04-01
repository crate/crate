/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import io.crate.common.exceptions.Exceptions;

/**
 * An extension to runnable.
 **/
public interface RejectableRunnable extends Runnable {

    /**
     * Should the runnable force its execution in case it gets rejected?
     */
    default boolean isForceExecution() {
        return false;
    }

    default void run() {
        try {
            doRun();
        } catch (Exception t) {
            onFailure(t);
        } finally {
            onAfter();
        }
    }

    /**
     * This method is called in a finally block after successful execution
     * or on a rejection.
     */
    default void onAfter() {
        // nothing by default
    }

    /**
     * This method is invoked for all exception thrown by {@link #doRun()}
     */
    default void onFailure(Exception e) {
        Exceptions.rethrowUnchecked(e);
    }

    /**
     * This should be executed if the thread-pool executing this action rejected the execution.
     * The default implementation forwards to {@link #onFailure(Exception)}
     */
    default void onRejection(Exception e) {
        onFailure(e);
    }

    /**
     * This method has the same semantics as {@link Runnable#run()}
     * @throws InterruptedException if the run method throws an InterruptedException
     */
    void doRun() throws Exception;
}
