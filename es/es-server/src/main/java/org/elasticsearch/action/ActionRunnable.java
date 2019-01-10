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

package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;

/**
 * Base class for {@link Runnable}s that need to call {@link ActionListener#onFailure(Exception)} in case an uncaught
 * exception or error is thrown while the actual action is run.
 */
public abstract class ActionRunnable<Response> extends AbstractRunnable {

    protected final ActionListener<Response> listener;

    public ActionRunnable(ActionListener<Response> listener) {
        this.listener = listener;
    }

    /**
     * Calls the action listeners {@link ActionListener#onFailure(Exception)} method with the given exception.
     * This method is invoked for all exception thrown by {@link #doRun()}
     */
    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }
}
