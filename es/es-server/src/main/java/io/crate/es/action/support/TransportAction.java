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

package io.crate.es.action.support;

import io.crate.es.action.ActionFuture;
import io.crate.es.action.ActionListener;
import io.crate.es.action.ActionRequest;
import io.crate.es.action.ActionRequestValidationException;
import io.crate.es.action.ActionResponse;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Settings;
import io.crate.es.tasks.Task;
import io.crate.es.tasks.TaskManager;
import io.crate.es.threadpool.ThreadPool;

import static io.crate.es.action.support.PlainActionFuture.newFuture;

public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final ThreadPool threadPool;
    protected final String actionName;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    protected final TaskManager taskManager;

    protected TransportAction(Settings settings, String actionName, ThreadPool threadPool,
                              IndexNameExpressionResolver indexNameExpressionResolver, TaskManager taskManager) {
        super(settings);
        this.threadPool = threadPool;
        this.actionName = actionName;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.taskManager = taskManager;
    }

    public final ActionFuture<Response> execute(Request request) {
        PlainActionFuture<Response> future = newFuture();
        execute(request, future);
        return future;
    }

    /**
     * Use this method when the transport action call should result in creation of a new task associated with the call.
     *
     * This is a typical behavior.
     */
    public final Task execute(Request request, ActionListener<Response> listener) {
        /*
         * While this version of execute could delegate to the TaskListener
         * version of execute that'd add yet another layer of wrapping on the
         * listener and prevent us from using the listener bare if there isn't a
         * task. That just seems like too many objects. Thus the two versions of
         * this method.
         */
        Task task = taskManager.register("transport", actionName, request);
        if (task == null) {
            execute(null, request, listener);
        } else {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    taskManager.unregister(task);
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    taskManager.unregister(task);
                    listener.onFailure(e);
                }
            });
        }
        return task;
    }

    /**
     * Use this method when the transport action should continue to run in the context of the current task
     */
    public final void execute(Task task, Request request, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        try {
            doExecute(task, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        doExecute(request, listener);
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);
}
