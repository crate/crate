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

import io.crate.exceptions.SQLExceptions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RetryListener<TReq, TResp> implements ActionListener<TResp> {

    private final ScheduledExecutorService scheduler;
    private final ActionListener<TResp> delegate;
    private final Iterator<TimeValue> delay;
    private final Runnable retryCommand;

    public RetryListener(ScheduledExecutorService scheduler,
                         Consumer<ActionListener<TResp>> command,
                         ActionListener<TResp> delegate,
                         Iterable<TimeValue> backOffPolicy) {
        this.scheduler = scheduler;
        this.delegate = delegate;
        this.delay = backOffPolicy.iterator();
        this.retryCommand = () -> command.accept(this);
    }

    @Override
    public void onResponse(TResp response) {
        delegate.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        Throwable throwable = SQLExceptions.unwrap(e);
        if (throwable instanceof EsRejectedExecutionException && delay.hasNext()) {
            TimeValue currentDelay = delay.next();
            scheduler.schedule(retryCommand, currentDelay.millis(), TimeUnit.MILLISECONDS);
        } else {
            delegate.onFailure(e);
        }
    }
}
