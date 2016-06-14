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

package io.crate.operation;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.FetchProperties;
import io.crate.executor.TaskResult;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Consumer;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class HandlerOperationsTest extends CrateUnitTest {

    private ScheduledExecutorService scheduledExecutorService;

    @Before
    public void createExecutor() throws Exception {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    @After
    public void shutdownExecutor() throws Exception {
        scheduledExecutorService.shutdown();
        scheduledExecutorService.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    public void testReaperRemovesExpiredContexts() throws Exception {
        final SettableFuture<UUID> killedFuture = SettableFuture.create();
        HandlerOperations handlerOperations = new HandlerOperations(scheduledExecutorService, new Consumer<UUID>() {
            @Override
            public void accept(UUID uuid) {
                killedFuture.set(uuid);
            }
        }, TimeValue.timeValueMillis(1));
        UUID cursorId = UUID.randomUUID();
        ClientPagingReceiver clientPagingReceiver = new ClientPagingReceiver(
            FetchProperties.DEFAULT, SettableFuture.<TaskResult>create(), Collections.<DataType>emptyList());
        handlerOperations.register(cursorId, TimeValue.timeValueNanos(1), clientPagingReceiver);

        assertThat(killedFuture.get(2, TimeUnit.SECONDS), is(cursorId));

        clientPagingReceiver = handlerOperations.get(cursorId, TimeValue.timeValueMinutes(1));
        assertThat(clientPagingReceiver, Matchers.nullValue());
    }

    @Test
    public void testAccessingReceiverRefreshesLastAccess() throws Exception {
        HandlerOperations handlerOperations = new HandlerOperations(scheduledExecutorService, new Consumer<UUID>() {
            @Override
            public void accept(UUID uuid) {
            }
        }, TimeValue.timeValueHours(1));

        ClientPagingReceiver clientPagingReceiver = new ClientPagingReceiver(
            FetchProperties.DEFAULT, SettableFuture.<TaskResult>create(), Collections.<DataType>emptyList());

        UUID cursorId = UUID.randomUUID();
        TimeValue keepAlive = TimeValue.timeValueSeconds(30);
        handlerOperations.register(cursorId, keepAlive, clientPagingReceiver);
        HandlerOperations.Context context = handlerOperations.cursorIdToReceiverMap.values().iterator().next();
        long lastAccess = context.lastAccess;

        handlerOperations.get(cursorId, keepAlive);

        assertThat(context.lastAccess, Matchers.greaterThan(lastAccess));
    }
}
