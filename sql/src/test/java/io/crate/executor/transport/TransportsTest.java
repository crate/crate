/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportsTest extends CrateUnitTest {


    private ESLogger logger;
    private String logLevel;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // mute WARN logging for this test
        logger = Loggers.getLogger(Transports.class);
        logLevel = logger.getLevel();
        logger.setLevel("ERROR");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        logger.setLevel(logLevel);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEsRejectedExecutionExceptionCallsFailOnListener() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        final String executorName = "dummy";
        when(threadPool.executor(executorName)).thenReturn(new Executor() {
            @Override
            public void execute(@Nonnull Runnable command) {
                throw new EsRejectedExecutionException();
            }
        });
        Transports transports = new Transports(new NoopClusterService(), mock(TransportService.class), threadPool);

        final SettableFuture<Boolean> failCalled = SettableFuture.create();
        ActionListener listener = new ActionListener() {
            @Override
            public void onResponse(Object o) {
                failCalled.set(false);
            }

            @Override
            public void onFailure(Throwable e) {
                failCalled.set(true);

            }
        };
        NodeAction nodeAction = mock(NodeAction.class);
        when(nodeAction.executorName()).thenReturn(executorName);
        transports.executeLocalOrWithTransport(nodeAction, "noop_id", mock(TransportRequest.class), listener,
                new DefaultTransportResponseHandler(listener) {
            @Override
            public TransportResponse newInstance() {
                return mock(TransportResponse.class);
            }
        });

        Boolean result = failCalled.get(100, TimeUnit.MILLISECONDS);
        assertThat(result, is(true));
    }
}