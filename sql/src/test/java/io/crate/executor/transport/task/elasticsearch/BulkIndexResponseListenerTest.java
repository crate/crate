/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class BulkIndexResponseListenerTest {

    @Test
    public void testRetryLogic() throws Throwable {

        /**
         * simulate a bulkRequest where one response contains a EsRejected Failure
         */

        TransportBulkAction bulkAction = mock(TransportBulkAction.class);
        ThreadPool threadPool = new ThreadPool(ImmutableSettings.EMPTY, null);

        SettableFuture<Object[][]> result = SettableFuture.create();
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request1 = new IndexRequest("dummy", "default", "1");
        request1.source("foo", "bar");
        IndexRequest request2 = new IndexRequest("dummy", "default", "2");
        request2.source("foo", "bla");
        bulkRequest.add(request1);
        bulkRequest.add(request2);
        ESBulkIndexTask.BulkIndexResponseListener responseListener = new ESBulkIndexTask.BulkIndexResponseListener(
                threadPool, bulkAction, bulkRequest, result, 0
        );

        // this is the retry with only 1 requests instead of both
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                BulkRequest request = (BulkRequest) invocation.getArguments()[0];
                assertEquals(1, request.requests().size());

                ESBulkIndexTask.BulkIndexResponseListener listener =
                        (ESBulkIndexTask.BulkIndexResponseListener) invocation.getArguments()[1];

                listener.onResponse(new BulkResponse(
                        new BulkItemResponse[]{
                                new BulkItemResponse(1, IndexRequest.OpType.CREATE.name(),
                                        new IndexResponse("dummy", "default", "2", 1L, true))
                        }, 100L));
                return null;
            }
        }).when(bulkAction).execute(any(BulkRequest.class), any(ActionListener.class));


        BulkResponse responses = new BulkResponse(
                new BulkItemResponse[] {
                        new BulkItemResponse(0, IndexRequest.OpType.CREATE.name(),
                                new IndexResponse("dummy", "default", "1", 1L, true)),
                        new BulkItemResponse(1, IndexRequest.OpType.CREATE.name(),
                                new BulkItemResponse.Failure("dummy", "default", "2", new EsRejectedExecutionException()))
                }, 200L
        );
        responseListener.onResponse(responses);


        Object[][] objects = result.get();
        assertThat((Long)objects[0][0], is(2L));
    }
}