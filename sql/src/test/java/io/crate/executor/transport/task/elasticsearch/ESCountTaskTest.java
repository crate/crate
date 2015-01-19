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
import io.crate.exceptions.FailedShardsException;
import io.crate.executor.TaskResult;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESCountTaskTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCountResponseOnShardFailures() throws Throwable {
        expectedException.expect(FailedShardsException.class);
        expectedException.expectMessage("query failed on shard 2 ( IndexShardException[[dummy][2] dummy message] ) of table dummy");

        SettableFuture<TaskResult> result = SettableFuture.create();
        ESCountTask.CountResponseListener listener = new ESCountTask.CountResponseListener(result);


        ShardOperationFailedException[] shardFailures = new ShardOperationFailedException[] {
                new DefaultShardOperationFailedException("dummy", 2,
                        new IndexShardException(new ShardId("dummy", 2), "dummy message"))
        };
        CountResponse countResponse = mock(CountResponse.class);
        when(countResponse.getFailedShards()).thenReturn(1);
        when(countResponse.getShardFailures()).thenReturn(shardFailures);

        listener.onResponse(countResponse);

        try {
            result.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}