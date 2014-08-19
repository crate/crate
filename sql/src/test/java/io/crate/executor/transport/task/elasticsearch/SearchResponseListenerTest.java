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
import io.crate.executor.QueryResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchShardTarget;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;

public class SearchResponseListenerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMissingShardsException() throws Throwable {
        expectedException.expect(FailedShardsException.class);

        SettableFuture<QueryResult> result = SettableFuture.create();
        ESSearchTask.SearchResponseListener listener = new ESSearchTask.SearchResponseListener(
                result, null, 4
        );

        listener.onResponse(new SearchResponse(
                null,
                "dummyScrollId",
                10,
                9,
                200L,
                new ShardSearchFailure[] {
                        new ShardSearchFailure("dummy reason", new SearchShardTarget("nodeX", "dummyIndex", 2))
                }
        ));

        try {
            result.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}