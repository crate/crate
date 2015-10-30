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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.repositories.RepositoryException;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PutRepositoryResponseActionListenerTest extends CrateUnitTest {


    @Test
    public void testOnFailure() throws Throwable {
        expectedException.expect(RepositoryException.class);
        expectedException.expectMessage("[foo] failed: [foo] missing location");

        SettableFuture<Long> future = SettableFuture.create();
        RepositoryDDLDispatcher.PutRepositoryResponseActionListener listener =
                new RepositoryDDLDispatcher.PutRepositoryResponseActionListener(future);

        listener.onFailure(new RepositoryException("foo", "failed", new CreationException(ImmutableList.of(
                new Message(Collections.<Object>singletonList(10),
                        "creation error", new RepositoryException("foo", "missing location"))
        ))));

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}