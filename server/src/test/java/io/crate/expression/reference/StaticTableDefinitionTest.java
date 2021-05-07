/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference;

import io.crate.user.User;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static io.crate.user.User.CRATE_USER;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StaticTableDefinitionTest {

    private final TransactionContext dummyTxnCtx = TransactionContext.of(
        new SessionSettings("", SearchPath.createSearchPathFrom("")));

    private final User dummyUser = User.of("dummy");

    @Test
    public void testTableDefinitionWithPredicate() throws ExecutionException, InterruptedException {
        List<JobContext> actual = List.of(
            new JobContext(UUID.randomUUID(), "select 1", 1L, CRATE_USER, null),
            new JobContext(UUID.randomUUID(), "select 2", 1L, CRATE_USER, null),
            new JobContext(UUID.randomUUID(), "select 3", 1L, dummyUser, null));

        StaticTableDefinition<JobContext> tableDef = new StaticTableDefinition<>(
            () -> completedFuture(actual),
            Map.of(),
            (user, ctx) -> user.isSuperUser() || ctx.username().equals(user.name()), true);

        Iterable<JobContext> expected = tableDef.retrieveRecords(dummyTxnCtx, CRATE_USER).get();
        assertThat(StreamSupport.stream(expected.spliterator(), false).count(), is(3L));

        expected = tableDef.retrieveRecords(dummyTxnCtx, dummyUser).get();
        assertThat(StreamSupport.stream(expected.spliterator(), false).count(), is(1L));
    }

    @Test
    public void testTableDefinitionWithPredicateOnEmptyRecords()
        throws ExecutionException, InterruptedException {
        StaticTableDefinition<JobContext> tableDef = new StaticTableDefinition<>(
            () -> completedFuture(emptyList()),
            Map.of(),
            (user, ctx) -> user.isSuperUser() || ctx.username().equals(user.name()), true);

        Iterable<JobContext> expected = tableDef.retrieveRecords(dummyTxnCtx, CRATE_USER).get();
        assertThat(StreamSupport.stream(expected.spliterator(), false).count(), is(0L));
    }
}
