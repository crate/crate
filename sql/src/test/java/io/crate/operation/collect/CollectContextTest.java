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

package io.crate.operation.collect;

import com.google.common.base.Function;
import io.crate.executor.transport.CollectContextService;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CollectContextTest {

    private CollectContextService collectContextService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        Releasables.close(collectContextService);
    }

    private static final Function<IndexReader, SearchContext> CONTEXT_FUNCTION =  new Function<IndexReader, SearchContext>() {

        @Nullable
        @Override
        public SearchContext apply(IndexReader input) {
            return new TestSearchContext();
        }
    };

    @Test
    public void testSameForSameArgs() throws Throwable {
        collectContextService = new CollectContextService();
        UUID jobId = UUID.randomUUID();
        SearchContext ctx1 = collectContextService.getOrCreateContext(jobId, 0, CONTEXT_FUNCTION);
        SearchContext ctx2 = collectContextService.getOrCreateContext(jobId, 0, CONTEXT_FUNCTION);
        assertThat(ctx1, is(ctx2));
        SearchContext ctx3 = collectContextService.getOrCreateContext(UUID.randomUUID(), 0, CONTEXT_FUNCTION);
        assertThat(ctx3, is(not(ctx1)));

        SearchContext ctx4 = collectContextService.getOrCreateContext(jobId, 1, CONTEXT_FUNCTION);
        assertThat(ctx4, is(not(ctx1)));
    }
}
