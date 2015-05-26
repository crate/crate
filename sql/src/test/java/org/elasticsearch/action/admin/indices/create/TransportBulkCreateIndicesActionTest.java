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

package org.elasticsearch.action.admin.indices.create;

import com.google.common.collect.ImmutableList;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class TransportBulkCreateIndicesActionTest extends SQLTransportIntegrationTest {

    TransportBulkCreateIndicesAction action;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    public ActionListener<BulkCreateIndicesResponse> responseActionListener;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        action = internalCluster().getInstance(TransportBulkCreateIndicesAction.class);
    }

    @Test
    public void testKillAllCancelsPendingOperations() throws Exception {
        Field pendingOperations = TransportBulkCreateIndicesAction.class.getDeclaredField("pendingOperations");
        pendingOperations.setAccessible(true);
        ArrayDeque<TransportBulkCreateIndicesAction.PendingOperation> operations =
                (ArrayDeque<TransportBulkCreateIndicesAction.PendingOperation>)pendingOperations.get(action);

        operations.add(new TransportBulkCreateIndicesAction.PendingOperation(
                new BulkCreateIndicesRequest(ImmutableList.<String>of()), responseActionListener));
        operations.add(new TransportBulkCreateIndicesAction.PendingOperation(
                new BulkCreateIndicesRequest(ImmutableList.<String>of()), responseActionListener));

        action.killAllCalled(System.nanoTime());

        assertThat(operations.size(), is(0));
    }

    @Test
    public void testCreateBulkIndicesSimple() throws Exception {
        BulkCreateIndicesResponse response = action.execute(
                new BulkCreateIndicesRequest(Arrays.asList("index1", "index2", "index3", "index4"))
        ).actionGet();
        assertThat(response.isAcknowledged(), is(true));
        ensureYellow();

        IndicesExistsResponse indicesExistsResponse = cluster().client().admin()
                .indices().prepareExists("index1", "index2", "index3", "index4")
                .execute().actionGet();
        assertThat(indicesExistsResponse.isExists(), is(true));
    }

    @Test
    public void testCreateBulkIndicesIgnoreExistingSame() throws Exception {
        BulkCreateIndicesResponse response = action.execute(
                new BulkCreateIndicesRequest(Arrays.asList("index1", "index2", "index3", "index1"))
        ).actionGet();
        assertThat(response.isAcknowledged(), is(true));

        IndicesExistsResponse indicesExistsResponse = cluster().client().admin()
                .indices().prepareExists("index1", "index2", "index3")
                .execute().actionGet();
        assertThat(indicesExistsResponse.isExists(), is(true));

        BulkCreateIndicesResponse response2 = action.execute(
                new BulkCreateIndicesRequest(Arrays.asList("index1", "index2", "index3", "index1"))
        ).actionGet();
        assertThat(response2.isAcknowledged(), is(true));
    }

    @Test
    public void testEmpty() throws Exception {
        BulkCreateIndicesResponse response = action.execute(
                new BulkCreateIndicesRequest(ImmutableList.<String>of())).actionGet();
        assertThat(response.isAcknowledged(), is(true));
    }

    @Test
    public void testCreateInvalidName() throws Exception {
        expectedException.expect(InvalidIndexNameException.class);
        expectedException.expectMessage("[invalid/#haha] Invalid index name [invalid/#haha], must not contain the following characters [\\, /, *, ?, \", <, >, |,  , ,]");

        BulkCreateIndicesRequest bulkCreateIndicesRequest = new BulkCreateIndicesRequest(Arrays.asList("valid", "invalid/#haha"));
        try {
            action.execute(bulkCreateIndicesRequest).actionGet();
            fail("no exception thrown");
        } catch (Throwable t) {
            ensureYellow();
            IndicesExistsResponse indicesExistsResponse = cluster().client().admin()
                    .indices().prepareExists("valid")
                    .execute().actionGet();
            assertThat(indicesExistsResponse.isExists(), is(false)); // if one name is invalid no index is created
            throw t;
        }
    }
}
