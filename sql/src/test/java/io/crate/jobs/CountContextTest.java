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

package io.crate.jobs;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.count.CountOperation;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.*;

public class CountContextTest extends CrateUnitTest {

    @Test
    public void testClose() throws Exception {

        SettableFuture<Long> future = SettableFuture.create();

        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(future);
        RowDownstream rowDownstream = mock(RowDownstream.class);
        when(rowDownstream.registerUpstream(any(RowUpstream.class))).thenReturn(mock(RowDownstreamHandle.class));

        CountContext countContext = new CountContext(countOperation, rowDownstream, null, WhereClause.MATCH_ALL);
        ContextCallback callback = mock(ContextCallback.class);
        countContext.addCallback(callback);
        countContext.start();
        future.set(1L);
        verify(callback, times(1)).onClose();

        // on error
        countContext = new CountContext(countOperation, rowDownstream, null, WhereClause.MATCH_ALL);
        callback = mock(ContextCallback.class);
        countContext.addCallback(callback);
        countContext.start();
        future.setException(new UnknownUpstreamFailure());
        verify(callback, times(1)).onClose();
    }
}
