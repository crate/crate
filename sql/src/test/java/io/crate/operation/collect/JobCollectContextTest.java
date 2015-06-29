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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.planner.node.dql.CollectNode;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * This class requires PowerMock in order to mock the final {@link SearchContext#close} method.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CrateSearchContext.class)
public class JobCollectContextTest extends CrateUnitTest {

    private JobCollectContext jobCollectContext;

    private RamAccountingContext ramAccountingContext = mock(RamAccountingContext.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobCollectContext = new JobCollectContext(
                UUID.randomUUID(),
                mock(CollectNode.class),
                mock(CollectOperation.class), ramAccountingContext, new CollectingProjector());
    }

    @Test
    public void testAddingSameContextTwice() throws Exception {
        CrateSearchContext mock1 = mock(CrateSearchContext.class);
        CrateSearchContext mock2 = mock(CrateSearchContext.class);
        try {
            jobCollectContext.addContext(1, mock1);
            jobCollectContext.addContext(1, mock2);

            assertFalse(true); // second addContext call should have raised an exception
        } catch (IllegalArgumentException e) {
            verify(mock1, times(1)).close();
            verify(mock2, times(1)).close();
        }
    }

    @Test
    public void testCloseClosesSearchContexts() throws Exception {
        CrateSearchContext mock1 = mock(CrateSearchContext.class);
        CrateSearchContext mock2 = mock(CrateSearchContext.class);

        jobCollectContext.addContext(1, mock1);
        jobCollectContext.addContext(2, mock2);

        jobCollectContext.close();

        verify(mock1, times(1)).close();
        verify(mock2, times(1)).close();
        verify(ramAccountingContext, times(1)).close();
    }
}
