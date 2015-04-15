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

import io.crate.Streamer;
import io.crate.operation.PageDownstream;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static org.mockito.Mockito.mock;

public class JobExecutionContextTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testInitializingFinalMergeForTheSameExecutionNodeThrowsAnError() throws Exception {
        expectedException.expect(IllegalStateException.class);

        JobExecutionContext context = new JobExecutionContext(UUID.randomUUID(), 100L);
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 1);
        context.setPageDownstreamContext(1, pageDownstreamContext);
        context.setPageDownstreamContext(1, pageDownstreamContext);
    }
}