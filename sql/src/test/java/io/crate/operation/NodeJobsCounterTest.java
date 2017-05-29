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

package io.crate.operation;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class NodeJobsCounterTest extends CrateUnitTest {

    private NodeJobsCounter nodeJobsCounter;

    @Before
    public void setupJobsTracker() {
        nodeJobsCounter = new NodeJobsCounter();
    }

    @Test
    public void testIncrementJobsUpdatesStats() {
        nodeJobsCounter.incJobsCount("node1");
        assertThat(nodeJobsCounter.getInProgressJobsForNode("node1"), is(1L));
    }

    @Test
    public void testIncrementRetriesUpdatesStats() {
        nodeJobsCounter.incRetriesCount("node1");
        assertThat(nodeJobsCounter.getParkedRetriesForNode("node1"), is(1L));
    }

    @Test
    public void testDecrementJobsForSpecifiedNodeOnly() {
        nodeJobsCounter.incJobsCount("node1");
        nodeJobsCounter.incJobsCount("node2");

        nodeJobsCounter.decJobsCount("node1");

        assertThat(nodeJobsCounter.getInProgressJobsForNode("node1"), is(0L));
        assertThat(nodeJobsCounter.getInProgressJobsForNode("node2"), is(1L));
    }

    @Test
    public void testDecrementRetriesForSpecifiedNodeOnly() {
        nodeJobsCounter.incRetriesCount("node1");
        nodeJobsCounter.incRetriesCount("node2");

        nodeJobsCounter.decRetriesCount("node1");

        assertThat(nodeJobsCounter.getParkedRetriesForNode("node1"), is(0L));
        assertThat(nodeJobsCounter.getParkedRetriesForNode("node2"), is(1L));
    }

    @Test
    public void testIncrementJobsNullUpdatesStatsForUnidentifiedNode() {
        try {
            nodeJobsCounter.incJobsCount(null);
        } catch (Throwable e) {
            fail("Did not expect exception when attempting to register a job against null node but got: " +
                 e.getMessage());
        }
        long jobsForNullNode = nodeJobsCounter.getInProgressJobsForNode(null);
        assertThat(jobsForNullNode, is(1L));
    }

    @Test
    public void testIncrementRetriesNullUpdatesStatsForUnidentifiedNode() {
        try {
            nodeJobsCounter.incRetriesCount(null);
        } catch (Throwable e) {
            fail("Did not expect exception when attempting register a retry against null node but got: " +
                 e.getMessage());
        }
        long jobsForNullNode = nodeJobsCounter.getParkedRetriesForNode(null);
        assertThat(jobsForNullNode, is(1L));
    }

    @Test
    public void testDecrementJobsForNullDoesntFail() {
        try {
            nodeJobsCounter.decJobsCount(null);
        } catch (Exception e) {
            fail("Did not expect unregistering a job for a null node to fail but got: " + e.getMessage());
        }
    }

    @Test
    public void testDecrementRetriesForNullDoesntFail() {
        try {
            nodeJobsCounter.decRetriesCount(null);
        } catch (Exception e) {
            fail("Did not expect unregistering a job for a null node to fail but got: " + e.getMessage());
        }
    }
}
