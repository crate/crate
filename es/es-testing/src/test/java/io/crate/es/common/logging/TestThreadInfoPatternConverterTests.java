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

package io.crate.es.common.logging;

import io.crate.es.common.util.concurrent.EsExecutors;
import io.crate.es.test.ESTestCase;
import org.junit.BeforeClass;

public class TestThreadInfoPatternConverterTests extends ESTestCase {
    private static String suiteInfo;

    @BeforeClass
    public static void captureSuiteInfo() {
        suiteInfo = TestThreadInfoPatternConverter.threadInfo(Thread.currentThread().getName());
    }

    public void testThreadInfo() {
        // Threads that are part of a node get the node name
        String nodeName = randomAlphaOfLength(5);
        String threadName = EsExecutors.threadName(nodeName, randomAlphaOfLength(20))
                            + "[T#" + between(0, 1000) + "]";
        assertEquals(nodeName, TestThreadInfoPatternConverter.threadInfo(threadName));

        // Test threads get the test name
        assertEquals(getTestName(), TestThreadInfoPatternConverter.threadInfo(Thread.currentThread().getName()));

        // Suite initialization gets "suite"
        assertEquals("suite", suiteInfo);

        // And stuff that doesn't match anything gets wrapped in [] so we can see it
        String unmatched = randomAlphaOfLength(5);
        assertEquals("[" + unmatched + "]", TestThreadInfoPatternConverter.threadInfo(unmatched));
    }
}
