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

package io.crate.expression.scalar.timestamp;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.metadata.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NowFunctionTest extends ScalarTestCase {

    private static final long EXPECTED_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(EXPECTED_TIMESTAMP);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void test_now_returns_current_timestamp() {
        assertEvaluate("now()", EXPECTED_TIMESTAMP);
    }
}
