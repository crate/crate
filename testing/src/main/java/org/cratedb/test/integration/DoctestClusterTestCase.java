/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.test.integration;

import org.junit.After;
import org.junit.Before;

/**
 * Doctest Testcase that starts a crate cluster in the background
 */
public abstract class DoctestClusterTestCase extends CrateIntegrationTest {

    private DoctestRunner doctestRunner = new DoctestRunner();


    @Before
    public void setupDoctests() throws Exception {
        doctestRunner.setUp();
    }

    @After
    public void tearDownDoctests() throws Exception {
        doctestRunner.tearDown();
    }

    public void execDocFile(String filePath, Class<?> klass) {
        doctestRunner.execDocFile(filePath, klass);
    }
}
