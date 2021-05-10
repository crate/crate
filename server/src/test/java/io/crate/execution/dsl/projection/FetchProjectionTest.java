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

package io.crate.execution.dsl.projection;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FetchProjectionTest {

    @Test
    public void testMaxFetchSize() throws Exception {
        assertThat(FetchProjection.maxFetchSize(56), is(2_000));
        assertThat(FetchProjection.maxFetchSize(256), is(30_000));
        assertThat(FetchProjection.maxFetchSize(512), is(65_840));
        assertThat(FetchProjection.maxFetchSize(1024), is(137_520));
        assertThat(FetchProjection.maxFetchSize(2048), is(280_880));
        assertThat(FetchProjection.maxFetchSize(4096), is(500_000));
    }

    @Test
    public void testBoundedFetchSize() throws Exception {
        assertThat(FetchProjection.boundedFetchSize(0, 65_840), is(65_840));
    }
}
