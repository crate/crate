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

package io.crate.data;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PagingTest {

    @Test
    public void testGetNodePageSize() throws Exception {
        double weight = 1.0d / 8.0d;
        int pageSize = Paging.getWeightedPageSize(10_000, weight);
        assertThat(pageSize, is(1875));
    }

    @Test
    public void testSmallLimitWithManyExecutionNodes() throws Exception {
        int pageSize = Paging.getWeightedPageSize(10, 1.0 / 20.0);
        assertThat(pageSize, is(10));
    }

    @Test
    public void testSmallLimitIsUnchanged() throws Exception {
        assertThat(Paging.getWeightedPageSize(10, 1.0d / 4), is(10));
    }
}
