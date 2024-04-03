/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

@IntegTestCase.ClusterScope(numDataNodes = 4, supportsDedicatedMasters = false, numClientNodes = 0)
public class PendingWritesReproductionTest extends IntegTestCase {

    @Test
    @Repeat(iterations = 100)
    @TestLogging("org.elasticsearch.indices.recovery:TRACE, org.elasticsearch.index.translog:TRACE")
    public void reproduction() throws Exception {
        execute("create table doc.t1 (id int) with(number_of_replicas = 3,  \"write.wait_for_active_shards\" = 'ALL')");
        execute("insert into doc.t1 (id) select b from generate_series(1,10000) a(b)");
    }

}
