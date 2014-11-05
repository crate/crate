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

package io.crate.integrationtests;

import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class CountStarIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCountWithPartitionFilter() throws Exception {
        execute("create table t (name string, p string) partitioned by (p) clustered into 2 shards " +
                "with (number_of_replicas = 0)");
        ensureGreen();

        execute("select count(*) from t");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(0L));

        execute("insert into t (name, p) values ('Arthur', 'foo')");
        execute("insert into t (name, p) values ('Trillian', 'foobar')");
        ensureGreen();
        refresh();

        execute("select count(*) from t where p = 'foobar'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(1L));
    }
}
