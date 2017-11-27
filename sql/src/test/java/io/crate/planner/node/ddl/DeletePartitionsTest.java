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

package io.crate.planner.node.ddl;

import io.crate.analyze.TableDefinitions;
import io.crate.data.RowN;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Test;

import static java.util.Collections.emptyMap;

public class DeletePartitionsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testIndexNameGeneration() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .build();
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?");

        Object[] args1 = {"1395874800000"};
        assertThat(
            plan.getIndices(e.functions(), new RowN(args1), emptyMap()),
            Matchers.containsInAnyOrder(".partitioned.parted_pks.04732cpp6ks3ed1o60o30c1g"));

        Object[] args2 = {"1395961200000"};
        assertThat(
            plan.getIndices(e.functions(), new RowN(args2), emptyMap()),
            Matchers.containsInAnyOrder(".partitioned.parted_pks.04732cpp6ksjcc9i60o30c1g"));
    }

}
