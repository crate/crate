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

package io.crate.planner;

import com.google.common.collect.Lists;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.symbol.ParameterSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.DeleteById;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingRowConsumer;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.isDocKey;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class DeletePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .build();
    }

    @Test
    public void testDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id = 1");
        assertThat(plan.table().ident().name(), is("users"));
        assertThat(plan.docKeys().size(), is(1));
        assertThat(plan.docKeys().getOnlyKey(), isDocKey(1L));
    }

    @Test
    public void testBulkDeletePartitionedTable() throws Exception {
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?");
        List<Symbol> partitionSymbols = plan.partitions().get(0);
        assertThat(partitionSymbols, contains(instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        DeleteById plan = e.plan("delete from users where id in (1, 2)");
        assertThat(plan.docKeys().size(), is(2));
        List<String> docKeys = Lists.newArrayList(plan.docKeys())
            .stream()
            .map(x -> x.getId(e.functions(), Row.EMPTY, Collections.emptyMap()))
            .collect(Collectors.toList());

        assertThat(docKeys, Matchers.containsInAnyOrder("1", "2"));
    }

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        Plan plan = e.plan("delete from users where _version is null");

        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage(VersionInvalidException.ERROR_MSG);
        plan.execute(
            mock(DependencyCarrier.class),
            e.getPlannerContext(clusterService.state()),
            new TestingRowConsumer(),
            Row.EMPTY,
            Collections.emptyMap()
        );
    }
}
