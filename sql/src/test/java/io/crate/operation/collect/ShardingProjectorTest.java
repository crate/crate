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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class ShardingProjectorTest extends CrateUnitTest {

    private final static ColumnIdent ID_IDENT = new ColumnIdent("_id");

    private Row row(Object ... cells){
        if (cells==null){
            cells = new Object[]{null};
        }
        return new RowN(cells);
    }

    @Test
    public void testNoPrimaryKeyNoRouting() {
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), ImmutableList.<Symbol>of(), null);
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row());

        // auto-generated id, no special routing
        assertNotNull(shardingProjector.id());
        assertNull(shardingProjector.routing());
    }

    @Test
    public void testNoPrimaryKeyButRouting() {
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), ImmutableList.<Symbol>of(), new InputColumn(1));
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(1, "hoschi"));

        // auto-generated id, special routing
        assertNotNull(shardingProjector.id());
        assertThat(shardingProjector.routing(), is("hoschi"));
    }

    @Test
    public void testPrimaryKeyNoRouting() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(0), new InputColumn(1));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), primaryKeySymbols, null);
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(1, "hoschi"));

        // compound encoded id, no special routing
        assertThat(shardingProjector.id(), is("AgExBmhvc2NoaQ=="));
        assertNull(shardingProjector.routing());
    }

    @Test
    public void testPrimaryKeyAndRouting() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(1), new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(1, "hoschi"));

        // compound encoded id, special routing
        assertThat(shardingProjector.id(), is("AgZob3NjaGkBMQ=="));
        assertThat(shardingProjector.routing(), is("hoschi"));
    }

    @Test
    public void testMultipleRows() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(1), new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection(mock(ExecutionState.class));

        shardingProjector.setNextRow(row(1, "hoschi"));
        assertThat(shardingProjector.id(), is("AgZob3NjaGkBMQ=="));
        assertThat(shardingProjector.routing(), is("hoschi"));

        shardingProjector.setNextRow(row(2, "galoschi"));
        assertThat(shardingProjector.id(), is("AghnYWxvc2NoaQEy"));
        assertThat(shardingProjector.routing(), is("galoschi"));
    }

    @Test
    public void testIdPrimaryKeyNull() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(2));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.of(ID_IDENT), primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(1, "hoschi", null));

        // generated _id, special routing
        assertNotNull(shardingProjector.id());
        assertThat(shardingProjector.routing(), is("hoschi"));
    }

    @Test
    public void testPrimaryKeyNullException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A primary key value must not be NULL");

        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), primaryKeySymbols, null);
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(new Object[] { null }));
    }

    @Test
    public void testMultiPrimaryKeyNullException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A primary key value must not be NULL");

        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(1), new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(ImmutableList.<ColumnIdent>of(), primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection(mock(ExecutionState.class));
        shardingProjector.setNextRow(row(1, null));
    }
}
