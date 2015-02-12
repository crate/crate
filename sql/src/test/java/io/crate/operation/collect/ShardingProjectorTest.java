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
import io.crate.metadata.Functions;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ShardingProjectorTest {

    private static Functions functions;

    @BeforeClass
    public static void setupClass() {
        Injector injector = new ModulesBuilder().add(
                new ScalarFunctionModule()
        ).createInjector();

        functions = injector.getInstance(Functions.class);
    }


    @Test
    public void testNoPrimaryKeyNoRouting() {
        ShardingProjector shardingProjector =
                new ShardingProjector(functions, ImmutableList.<Symbol>of(), null);
        shardingProjector.startProjection();
        shardingProjector.setNextRow(new Object[]{});
        ShardingProjector.IdAndRouting idAndRouting = shardingProjector.idAndRouting();

        // auto-generated id, no special routing
        assertNotNull(idAndRouting.id());
        assertNull(idAndRouting.routing());
    }

    @Test
    public void testNoPrimaryKeyButRouting() {
        ShardingProjector shardingProjector =
                new ShardingProjector(functions, ImmutableList.<Symbol>of(), new InputColumn(1));
        shardingProjector.startProjection();
        shardingProjector.setNextRow(new Object[]{1, "hoschi"});
        ShardingProjector.IdAndRouting idAndRouting = shardingProjector.idAndRouting();

        // auto-generated id, special routing
        assertNotNull(idAndRouting.id());
        assertThat(idAndRouting.routing(), is("hoschi"));
    }

    @Test
    public void testPrimaryKeyNoRouting() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(0), new InputColumn(1));
        ShardingProjector shardingProjector =
                new ShardingProjector(functions, primaryKeySymbols, null);
        shardingProjector.startProjection();
        shardingProjector.setNextRow(new Object[]{1, "hoschi"});
        ShardingProjector.IdAndRouting idAndRouting = shardingProjector.idAndRouting();

        // compound encoded id, no special routing
        assertThat(idAndRouting.id(), is("AgExBmhvc2NoaQ=="));
        assertNull(idAndRouting.routing());
    }

    @Test
    public void testPrimaryKeyAndRouting() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(1), new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(functions, primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection();
        shardingProjector.setNextRow(new Object[]{1, "hoschi"});
        ShardingProjector.IdAndRouting idAndRouting = shardingProjector.idAndRouting();

        // compound encoded id, special routing
        assertThat(idAndRouting.id(), is("AgZob3NjaGkBMQ=="));
        assertThat(idAndRouting.routing(), is("hoschi"));
    }

    @Test
    public void testMultipleRows() {
        List<Symbol> primaryKeySymbols = ImmutableList.<Symbol>of(new InputColumn(1), new InputColumn(0));
        ShardingProjector shardingProjector =
                new ShardingProjector(functions, primaryKeySymbols, new InputColumn(1));
        shardingProjector.startProjection();

        shardingProjector.setNextRow(new Object[]{1, "hoschi"});
        ShardingProjector.IdAndRouting idAndRouting = shardingProjector.idAndRouting();
        assertThat(idAndRouting.id(), is("AgZob3NjaGkBMQ=="));
        assertThat(idAndRouting.routing(), is("hoschi"));

        shardingProjector.setNextRow(new Object[]{2, "galoschi"});
        assertThat(idAndRouting.id(), is("AghnYWxvc2NoaQEy"));
        assertThat(idAndRouting.routing(), is("galoschi"));
    }
}
