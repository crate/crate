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

package io.crate.execution.engine.collect.sources;

import static io.crate.testing.T3.T1;
import static io.crate.testing.T3.T1_DEFINITION;
import static io.crate.testing.T3.T4;
import static io.crate.testing.T3.T4_DEFINITION;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.RelationInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ColumnsIterableTest extends CrateDummyClusterServiceUnitTest {

    private RelationInfo t1Info;
    private RelationInfo t4Info;

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(T1_DEFINITION)
            .addTable(T4_DEFINITION);
        t1Info = e.resolveTableInfo(T1.fqn());
        t4Info = e.resolveTableInfo(T4.fqn());
    }

    @Test
    public void testColumnsIteratorCanBeMaterializedToList() {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t1Info);
        List<ColumnContext> contexts = StreamSupport.stream(columns.spliterator(), false)
            .collect(Collectors.toList());

        assertThat(
            contexts.stream().map(c -> c.ref().column().name()).collect(Collectors.toList()),
            Matchers.contains("a", "x", "i"));
    }

    @Test
    public void testColumnsIterableCanBeConsumedTwice() {
        List<String> names = new ArrayList<>(6);
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t1Info);
        for (ColumnContext column : columns) {
            names.add(column.ref().column().name());
        }
        for (ColumnContext column : columns) {
            names.add(column.ref().column().name());
        }
        assertThat(names, Matchers.contains("a", "x", "i", "a", "x", "i"));
    }

    @Test
    public void testOrdinalIsNotNullOnSubColumns() throws Exception {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t4Info);
        List<ColumnContext> contexts = StreamSupport.stream(columns.spliterator(), false)
            .collect(Collectors.toList());

        // sub columns must have NON-NULL ordinal value
        assertThat(contexts.get(1).ref().position(), is(2));
        assertThat(contexts.get(2).ref().position(), is(3));

        // array of object sub columns also
        assertThat(contexts.get(3).ref().position(), is(4));
        assertThat(contexts.get(4).ref().position(), is(5));
    }

    @Test
    public void test_bit_type_has_character_maximum_length_set() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table bit_table (xs bit(12))");
        var columns = new InformationSchemaIterables.ColumnsIterable(e.resolveTableInfo("bit_table"));
        var contexts = StreamSupport.stream(columns.spliterator(), false).toList();

        assertThat(contexts.get(0).ref().valueType().characterMaximumLength(), is(12));
    }
}
