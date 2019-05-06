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

package io.crate.execution.engine.collect.sources;

import com.google.common.collect.ImmutableList;
import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.RelationInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.crate.testing.T3.T1_DEFINITION;
import static io.crate.testing.T3.T1_RN;
import static io.crate.testing.T3.T4_DEFINITION;
import static io.crate.testing.T3.T4_RN;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ColumnsIterableTest extends CrateDummyClusterServiceUnitTest {

    private RelationInfo t1Info;
    private RelationInfo t4Info;

    @Before
    public void prepare() throws Exception {
        t1Info = SQLExecutor.tableInfo(T1_RN, T1_DEFINITION, clusterService);
        t4Info = SQLExecutor.tableInfo(T4_RN, T4_DEFINITION, clusterService);
    }

    @Test
    public void testColumnsIteratorCanBeMaterializedToList() {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t1Info);
        ImmutableList<ColumnContext> contexts = ImmutableList.copyOf(columns);

        assertThat(
            contexts.stream().map(c -> c.info.column().name()).collect(Collectors.toList()),
            Matchers.contains("a", "x", "i"));
    }

    @Test
    public void testColumnsIterableCanBeConsumedTwice() {
        List<String> names = new ArrayList<>(6);
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t1Info);
        for (ColumnContext column : columns) {
            names.add(column.info.column().name());
        }
        for (ColumnContext column : columns) {
            names.add(column.info.column().name());
        }
        assertThat(names, Matchers.contains("a", "x", "i", "a", "x", "i"));
    }

    @Test
    public void testOrdinalIsNullOnSubColumns() throws Exception {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(t4Info);
        ImmutableList<ColumnContext> contexts = ImmutableList.copyOf(columns);

        // sub columns must have NULL ordinal value
        assertThat(contexts.get(1).ordinal, is(2));
        assertThat(contexts.get(2).ordinal, nullValue());

        // array of object sub columns also
        assertThat(contexts.get(3).ordinal, is(3));
        assertThat(contexts.get(4).ordinal, nullValue());
    }
}
