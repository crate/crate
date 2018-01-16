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
import io.crate.operation.reference.information.ColumnContext;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ColumnsIterableTest {


    @Test
    public void testColumnsIteratorCanBeMaterializedToList() throws Exception {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(T3.T1_INFO);
        ImmutableList<ColumnContext> contexts = ImmutableList.copyOf((Iterable<ColumnContext>) columns);

        assertThat(
            contexts.stream().map(c -> c.info.ident().columnIdent().name()).collect(Collectors.toList()),
            Matchers.contains("a", "x", "i"));
    }

    @Test
    public void testColumnsIterableCanBeConsumedTwice() throws Exception {
        List<String> names = new ArrayList<>(6);
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(T3.T1_INFO);
        for (ColumnContext column : columns) {
            names.add(column.info.ident().columnIdent().name());
        }
        for (ColumnContext column : columns) {
            names.add(column.info.ident().columnIdent().name());
        }
        assertThat(names, Matchers.contains("a", "x", "i", "a", "x", "i"));
    }

    @Test
    public void testOrdinalIsNullOnSubColumns() throws Exception {
        InformationSchemaIterables.ColumnsIterable columns = new InformationSchemaIterables.ColumnsIterable(T3.T4_INFO);
        ImmutableList<ColumnContext> contexts = ImmutableList.copyOf(columns);

        // sub columns must have NULL ordinal value
        assertThat(contexts.get(1).ordinal, is(new Short("2")));
        assertThat(contexts.get(2).ordinal, nullValue());

        // array of object sub columns also
        assertThat(contexts.get(3).ordinal, is(new Short("3")));
        assertThat(contexts.get(4).ordinal, nullValue());
    }
}
