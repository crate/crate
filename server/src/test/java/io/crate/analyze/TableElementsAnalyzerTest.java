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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import io.crate.sql.tree.CollectionColumnType;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.TableElement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TableElementsAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void test_analyze_method_assigned_proper_column_positions_to_nested_objects() {
        TableElement e1 = new ColumnDefinition(
            "nested",
            null,
            null,
            new ObjectColumnType(
                "dynamic",
                         List.of(
                             new ColumnDefinition(
                                 "nested2",
                                 null,
                                 null,
                                 new ObjectColumnType(
                                     "dynamic",
                                              List.of(
                                                  new ColumnDefinition(
                                                      "sub1",
                                                      null,
                                                      null,
                                                      new CollectionColumnType<>(
                                                          new ColumnType<>("integer")
                                                      ),
                                                      List.of())
                                              )
                                 ),
                                 List.of())
                         )
            ),
            List.of());

        TableElement e2 = new ColumnDefinition(
            "notNested",
            null,
            null,
            new ColumnType("integer"),
            List.of()
        );
        var analyzed = TableElementsAnalyzer.analyze(List.of(e1, e2), new RelationName(null, "dummy"), null, true);

        var nested = (AnalyzedColumnDefinition) analyzed.columns().get(0);
        assertThat(nested.position).isEqualTo(-1);

        var nested2 = (AnalyzedColumnDefinition) nested.children().get(0);
        assertThat(nested2.position).isEqualTo(-2);

        var sub1 = (AnalyzedColumnDefinition) nested2.children().get(0);
        assertThat(sub1.position).isEqualTo(-3);

        var notNested = (AnalyzedColumnDefinition) analyzed.columns().get(1);
        assertThat(notNested.position).isEqualTo(-4);

        // -1 ~ -4 represents column ordering, which will be used to re-calculated actual column positions.
        // see ColumnPositionResolver for more details
    }

    @Test
    public void test_analyze_can_calculate_position_values_when_index_columns_involved() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(
              """
              CREATE TABLE tbl (
                author TEXT NOT NULL,
                INDEX author_ft USING FULLTEXT (author) WITH (analyzer = 'standard')
              );
              """)
            .build();
        var analyzedRelation = (AnalyzedAlterTableAddColumn) e.analyze("ALTER TABLE tbl ADD COLUMN dummy text NOT NULL");
        /*
         * author       - position 1
         * author_ft    - position 2
         * dummy        - position -1
         */
        assertThat(analyzedRelation.analyzedTableElements().columns().get(0).position).isEqualTo(-1);
    }
}
