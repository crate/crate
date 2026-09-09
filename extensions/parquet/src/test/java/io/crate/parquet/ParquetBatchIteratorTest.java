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

package io.crate.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dev.hardwood.InputFile;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Reference;
import io.crate.parquet.exceptions.IncompatibleSchemaForParquetException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ParquetBatchIteratorTest extends CrateDummyClusterServiceUnitTest {
    private final List<Path> parquetFile = List.of(
            Paths.get(getClass().getResource("/data").toURI())
                    .resolve("yellow_tripdata_2026-01.parquet"),
            Paths.get(getClass().getResource("/data").toURI())
                    .resolve("yellow_tripdata_2026-02.parquet"));

    public ParquetBatchIteratorTest() throws URISyntaxException {
    }

    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
                .addTable("create table doc.taxi (trip_distance double, passenger_count bigint)");
    }

    @Test
    public void test_reads_all_data_from_two_parquet_files() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_distance")),
                table.getReadReference(ColumnIdent.of("passenger_count")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { 0.97, 1L },
                new Object[] { 0.9, 0L },
                new Object[] { 1.4, 0L },
                new Object[] { 5.58, 4L },
                new Object[] { 2.16, 0L },
                new Object[] { 2.33, 2L },
                new Object[] { 1.3, 1L },
                new Object[] { 2.9, 0L },
                new Object[] { 5.34, 1L },
                new Object[] { 1.83, 3L },
                new Object[] { 1.54, 1L },
                new Object[] { 1.79, 1L },
                new Object[] { 1.24, 2L },
                new Object[] { 2.0, 4L },
                new Object[] { 0.83, 1L },
                new Object[] { 3.83, 1L },
                new Object[] { 5.38, 1L },
                new Object[] { 1.22, 2L },
                new Object[] { 1.69, 3L },
                new Object[] { 1.13, 1L });
    }

    @Test
    public void test_pushes_projections_down() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("passenger_count")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        // we should only see the rows for passenger_count and not trip_distance
        assertThat(rows).containsExactly(
                new Object[] { 1L },
                new Object[] { 0L },
                new Object[] { 0L },
                new Object[] { 4L },
                new Object[] { 0L },
                new Object[] { 2L },
                new Object[] { 1L },
                new Object[] { 0L },
                new Object[] { 1L },
                new Object[] { 3L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 2L },
                new Object[] { 4L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 2L },
                new Object[] { 3L },
                new Object[] { 1L });
    }

    @Test
    public void test_raises_if_cratedb_foreign_table_schema_is_incompatible_with_the_underlying_parquet_schema()
            throws Exception {
        SQLExecutor executor = SQLExecutor.of(clusterService)
                .addTable("create table doc.taxi_wrong_data_type (trip_distance text)");
        DocTableInfo table = executor.resolveTableInfo("doc.taxi_wrong_data_type");

        Symbol query = executor.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_distance")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        assertThatThrownBy(() -> Utils.getRows(it))
                .isExactlyInstanceOf(IncompatibleSchemaForParquetException.class)
                .hasMessage(
                        "The requested column `trip_distance` has type `text`, but that cannot be converted to the type of the column in the parquet file");
    }
}
