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

import static io.crate.testing.TestingHelpers.printedTable;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dev.hardwood.InputFile;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Reference;
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
    public void test_pushes_projections_down() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("passenger_count")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Row> rows = Utils.getRows(it);
        assertThat(printedTable(rows)).isEqualTo("1\n0\n0\n4\n0\n2\n1\n0\n1\n3\n1\n1\n2\n4\n1\n1\n1\n2\n3\n1\n");

    }
}
