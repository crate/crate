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

package io.crate.fdw;

import java.util.List;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ParquetBatchIteratorTest extends CrateDummyClusterServiceUnitTest {
    private final Path parquetFile = Paths.get(getClass().getResource("/essetup/data/parquet").toURI())
            .resolve("yellow_tripdata_2026-10rows.parquet");

    public ParquetBatchIteratorTest() throws URISyntaxException {
    }

    @Test
    public void test_reads_all_records() throws Exception {
        var e = SQLExecutor.of(clusterService)
                .addTable("create table doc.taxi (trip_distance double)");
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_distance")));
        ParquetBatchIterator it = new ParquetBatchIterator(parquetFile, columns);
        it.loadNextBatch();
        it.moveNext();

    }
}
