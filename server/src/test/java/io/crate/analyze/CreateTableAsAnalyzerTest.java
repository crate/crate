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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Test;

import java.io.IOException;

public class CreateTableAsAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSimpleCompareaAgainstAnalyzedCreateTable() throws IOException {

        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(
                "create table tbl (" +
                "   col_default_object object as (" +
                "       col_nested_integer integer," +
                "       col_nested_object object as (" +
                "           col_nested_timestamp_with_time_zone timestamp with time zone" +
                "       )" +
                "   )" +
                ")"
            )
            .build();

        var expected = (AnalyzedCreateTable) e.analyze(
            "create table cpy (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")"
        );

        AnalyzedCreateTableAs analyzedCreateTableAs = e.analyze(
            "create table cpy as select * from  tbl"
        );
        var actual = analyzedCreateTableAs.analyzedCreateTable();

        assertEquals(expected.relationName(), actual.relationName());
        //used toString() to avoid testing nested elements. handled by SymbolToColumnDefinitionConverterTest
        assertEquals(expected.createTable().toString(), actual.createTable().toString());
    }
}
