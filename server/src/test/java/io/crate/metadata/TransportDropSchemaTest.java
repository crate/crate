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

package io.crate.metadata;

import static io.crate.expression.udf.UdfUnitTest.DUMMY_LANG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class TransportDropSchemaTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_getViewsToDrop() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table foo.tbl (x long)")
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(new UserDefinedFunctionMetadata(
                "foo",
                "fn",
                List.of(FunctionArgumentDefinition.of("i", DataTypes.LONG)),
                DataTypes.LONG,
                DUMMY_LANG.name(),
                "function fn(i) { return i; }"
            ))
            .addView(new RelationName("bar", "v1"),
                "SELECT * FROM foo.tbl")
            .addView(new RelationName("bar", "v2"),
                "SELECT foo.fn(g) FROM generate_series(1, 10, 1) as g")
            .addView(new RelationName("bar", "v3"),
                "SELECT * FROM generate_series(1, 10, 1) as g WHERE g IN (SELECT foo.fn(x) FROM foo.tbl)")
            .addView(new RelationName("bar", "v4"),
                "SELECT * FROM (select * from unnest([1])) as t1(x) INNER JOIN (select * from unnest([1]) as u(x) WHERE x IN (SELECT * FROM foo.tbl)) as t2 ON t1.x = t2.x");

        Set<RelationName> viewsToDrop = TransportDropSchema.getViewsToDrop(
            new ViewInfoFactory(new RelationAnalyzer(e.nodeCtx)), clusterService.state(), "foo");
        assertThat(viewsToDrop).containsExactlyInAnyOrder(
            new RelationName("bar", "v1"),
            new RelationName("bar", "v2"),
            new RelationName("bar", "v3"),
            new RelationName("bar", "v4"));
    }
}
