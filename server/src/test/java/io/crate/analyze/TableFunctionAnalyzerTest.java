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

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TableFunctionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).build();
    }

    @Test
    public void TestTableFunctionNameIsUsedWhenTableFunctionReturnsBaseDataType() {
        var analyzedRelation = e.analyze("select * from regexp_matches('a', 'a')");
        assertThat(analyzedRelation.outputs().get(0)).isReference().hasName("regexp_matches");

        analyzedRelation = e.analyze("select * from regexp_matches('a', 'a') as r");
        assertThat(analyzedRelation.outputs().get(0)).isField("r");

        analyzedRelation = e.analyze("select * from generate_series(0, 1, 1)");
        assertThat(analyzedRelation.outputs().get(0)).isReference().hasName("generate_series");

        analyzedRelation = e.analyze("select * from generate_series(0, 1, 1) as g");
        assertThat(analyzedRelation.outputs().get(0)).isField("g");

        analyzedRelation = e.analyze("select * from unnest([1])");
        assertThat(analyzedRelation.outputs().get(0)).isReference().hasName("unnest");

        analyzedRelation = e.analyze("select * from unnest([1]) as u");
        assertThat(analyzedRelation.outputs().get(0)).isField("u");
    }
}
