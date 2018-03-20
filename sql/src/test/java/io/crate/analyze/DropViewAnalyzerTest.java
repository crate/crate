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

package io.crate.analyze;

import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class DropViewAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testDropViewContainsIfExistsAndViewNamesWithSchema() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();

        DropViewStmt dropView = e.analyze("drop view if exists v1, v2, x.v3");

        assertThat(dropView.ifExists(), is(true));
        String defaultSchema = e.getSessionContext().defaultSchema();
        assertThat(dropView.views(), contains(
            is(new TableIdent(defaultSchema, "v1")),
            is(new TableIdent(defaultSchema, "v2")),
            is(new TableIdent("x", "v3")))
        );
    }
}
