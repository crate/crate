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

import io.crate.exceptions.InvalidRelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class AlterTableRenameAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testRenamePartitionThrowsException() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Renaming a single partition is not supported");
        e.analyze("alter table t1 partition (i=1) rename to t2");
    }

    @Test
    public void testRenameToUsingSchemaThrowsException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Target table name must not include a schema");
        e.analyze("alter table t1 rename to my_schema.t1");
    }

    @Test
    public void testRenameToInvalidName() throws Exception {
        expectedException.expect(InvalidRelationName.class);
        e.analyze("alter table t1 rename to \"foo.bar\"");
    }
}
