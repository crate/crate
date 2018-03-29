/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableMap;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class PartitionPropertiesAnalyzerTest extends CrateUnitTest {

    private PartitionName getPartitionName(DocTableInfo tableInfo) {
        return PartitionPropertiesAnalyzer.toPartitionName(
            tableInfo,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("name")),
                new StringLiteral("foo"))),
            Row.EMPTY);
    }

    @Test
    public void testPartitionNameFromAssignmentWithBytesRef() throws Exception {
        DocTableInfo tableInfo = TestingTableInfo.builder(new RelationName("doc", "users"),
            new Routing(ImmutableMap.of()))
            .add("name", DataTypes.STRING, null, true)
            .addPrimaryKey("name").build();

        PartitionName partitionName = getPartitionName(tableInfo);
        assertThat(partitionName.asIndexName(), is(".partitioned.users.0426crrf"));
    }

    @Test
    public void testPartitionNameOnRegularTable() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table 'doc.users' is not partitioned");
        DocTableInfo tableInfo = TestingTableInfo.builder(new RelationName("doc", "users"),
            new Routing(ImmutableMap.of()))
            .add("name", DataTypes.STRING, null, false)
            .addPrimaryKey("name").build();

        getPartitionName(tableInfo);
    }
}
