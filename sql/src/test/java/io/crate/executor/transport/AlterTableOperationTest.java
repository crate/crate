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

package io.crate.executor.transport;

import io.crate.Constants;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlterTableOperationTest extends CrateUnitTest {

    public static final BlobTableInfo BLOB_TABLE_INFO = TableDefinitions.createBlobTable(
        new TableIdent(BlobSchemaInfo.NAME, "screenshots"));

    @Test
    public void testPrepareAlterTableMappingRequest() throws Exception {
        Map<String, Object> oldMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "foo").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder().put("meta1", "val1").map())
            .map();

        Map<String, Object> newMapping = MapBuilder.<String, Object>newMapBuilder()
            .put("properties", MapBuilder.<String, String>newMapBuilder().put("foo", "bar").map())
            .put("_meta", MapBuilder.<String, String>newMapBuilder()
                .put("meta1", "v1")
                .put("meta2", "v2")
                .map())
            .map();

        PutMappingRequest request = AlterTableOperation.preparePutMappingRequest(oldMapping, newMapping);

        assertThat(request.type(), is(Constants.DEFAULT_MAPPING_TYPE));
        assertThat(request.source(), is("{\"_meta\":{\"meta2\":\"v2\",\"meta1\":\"v1\"},\"properties\":{\"foo\":\"bar\"}}"));
    }

    @Test
    public void testRerouteIndexOfBlobTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            BLOB_TABLE_INFO,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        String index = AlterTableOperation.getRerouteIndex(statement, Row.EMPTY);
        assertThat(index, is("blob.screenshots"));
    }

    @Test
    public void testRerouteIndexOfPartedDocTable() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1395874800000"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        String index = AlterTableOperation.getRerouteIndex(statement, Row.EMPTY);
        assertThat(index, is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testRerouteIndexOfPartedDocTableWithoutPartitionClause() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("table is partitioned however no partition clause has been specified");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
            Collections.emptyList(),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );
        AlterTableOperation.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void testRerouteMoveShardPartitionedTableUnknownPartition() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Referenced partition \".partitioned.parted.04132\" does not exist.");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.TEST_PARTITIONED_TABLE_INFO,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );

        AlterTableOperation.getRerouteIndex(statement, Row.EMPTY);
    }

    @Test
    public void testRerouteMoveShardUnpartitionedTableWithPartitionClause() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table 'doc.users' is not partitioned");
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TableDefinitions.USER_TABLE_INFO,
            Arrays.asList(new Assignment(
                new QualifiedNameReference(new QualifiedName("date")), new StringLiteral("1"))),
            SqlParser.createExpression("0"),
            SqlParser.createExpression("node1"),
            SqlParser.createExpression("node2")
        );

        AlterTableOperation.getRerouteIndex(statement, Row.EMPTY);
    }
}
