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

package io.crate.operation.count;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class InternalCountOperationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCount() throws Exception {
        execute("create table t (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin'), ('Arthur'), ('Trillian')");
        execute("refresh table t");

        CountOperation countOperation = internalCluster().getDataNodeInstance(CountOperation.class);
        ClusterService clusterService = internalCluster().getDataNodeInstance(ClusterService.class);
        MetaData metaData = clusterService.state().getMetaData();
        Index index = metaData.index("t").getIndex();
        assertThat(countOperation.count(index, 0, WhereClause.MATCH_ALL), is(3L));

        Schemas schemas = internalCluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new TableIdent(Schemas.DOC_SCHEMA_NAME, "t"));
        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<QualifiedName, AnalyzedRelation> tableSources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(new QualifiedName(tableInfo.ident().name()), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);

        WhereClause whereClause = new WhereClause(sqlExpressions.normalize(sqlExpressions.asSymbol("name = 'Marvin'")));
        assertThat(countOperation.count(index, 0, whereClause), is(1L));
    }
}
