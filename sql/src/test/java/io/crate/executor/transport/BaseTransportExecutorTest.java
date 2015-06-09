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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.where.DocKeys;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.integrationtests.Setup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class BaseTransportExecutorTest extends SQLTransportIntegrationTest {

    Setup setup = new Setup(sqlExecutor);

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    TransportExecutor executor;
    DocSchemaInfo docSchemaInfo;
    ClusterService clusterService;

    TableIdent charactersIdent = new TableIdent(null, "characters");
    TableIdent booksIdent = new TableIdent(null, "books");

    Reference idRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference nameRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "name"), RowGranularity.DOC, DataTypes.STRING));
    Reference femaleRef = TestingHelpers.createReference(charactersIdent.name(), new ColumnIdent("female"), DataTypes.BOOLEAN);

    TableIdent partedTable = new TableIdent("doc", "parted");
    Reference partedIdRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference partedNameRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "name"), RowGranularity.DOC, DataTypes.STRING));
    Reference partedDateRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "date"), RowGranularity.PARTITION, DataTypes.TIMESTAMP));

    public static ESGetNode newGetNode(TableInfo tableInfo, List<Symbol> outputs, List<String> singleStringKeys, int executionNodeId) {
        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(outputs);
        List<List<Symbol>> keys = new ArrayList<>(singleStringKeys.size());
        for (String v : singleStringKeys) {
            keys.add(ImmutableList.<Symbol>of(Literal.newLiteral(v)));
        }
        WhereClause whereClause = new WhereClause();
        whereClause.docKeys(new DocKeys(keys, false, -1, null));
        querySpec.where(whereClause);
        return new ESGetNode(executionNodeId, tableInfo, querySpec);
    }

    @Before
    public void transportSetUp() {
        String[] nodeNames = internalCluster().getNodeNames();
        String handlerNodeName = nodeNames[randomIntBetween(0, nodeNames.length-1)];
        executor = internalCluster().getInstance(TransportExecutor.class, handlerNodeName);
        docSchemaInfo = internalCluster().getInstance(DocSchemaInfo.class, handlerNodeName);
        clusterService = internalCluster().getInstance(ClusterService.class, handlerNodeName);
    }

    @After
    public void transportTearDown() {
        executor = null;
        docSchemaInfo = null;
    }
}
