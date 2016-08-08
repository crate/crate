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
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.integrationtests.Setup;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.ESGet;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BaseTransportExecutorTest extends SQLTransportIntegrationTest {

    Setup setup = new Setup(sqlExecutor);

    TransportExecutor executor;
    DocSchemaInfo docSchemaInfo;

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference idRef = new Reference(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER);
    Reference nameRef = new Reference(
            new ReferenceIdent(charactersIdent, "name"), RowGranularity.DOC, DataTypes.STRING);
    Reference femaleRef = TestingHelpers.createReference(charactersIdent.name(), new ColumnIdent("female"), DataTypes.BOOLEAN);


    public static ESGet newGetNode(DocTableInfo tableInfo, List<Symbol> outputs, List<String> singleStringKeys, int executionNodeId) {
        QuerySpec querySpec = new QuerySpec();
        querySpec.outputs(outputs);
        List<List<Symbol>> keys = new ArrayList<>(singleStringKeys.size());
        for (String v : singleStringKeys) {
            keys.add(ImmutableList.<Symbol>of(Literal.newLiteral(v)));
        }
        WhereClause whereClause = new WhereClause(null, new DocKeys(keys, false, -1, null), null);
        querySpec.where(whereClause);
        return new ESGet(executionNodeId, tableInfo, querySpec, TopN.NO_LIMIT, UUID.randomUUID());
    }

    @Before
    public void transportSetUp() {
        String[] nodeNames = internalCluster().getNodeNames();
        String handlerNodeName = nodeNames[randomIntBetween(0, nodeNames.length-1)];
        executor = internalCluster().getInstance(TransportExecutor.class, handlerNodeName);
        docSchemaInfo = internalCluster().getInstance(DocSchemaInfo.class, handlerNodeName);
    }

    @After
    public void transportTearDown() {
        executor = null;
        docSchemaInfo = null;
    }

    protected ESGet newGetNode(String tableName, List<Symbol> outputs, String singleStringKey, int executionNodeId) {
        return newGetNode(tableName, outputs, Collections.singletonList(singleStringKey), executionNodeId);
    }

    protected ESGet newGetNode(String tableName, List<Symbol> outputs, List<String> singleStringKeys, int executionNodeId) {
        return newGetNode(docSchemaInfo.getTableInfo(tableName), outputs, singleStringKeys, executionNodeId);
    }

    protected Planner.Context newPlannerContext() {
        return new Planner.Context(clusterService(), UUID.randomUUID(), null, new StmtCtx(), 0, 0);
    }
}
