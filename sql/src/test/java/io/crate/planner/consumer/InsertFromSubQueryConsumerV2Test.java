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

package io.crate.planner.consumer;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.ProjectedNode;
import io.crate.planner.node.dml.InsertNode;
import io.crate.planner.node.dml.QueryAndFetchNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class InsertFromSubQueryConsumerV2Test {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final TableIdent TEST_TABLE_IDENT = new TableIdent(null, "test");
    private static final TableInfo TEST_TABLE_INFO = TestingTableInfo.builder(TEST_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .addPrimaryKey("id")
            .clusteredBy("id")
            .build();
    private static final TableRelation TEST_TABLE_RELATION = new TableRelation(TEST_TABLE_INFO);

    @Test
    public void testQueryAndFetchSubQuery() throws Exception {
        Routing routing = TEST_TABLE_INFO.getRouting(WhereClause.MATCH_ALL, null);
        CollectNode collectNode = new CollectNode("collect", routing);

        collectNode.toCollect(Lists.transform(TEST_TABLE_RELATION.fields(), new Function<Field, Symbol>() {
            @Nullable
            @Override
            public Symbol apply(Field input) {
                return input;
            }
        }));
        QueryAndFetchNode queryAndFetchNode = new QueryAndFetchNode(collectNode);

        InsertFromSubQueryAnalyzedStatement statement =
                new InsertFromSubQueryAnalyzedStatement(queryAndFetchNode, TEST_TABLE_INFO);
        statement.columns(Lists.transform(Lists.newArrayList(TEST_TABLE_INFO.columns()), new Function<ReferenceInfo, Reference>() {
            @Nullable
            @Override
            public Reference apply(@Nullable ReferenceInfo input) {
                return new Reference(input);
            }
        }));

        ConsumerContext consumerContext = new ConsumerContext(statement);
        Consumer consumer = new InsertFromSubQueryConsumerV2();
        consumer.consume(consumerContext.rootRelation(), consumerContext);

        assertThat(consumerContext.rootRelation(), instanceOf(InsertNode.class));

        InsertNode insertNode = (InsertNode)consumerContext.rootRelation();
        assertThat(insertNode.nodes().size(), is(2));

        assertThat(insertNode.nodes().get(0), instanceOf(QueryAndFetchNode.class));
        assertThat(insertNode.nodes().get(1), instanceOf(ProjectedNode.class));

        ProjectedNode projectedNode = (ProjectedNode)insertNode.nodes().get(1);
        assertThat(projectedNode.node(), is(insertNode.nodes().get(0)));
        assertThat(projectedNode.projection(), instanceOf(ColumnIndexWriterProjection.class));
    }
}
