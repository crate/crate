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

package io.crate.metadata.relation;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.where.PartitionResolver;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Symbol;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AnalyzedRelationTest {

    @Test
    public void testIdentifiersUnique() throws Exception {
        AnalyzedRelation table = new TableRelation(
                TestingTableInfo.builder(new TableIdent(null, "test"), RowGranularity.DOC, new Routing())
                        .build(),
                mock(PartitionResolver.class));
        AnalyzedRelation querySpec = new AnalyzedQuerySpecification(
                ImmutableList.<Symbol>of(), table, ImmutableList.<Symbol>of(),
                null, null, null, null);
        AnalyzedRelation aliasedRelation = new AliasedAnalyzedRelation("alias", table);
        AnalyzedRelation joinRelation = new JoinRelation(JoinRelation.Type.CROSS_JOIN, table, aliasedRelation);
        IntSet idents = IntOpenHashSet.from(table.ident(), querySpec.ident(), aliasedRelation.ident(), joinRelation.ident());
        assertThat(idents.size(), is(4));
    }
}
