/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.*;

public class RoutedCollectPhaseTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ImmutableList<Symbol> toCollect = ImmutableList.<Symbol>of(new Value(DataTypes.STRING));
        UUID jobId = UUID.randomUUID();
        RoutedCollectPhase cn = new RoutedCollectPhase(
            jobId,
            0,
            "cn",
            new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()),
            RowGranularity.DOC,
            toCollect,
            ImmutableList.<Projection>of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_MODULO,
            (byte) 5
        );

        BytesStreamOutput out = new BytesStreamOutput();
        cn.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        RoutedCollectPhase cn2 = RoutedCollectPhase.FACTORY.create();
        cn2.readFrom(in);
        assertThat(cn, equalTo(cn2));

        assertThat(cn.toCollect(), is(cn2.toCollect()));
        assertThat(cn.nodeIds(), is(cn2.nodeIds()));
        assertThat(cn.jobId(), is(cn2.jobId()));
        assertThat(cn.phaseId(), is(cn2.phaseId()));
        assertThat(cn.maxRowGranularity(), is(cn2.maxRowGranularity()));
        assertThat(cn.distributionInfo(), is(cn2.distributionInfo()));
        assertThat(cn.relationId(), is(cn2.relationId()));
    }

    @Test
    public void testNormalizeDoesNotRemoveOrderBy() throws Exception {
        Symbol toInt10 = CastFunctionResolver.generateCastFunction(Literal.of(10L), DataTypes.INTEGER, false);
        RoutedCollectPhase collect = new RoutedCollectPhase(
            UUID.randomUUID(),
            1,
            "collect",
            new Routing(Collections.emptyMap()),
            RowGranularity.DOC,
            Collections.singletonList(toInt10),
            Collections.emptyList(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_SAME_NODE,
            (byte) 0);
        collect.orderBy(new OrderBy(Collections.singletonList(toInt10), new boolean[]{false}, new Boolean[]{null}));
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(getFunctions(), ReplaceMode.COPY);
        RoutedCollectPhase normalizedCollect = collect.normalize(
            normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION));

        assertThat(normalizedCollect.orderBy(), notNullValue());
    }
}
