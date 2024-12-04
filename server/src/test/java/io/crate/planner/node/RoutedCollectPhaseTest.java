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

package io.crate.planner.node;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataTypes;

public class RoutedCollectPhaseTest extends ESTestCase {

    private NodeContext nodeCtx = createNodeContext();

    @Test
    public void testStreaming() throws Exception {
        List<Symbol> toCollect = List.of(Literal.of(DataTypes.STRING, null));
        UUID jobId = UUID.randomUUID();
        RoutedCollectPhase cn = new RoutedCollectPhase(
            jobId,
            0,
            "cn",
            new Routing(Map.of()),
            RowGranularity.DOC,
            toCollect,
            List.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_MODULO
        );

        BytesStreamOutput out = new BytesStreamOutput();
        cn.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RoutedCollectPhase cn2 = new RoutedCollectPhase(in);
        assertThat(cn).isEqualTo(cn2);

        assertThat(cn.toCollect()).isEqualTo(cn2.toCollect());
        assertThat(cn.nodeIds()).isEqualTo(cn2.nodeIds());
        assertThat(cn.jobId()).isEqualTo(cn2.jobId());
        assertThat(cn.phaseId()).isEqualTo(cn2.phaseId());
        assertThat(cn.maxRowGranularity()).isEqualTo(cn2.maxRowGranularity());
        assertThat(cn.distributionInfo()).isEqualTo(cn2.distributionInfo());
    }
}
