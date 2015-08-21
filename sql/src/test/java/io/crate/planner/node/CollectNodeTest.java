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
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CollectNodeTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ImmutableList<Symbol> toCollect = ImmutableList.<Symbol>of(new Value(DataTypes.STRING));
        UUID jobId = UUID.randomUUID();
        CollectPhase cn = new CollectPhase(
                jobId,
                0,
                "cn",
                new Routing(),
                RowGranularity.DOC,
                toCollect,
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL
        );

        BytesStreamOutput out = new BytesStreamOutput();
        cn.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        CollectPhase cn2 = CollectPhase.FACTORY.create();
        cn2.readFrom(in);
        assertThat(cn, equalTo(cn2));

        assertThat(cn.toCollect(), is(cn2.toCollect()));
        assertThat(cn.executionNodes(), is(cn2.executionNodes()));
        assertThat(cn.jobId(), is(cn2.jobId()));
        assertThat(cn.executionPhaseId(), is(cn2.executionPhaseId()));
        assertThat(cn.maxRowGranularity(), is(cn2.maxRowGranularity()));
    }
}
