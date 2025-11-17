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

package io.crate.planner.distribution;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.InputColumn;

public class DistributionInfoTest extends ESTestCase {

    @Test
    public void testStreamingDefaultImpl() throws Exception {
        DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

        BytesStreamOutput out = new BytesStreamOutput(10);
        distributionInfo.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        DistributionInfo streamed = new DistributionInfo(in);

        assertThat(streamed).isEqualTo(distributionInfo);
    }

    @Test
    public void testStreamingConcreteImpl() throws Exception {
        DistributionInfo distributionInfo = new DistributionInfo(DistributionType.MODULO, 1);

        BytesStreamOutput out = new BytesStreamOutput(10);
        distributionInfo.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        DistributionInfo streamed = new DistributionInfo(in);

        assertThat(streamed).isEqualTo(distributionInfo);
    }

    @Test
    public void test_bwc_streaming() throws Exception {
        DistributionInfo distributionInfo = new DistributionInfo(DistributionType.MODULO, new InputColumn(2));
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_13);
            distributionInfo.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_13);
                DistributionInfo infoIn = new DistributionInfo(in);
                assertThat(infoIn).isEqualTo(distributionInfo);
                assertThat(infoIn.distributeByColumn()).isInputColumn(2);
            }
        }
    }
}
