/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.planner.distribution;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;

public class DistributionInfoTest extends CrateUnitTest {

    @Test
    public void testStreamingDefaultImpl() throws Exception {
        DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

        BytesStreamOutput out = new BytesStreamOutput(10);
        distributionInfo.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        DistributionInfo streamed = DistributionInfo.fromStream(in);

        assertThat(streamed, equalTo(distributionInfo));
    }

    @Test
    public void testStreamingConcreteImpl() throws Exception {
        DistributionInfo distributionInfo = new DistributionInfo(DistributionType.MODULO, 1);

        BytesStreamOutput out = new BytesStreamOutput(10);
        distributionInfo.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        DistributionInfo streamed = DistributionInfo.fromStream(in);

        assertThat(streamed, equalTo(distributionInfo));
    }
}
