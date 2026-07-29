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

package io.crate.execution.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class SwapRelationsRequestTest {

    @Test
    public void test_streaming_table_oids() throws Exception {
        // streaming to nodes with supported versions
        for (Version version : List.of(Version.V_6_3_7, Version.V_6_4_2, Version.CURRENT)) {
            RelationName source = new RelationName("doc", "source");
            RelationName target = new RelationName("doc", "target");
            int sourceOid = 1234;
            int targetOid = 5678;
            SwapRelationsRequest request = new SwapRelationsRequest(
                List.of(new RelationNameSwap(source, sourceOid, target, targetOid)),
                List.of(source)
            );

            var out = new BytesStreamOutput();
            out.setVersion(version);
            request.writeTo(out);

            var in = out.bytes().streamInput();
            in.setVersion(version);
            SwapRelationsRequest streamed = new SwapRelationsRequest(in);
            RelationNameSwap swap = streamed.swapActions().get(0);

            assertThat(swap.sourceOid()).isEqualTo(sourceOid);
            assertThat(swap.targetOid()).isEqualTo(targetOid);
        }

        // streaming to nodes with unsupported versions
        for (Version version : List.of(Version.V_6_3_6, Version.V_6_4_0, Version.V_6_4_1)) {
            RelationName source = new RelationName("doc", "source");
            RelationName target = new RelationName("doc", "target");
            SwapRelationsRequest request = new SwapRelationsRequest(
                List.of(new RelationNameSwap(source, 1234, target, 5678)),
                List.of(source)
            );

            var out = new BytesStreamOutput();
            out.setVersion(version);
            request.writeTo(out);

            var in = out.bytes().streamInput();
            in.setVersion(version);
            SwapRelationsRequest streamed = new SwapRelationsRequest(in);
            RelationNameSwap swap = streamed.swapActions().get(0);

            assertThat(swap.sourceOid()).isEqualTo(OID_UNASSIGNED);
            assertThat(swap.targetOid()).isEqualTo(OID_UNASSIGNED);
        }
    }
}
