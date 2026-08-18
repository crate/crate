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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class RelationNameSwapTest {

    @Test
    public void test_streaming_table_oids() throws Exception {
        // streaming to nodes with supported versions
        RelationName source = new RelationName("doc", "source");
        RelationName target = new RelationName("doc", "target");
        int sourceOid = 1234;
        int targetOid = 5678;
        RelationNameSwap swap = new RelationNameSwap(source, sourceOid, target, targetOid);

        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);
        swap.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_5_0);
        RelationNameSwap streamed = new RelationNameSwap(in);

        assertThat(streamed.sourceOid()).isEqualTo(sourceOid);
        assertThat(streamed.targetOid()).isEqualTo(targetOid);

        // streaming to nodes with unsupported versions
        swap = new RelationNameSwap(source, sourceOid, target, targetOid);

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_0);
        swap.writeTo(out);

        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_4_0);
        streamed = new RelationNameSwap(in);

        assertThat(streamed.sourceOid()).isEqualTo(OID_UNASSIGNED);
        assertThat(streamed.targetOid()).isEqualTo(OID_UNASSIGNED);
    }
}
