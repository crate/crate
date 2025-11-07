/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

public class MetadataTest {

    @Test
    public void test_bwc_read_writes_with_6_1_0() throws Exception {
        Metadata metadata = Metadata.builder()
                .columnOID(123L)
                // builder() adds IndexGraveyard custom, which causes "can't read named writeable from StreamInput" error on reads.
                // In production NamedWriteableAwareStreamInput is used.
                // Resetting it here for simplicity as it's irrelevant for the test.
                .removeCustom(IndexGraveyard.TYPE)
                .build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.fromString("6.1.0"));
        metadata.writeTo(out); // OID should be written, 6.1.0 expects it.
        var in = out.bytes().streamInput();
        in.setVersion(Version.fromString("6.1.0"));
        Metadata recievedMetadata = Metadata.readFrom(in); // We are reading from 6.1.0, which sends out OID.
        assertThat(recievedMetadata.columnOID()).isEqualTo(123L);
    }
}
