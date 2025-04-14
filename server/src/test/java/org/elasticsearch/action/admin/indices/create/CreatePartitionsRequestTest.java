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

package org.elasticsearch.action.admin.indices.create;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


public class CreatePartitionsRequestTest {

    @Test
    public void testSerialization() throws Exception {
        RelationName tbl = new RelationName("doc", "tbl");
        List<PartitionName> partitionNames = List.of(new PartitionName(tbl, List.of("1")), new PartitionName(tbl, List.of("2")));
        CreatePartitionsRequest request = new CreatePartitionsRequest(tbl, partitionNames);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        CreatePartitionsRequest requestDeserialized = new CreatePartitionsRequest(out.bytes().streamInput());

        assertThat(requestDeserialized.partitionNames()).isEqualTo(partitionNames);
        assertThat(requestDeserialized.relationName()).isEqualTo(tbl);
    }
}
