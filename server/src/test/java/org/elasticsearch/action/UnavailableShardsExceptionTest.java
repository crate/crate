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

package org.elasticsearch.action;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import io.crate.metadata.RelationName;


public class UnavailableShardsExceptionTest {

    @Test
    public void test_can_get_relation_name_from_streamed_exception() throws Exception {
        var ex = new UnavailableShardsException(new ShardId("idx1", UUIDs.randomBase64UUID(), 0));
        var out = new BytesStreamOutput();
        ex.writeTo(out);
        try (var in = out.bytes().streamInput()) {
            var inEx = new UnavailableShardsException(in);

            assertThat(inEx.getTableIdents()).containsExactly(new RelationName("doc", "idx1"));
        }
    }
}

