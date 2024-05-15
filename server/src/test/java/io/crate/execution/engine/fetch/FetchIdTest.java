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

package io.crate.execution.engine.fetch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.LongStream;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class FetchIdTest extends ESTestCase {

    @Test
    public void testEncodeAndDecode() throws Exception {
        LongStream longs = random().longs(50, 0, Long.MAX_VALUE);
        longs.forEach(this::assertEncodeDecodeMatches);
    }

    private void assertEncodeDecodeMatches(long val) {
        int docId = FetchId.decodeDocId(val);
        int readerId = FetchId.decodeReaderId(val);

        assertThat(FetchId.encode(readerId, docId)).isEqualTo(val);
    }
}
