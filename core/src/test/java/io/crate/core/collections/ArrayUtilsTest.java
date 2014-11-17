/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.core.collections;

import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ArrayUtilsTest {

    @Test
    public void testBytesRefContains() throws Exception {
        byte comma = ",".getBytes("ascii")[0];
        BytesRef ref = new BytesRef("a,b,c");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(true));
        ref = new BytesRef(",");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(true));
        ref = new BytesRef("");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(false));
        ref = new BytesRef("no_comma");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(false));
        ref = new BytesRef("comma at the end,");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(true));
        ref = new BytesRef(",comma at the beginning");
        assertThat(ArrayUtils.bytesArrayContains(ref.bytes, ref.offset, ref.length, comma), is(true));

    }
}
