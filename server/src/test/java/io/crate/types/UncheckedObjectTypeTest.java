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

package io.crate.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class UncheckedObjectTypeTest extends ESTestCase {

    UncheckedObjectType uncheckedObjectType = UncheckedObjectType.INSTANCE;

    @Test
    public void testStreamingMap() throws IOException {
        long longKey = 11L;
        int[] arrayValue = {2, 3};
        String stringKey = "string_key";
        int intValue = 42;
        Map<Object, Object> genericValues = Map.of(longKey, arrayValue, stringKey, intValue);

        BytesStreamOutput out = new BytesStreamOutput();
        uncheckedObjectType.writeValueTo(out, genericValues);

        StreamInput in = out.bytes().streamInput();
        Map<Object, Object> readMap = uncheckedObjectType.readValueFrom(in);

        assertThat(readMap.get(longKey)).isEqualTo(arrayValue);
        assertThat(readMap.get(stringKey)).isEqualTo(intValue);
    }

    @Test
    public void testStreamingOfNullValue() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        uncheckedObjectType.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        Map<Object, Object> v = uncheckedObjectType.readValueFrom(in);
        assertThat(v).isNull();
    }
}
