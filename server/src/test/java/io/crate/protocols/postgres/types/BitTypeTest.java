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

package io.crate.protocols.postgres.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.crate.sql.tree.BitString;

public class BitTypeTest {

    @Test
    public void test_can_decode_text_without_B_prefix() {
        assertThat(BitType.INSTANCE.decodeUTF8Text("0010".getBytes(StandardCharsets.UTF_8))).isEqualTo(BitString.ofRawBits("0010"));
    }

    @Test
    public void test_can_decode_text_with_B_prefix() {
        assertThat(BitType.INSTANCE.decodeUTF8Text("B'0010'".getBytes(StandardCharsets.UTF_8))).isEqualTo(BitString.ofRawBits("0010"));
    }
}
