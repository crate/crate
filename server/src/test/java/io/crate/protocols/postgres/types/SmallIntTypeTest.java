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

import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class SmallIntTypeTest extends BasePGTypeTest<Short> {

    public SmallIntTypeTest() {
        super(SmallIntType.INSTANCE);
    }

    @Test
    public void testWriteValue() throws Exception {
        assertBytesWritten(Short.MIN_VALUE, new byte[]{0, 0, 0, 2, -128, 0});
    }

    @Test
    public void testReadValueBinary() throws Exception {
        assertBytesReadBinary(new byte[]{127, -1}, Short.MAX_VALUE);
    }

    @Test
    public void testReadValueText() throws Exception {
        byte[] bytesToRead = String.valueOf(Short.MAX_VALUE).getBytes(StandardCharsets.UTF_8);
        assertBytesReadText(bytesToRead, Short.MAX_VALUE, bytesToRead.length);
    }
}
