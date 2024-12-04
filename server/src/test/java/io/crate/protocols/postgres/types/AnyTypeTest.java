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

import org.junit.Test;

public class AnyTypeTest extends BasePGTypeTest<Integer> {

    public AnyTypeTest() {
        super(AnyType.INSTANCE);
    }

    @Test
    public void test_write_value() {
        assertBytesWritten(Integer.MIN_VALUE, new byte[]{0, 0, 0, 4, -128, 0, 0, 0});
    }

    @Test
    public void test_read_value_binary() {
        assertBytesReadBinary(new byte[]{127, -1, -1, -1}, Integer.MAX_VALUE);
    }

    @Test
    public void test_read_value_text() {
        byte[] bytesToRead = String.valueOf(Integer.MAX_VALUE).getBytes(StandardCharsets.UTF_8);
        assertBytesReadText(bytesToRead, Integer.MAX_VALUE, bytesToRead.length);
    }

    @Test
    public void test_typreceive_is_0_for_anytype() throws Exception {
        /** PostgreSQL types that have a 0 oid as "typreceive" value
         * select typname  from pg_type where typreceive = 0;
         *     typname
         *------------------
         * aclitem
         * gtsvector
         * any
         * trigger
         * event_trigger
         * language_handler
         * internal
         * opaque
         * anyelement
         * anynonarray
         * anyenum
         * fdw_handler
         * index_am_handler
         * tsm_handler
         * table_am_handler
         * anyrange
         **/
        assertThat(AnyType.INSTANCE.typReceive().oid()).isEqualTo(0);
        assertThat(AnyType.INSTANCE.typReceive().name()).isEqualTo("-");
    }

    @Test
    public void test_typsend_is_0_for_anytype() throws Exception {
        assertThat(AnyType.INSTANCE.typSend().oid()).isEqualTo(0);
        assertThat(AnyType.INSTANCE.typSend().name()).isEqualTo("-");
    }
}
