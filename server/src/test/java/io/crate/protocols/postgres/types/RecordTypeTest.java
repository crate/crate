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
import java.util.List;

import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.RowN;

public class RecordTypeTest extends BasePGTypeTest<Row> {

    public RecordTypeTest() {
        super(new RecordType(List.of(
            IntegerType.INSTANCE,
            VarCharType.INSTANCE,
            VarCharType.INSTANCE,
            VarCharType.INSTANCE,
            VarCharType.INSTANCE
        )));
    }

    @Test
    public void test_record_text_encoding() throws Exception {
        Row record = new RowN(10, "", "foo", "foo bar");

        byte[] bytes = pgType.encodeAsUTF8Text(record);
        var str = new String(bytes, StandardCharsets.UTF_8);
        assertThat(str).isEqualTo("(10,\"\",foo,\"foo bar\")");
    }

    @Test
    public void test_record_null_value_text_encoding() throws Exception {
        Row record = new RowN(10, "", null, "foo");

        byte[] bytes = pgType.encodeAsUTF8Text(record);
        var str = new String(bytes, StandardCharsets.UTF_8);
        assertThat(str).isEqualTo("(10,\"\",,foo)");
    }
}
