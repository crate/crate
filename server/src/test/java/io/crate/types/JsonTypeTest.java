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

import static io.crate.testing.Asserts.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.crate.metadata.CoordinatorTxnCtx;

// TODO convert to DataTypeTestCase and add DataGenerator
public class JsonTypeTest {

    @Test
    void test_can_cast_object_to_json_string() throws Exception {
        String result = JsonType.INSTANCE.explicitCast(Map.of("x", 200), CoordinatorTxnCtx.systemTransactionContext().sessionSettings());
        assertThat(result).isEqualTo("{\"x\":200}");
    }
}
