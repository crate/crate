/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateDropSchemaAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Test
    public void test_cannot_drop_system_schemas() {
        e = SQLExecutor.of(clusterService);

        assertThatThrownBy(() -> e.analyze("drop schema sys"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop reserved schema 'sys'");

        assertThatThrownBy(() -> e.analyze("drop schema doc"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop reserved schema 'doc'");

    }

    @Test
    public void test_cannot_create_system_schemas() {
        e = SQLExecutor.of(clusterService);

        assertThatThrownBy(() -> e.analyze("create schema sys"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Schema 'sys' is reserved");

        assertThatThrownBy(() -> e.analyze("create schema doc"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Schema 'doc' is reserved");
    }
}
