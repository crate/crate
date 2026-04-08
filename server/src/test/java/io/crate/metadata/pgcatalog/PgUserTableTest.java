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

package io.crate.metadata.pgcatalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SystemTable;
import io.crate.role.Role;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class PgUserTableTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_table_has_exactly_9_columns() {
        SystemTable<Role> table = PgUserTable.create(() -> List.of());
        assertThat(table.rootColumns()).hasSize(9);
    }

    @Test
    public void test_session_settings_are_returned_as_array_of_key_eq_value_strings() throws Exception {
        Role role = new Role("arthur", true, Set.of(), Set.of(), null, null, Map.of("statement_timeout", "10m"));
        SystemTable<Role> table = PgUserTable.create(() -> List.of(role));
        NestableCollectExpression<Role, ?> expr = table.expressions().get(ColumnIdent.of("useconfig")).create();
        expr.setNextRow(role);
        assertThat(expr.value()).isEqualTo(List.of("statement_timeout=10m"));
    }
}
