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

import java.util.Collection;
import java.util.List;

import org.junit.Test;

import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class PgUserTableTest extends CrateDummyClusterServiceUnitTest {

    private static Roles rolesOf(Collection<Role> roleList) {
        return () -> roleList;
    }

    @Test
    public void test_table_has_exactly_9_columns() {
        var table = PgUserTable.create(rolesOf(List.of()));
        assertThat(table.rootColumns()).hasSize(9);
    }
}
