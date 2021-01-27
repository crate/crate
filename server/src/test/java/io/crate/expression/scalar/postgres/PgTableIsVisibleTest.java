/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.postgres;

import org.junit.jupiter.api.Test;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.metadata.pgcatalog.OidHash;

public class PgTableIsVisibleTest extends AbstractScalarFunctionsTest {

    @Test
    public void test_null_oid_results_in_null() throws Exception {
        assertEvaluate("pg_table_is_visible(null)", null);
    }

    @Test
    public void test_returns_true_for_table_oid_which_exists_and_is_visible() throws Exception {
        int oid = OidHash.relationOid(usersTable);
        assertEvaluate("pg_table_is_visible(" + oid + ")", null);
    }
}
