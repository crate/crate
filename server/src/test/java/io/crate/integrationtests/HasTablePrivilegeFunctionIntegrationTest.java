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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.pgcatalog.OidHash;

public class HasTablePrivilegeFunctionIntegrationTest extends IntegTestCase {

    @Test
    public void test_has_table_privilege_function_with_system_table() {
        execute("create user john");

        execute("select has_table_privilege('john', 'sys.summits', 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(false);

        execute("grant dql on table sys.summits to john");

        execute("select has_table_privilege('john', 'sys.summits', 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(true);

        final RelationName sysSummits = new RelationName("sys", "summits");
        final int sysSummitsOid = OidHash.relationOid(OidHash.Type.TABLE, sysSummits);
        execute("select has_table_privilege('john', " + sysSummitsOid + ", 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(true);
    }

    @Test
    public void test_has_table_privilege_function_with_view() {
        execute("create user john");
        execute("create view doc.v as select * from sys.summits");

        execute("select has_table_privilege('john', 'v', 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(false);

        execute("grant dql on view doc.v to john");

        execute("select has_table_privilege('john', 'v', 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(true);

        final RelationName view = new RelationName("doc", "v");
        final int viewOid = OidHash.relationOid(OidHash.Type.VIEW, view);
        execute("select has_table_privilege('john', " + viewOid + ", 'usage')");
        assertThat(response.rows()[0][0]).isEqualTo(true);
    }
}
