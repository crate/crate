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

import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;
import static io.crate.testing.Asserts.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewMetadata;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.types.StringType;

public class MixedCaseSchemaAndRelationNamesIntegrationTest extends IntegTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        var dummyLang = new UserDefinedFunctionsIntegrationTest.DummyLang();
        Iterable<UserDefinedFunctionService> udfServices = cluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
    }

    @Test
    public void test_mixed_cased_identifiers_are_persisted_to_metadata() {
        execute("create table \"Abc\".\"T\" (a int) partitioned by (a)");
        execute("insert into \"Abc\".\"T\" values (1)");
        execute("create view \"Abc\".v1 as select a from \"Abc\".\"T\"");
        execute("CREATE FUNCTION \"Abc\".func(string) RETURNS STRING LANGUAGE dummy_lang AS 'DUMMY EATS text'");
        refresh();

        // check index/template names
        var meta = clusterService().state().metadata();
        assertThat(meta.indices().keysIt().next()).isEqualTo("Abc..partitioned.T.04132");
        assertThat(meta.templates().keysIt().next()).isEqualTo("Abc..partitioned.T.");

        // check viewMetadata names as well as its target query
        ViewsMetadata viewsMetadata = meta.custom(ViewsMetadata.TYPE);
        ViewMetadata viewMetadata = viewsMetadata.getView(new RelationName("Abc", "v1"));
        assertThat(viewMetadata).isNotNull();
        assertThat(viewMetadata.stmt()).isEqualTo(
            """
                SELECT "a"
                FROM "Abc"."T"
                """
        );

        // check udfMetadata for proper schema name
        UserDefinedFunctionsMetadata userDefinedFunctionsMetadata = meta.custom(UserDefinedFunctionsMetadata.TYPE);
        assertThat(userDefinedFunctionsMetadata.contains("Abc", "func", List.of(StringType.INSTANCE))).isTrue();

        // a little more complex scenario involving schema names with upper cases
        execute("create table Abc.\"T\" (b string, c string as \"Abc\".func(b)) partitioned by (c)");
        execute("insert into Abc.\"T\"(b) values ('Abc')"); // NOTE: here failed before due to - Unknown function: abc.func(abc.t.b)
        refresh();

        execute("select * from Abc.\"T\"");
        assertThat(printedTable(response.rows())).isEqualTo("Abc| DUMMY EATS text\n");

        assertThatThrownBy(() -> execute("drop function \"Abc\".func(string)"))
            .hasMessageContaining("Cannot drop function 'Abc.func(text)', it is still in use by 'abc.T.c AS \"Abc\".func(b)'");
    }
}
