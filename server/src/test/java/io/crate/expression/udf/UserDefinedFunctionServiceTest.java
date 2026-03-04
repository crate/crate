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

package io.crate.expression.udf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SchemaMetadata;
import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.types.DataTypes;

public class UserDefinedFunctionServiceTest extends UdfUnitTest {

    private final UserDefinedFunctionMetadata same1 = new UserDefinedFunctionMetadata(
        DocSchemaInfo.NAME, "same", List.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function same(){ return 3; }"
    );

    private final UserDefinedFunctionMetadata different = new UserDefinedFunctionMetadata(
        DocSchemaInfo.NAME, "different", List.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function different() { return 3; }"
    );

    private static final UserDefinedFunctionMetadata FOO = new UserDefinedFunctionMetadata(
        DocSchemaInfo.NAME, "foo", List.of(FunctionArgumentDefinition.of("i", DataTypes.INTEGER)), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function foo(i) { return i; }"
    );

    @Test
    public void testRegisterLanguage() throws Exception {
        udfService.registerLanguage(DUMMY_LANG);
        assertThat(udfService.getLanguage(DUMMY_LANG.name())).isEqualTo(DUMMY_LANG);
    }

    @Test
    public void test_create_replace_and_remove_functions() throws Exception {
        ClusterState state1 = UserDefinedFunctionService.addUDF(same1, true, ClusterState.EMPTY_STATE);
        SchemaMetadata schema1 = state1.metadata().schemas().get(DocSchemaInfo.NAME);
        assertThat(schema1).isNotNull();
        assertThat(schema1.udfs()).containsExactly(same1);

        ClusterState state2 = UserDefinedFunctionService.addUDF(same1, true, state1);
        SchemaMetadata schema2 = state2.metadata().schemas().get(DocSchemaInfo.NAME);
        assertThat(schema2).isNotNull();
        assertThat(schema2.udfs()).containsExactly(same1);

        assertThatThrownBy(() -> UserDefinedFunctionService.addUDF(same1, false, state2))
            .isExactlyInstanceOf(UserDefinedFunctionAlreadyExistsException.class);

        ClusterState state3 = UserDefinedFunctionService.addUDF(different, true, state2);
        SchemaMetadata schema3 = state3.metadata().schemas().get(DocSchemaInfo.NAME);
        assertThat(schema3).isNotNull();
        assertThat(schema3.udfs()).containsExactly(same1, different);

        ClusterState state4 = udfService.dropUDF("doc", "different", List.of(), true, state3);
        SchemaMetadata schema4 = state4.metadata().schemas().get(DocSchemaInfo.NAME);
        assertThat(schema4).isNotNull();
        assertThat(schema4.udfs()).containsExactly(same1);

        assertThatThrownBy(() -> udfService.dropUDF("doc", "different", List.of(), false, state4))
            .isExactlyInstanceOf(UserDefinedFunctionUnknownException.class);

        ClusterState state5 = udfService.dropUDF("doc", "different", List.of(), true, state4);
        assertThat(state5).isEqualTo(state4);
    }


    @Test
    public void test_validate_table_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addUDF(new UserDefinedFunctionMetadata(
                DocSchemaInfo.NAME,
                "foo",
                List.of(FunctionArgumentDefinition.of("i", DataTypes.LONG)),
                DataTypes.LONG,
                DUMMY_LANG.name(),
                "function foo(i) { return i; }"
            ))
            .addTable("create table doc.t1 (id int, gen as foo(id))");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(DocSchemaInfo.NAME, "foo", List.of(DataTypes.INTEGER)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.t1'");

        sqlExecutor.udfService().ensureFunctionIsUnused(DocSchemaInfo.NAME, "foo", List.of(DataTypes.LONG));
    }

    @Test
    public void test_validate_partitioned_table_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addTable("create table doc.p1 (id int, p int, gen as foo(id)) partitioned by (p)");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(DocSchemaInfo.NAME, "foo", List.of(DataTypes.INTEGER)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.p1'");
    }

    @Test
    public void test_validate_sub_columns_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addTable("create table doc.p1 (o object as (id int), gen as foo(o['id'])) partitioned by (o['id'])");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(DocSchemaInfo.NAME, "foo", List.of(DataTypes.INTEGER)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.p1'");
    }
}
