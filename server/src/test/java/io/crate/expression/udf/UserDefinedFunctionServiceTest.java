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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.Schemas;
import io.crate.types.DataTypes;

public class UserDefinedFunctionServiceTest extends UdfUnitTest {

    private final UserDefinedFunctionMetadata same1 = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "same", List.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function same(){ return 3; }"
    );
    private final UserDefinedFunctionMetadata same2 = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "same", List.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function same() { return 2; }"
    );
    private final UserDefinedFunctionMetadata different = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "different", List.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function different() { return 3; }"
    );
    private static final UserDefinedFunctionMetadata FOO = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "foo", List.of(FunctionArgumentDefinition.of("i", DataTypes.INTEGER)), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function foo(i) { return i; }"
    );

    @Test
    public void testRegisterLanguage() throws Exception {
        udfService.registerLanguage(DUMMY_LANG);
        assertThat(udfService.getLanguage(DUMMY_LANG.name())).isEqualTo(DUMMY_LANG);
    }

    @Test
    public void testFirstFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = udfService.putFunction(null, same1, true);
        assertThat(metadata.functionsMetadata()).hasSize(1);
        assertThat(metadata.functionsMetadata()).containsExactly(same1);
    }

    @Test
    public void testReplaceExistingFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), same2, true);
        assertThat(metadata.functionsMetadata()).hasSize(1);
        assertThat(metadata.functionsMetadata()).containsExactly(same2);
    }

    @Test
    public void testReplaceNotExistingFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata =
            udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), different, true);
        assertThat(metadata.functionsMetadata()).hasSize(2);
        assertThat(metadata.functionsMetadata(), containsInAnyOrder(same1, different));
    }

    @Test
    public void testRemoveFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = UserDefinedFunctionsMetadata.of(same1);
        UserDefinedFunctionsMetadata newMetadata = udfService.removeFunction(metadata, same1.schema(), same1.name(), same1.argumentTypes(), false);
        assertThat(metadata).isNotEqualTo(newMetadata); // A new instance of metadata must be returned on a change
        assertThat(newMetadata.functionsMetadata()).hasSize(0);
    }

    @Test
    public void testRemoveIfExistsEmptyMetadata() throws Exception {
        UserDefinedFunctionsMetadata newMetadata = udfService.removeFunction(null, same1.schema(), same1.name(), same1.argumentTypes(), true);
        assertThat(newMetadata).isNotNull();
    }

    @Test
    public void testRemoveDoesNotExist() throws Exception {
        UserDefinedFunctionsMetadata metadata = UserDefinedFunctionsMetadata.of(same1);
        assertThatThrownBy(() ->
                udfService.removeFunction(metadata, different.schema(), different.name(), different.argumentTypes(), false))
            .isExactlyInstanceOf(UserDefinedFunctionUnknownException.class)
            .hasMessage("Cannot resolve user defined function: 'doc.different()'");
    }

    @Test
    public void testReplaceIsFalse() throws Exception {
        assertThatThrownBy(() -> udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), same2, false))
            .isExactlyInstanceOf(UserDefinedFunctionAlreadyExistsException.class)
            .hasMessage("User defined Function 'doc.same()' already exists.");
    }

    @Test
    public void test_validate_table_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addUDF(new UserDefinedFunctionMetadata(
                Schemas.DOC_SCHEMA_NAME,
                "foo",
                List.of(FunctionArgumentDefinition.of("i", DataTypes.LONG)),
                DataTypes.LONG,
                DUMMY_LANG.name(),
                "function foo(i) { return i; }"
            ))
            .addTable("create table doc.t1 (id int, gen as foo(id))");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(Schemas.DOC_SCHEMA_NAME, "foo", List.of(DataTypes.LONG)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.t1'");

        sqlExecutor.udfService().ensureFunctionIsUnused(Schemas.DOC_SCHEMA_NAME, "foo", List.of(DataTypes.INTEGER));
    }

    @Test
    public void test_validate_partitioned_table_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addPartitionedTable("create table doc.p1 (id int, p int, gen as foo(id)) partitioned by (p)");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(Schemas.DOC_SCHEMA_NAME, "foo", List.of(DataTypes.INTEGER)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.p1'");
    }

    @Test
    public void test_validate_sub_columns_while_dropping_udf() throws Exception {
        sqlExecutor
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(FOO)
            .addPartitionedTable("create table doc.p1 (o object as (id int), gen as foo(o['id'])) partitioned by (o['id'])");

        assertThatThrownBy(() -> udfService.ensureFunctionIsUnused(Schemas.DOC_SCHEMA_NAME, "foo", List.of(DataTypes.INTEGER)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot drop function 'doc.foo'. It is in use by column 'gen' of table 'doc.p1'");
    }
}
