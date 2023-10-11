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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Input;
import io.crate.expression.scalar.timestamp.CurrentTimeFunction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.testing.Asserts;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class UserDefinedFunctionsIntegrationTest extends IntegTestCase {

    public static class DummyFunction<InputType> extends Scalar<String, InputType> {

        private final UserDefinedFunctionMetadata metadata;

        private DummyFunction(UserDefinedFunctionMetadata metadata,
                              Signature signature,
                              BoundSignature boundSignature) {
            super(signature, boundSignature);
            this.metadata = metadata;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<InputType>... args) {
            // dummy-lang functions simple print the type of the only argument
            return "DUMMY EATS " + metadata.argumentTypes().get(0).getName();
        }
    }

    public static class DummyLang implements UDFLanguage {

        @Override
        public Scalar<?, ?> createFunctionImplementation(UserDefinedFunctionMetadata metadata,
                                                         Signature signature,
                                                         BoundSignature boundSignature) {
            return new DummyFunction<>(metadata, signature, boundSignature);
        }

        @Override
        public String validate(UserDefinedFunctionMetadata metadata) {
            // dummy language does not validate anything
            return null;
        }

        @Override
        public String name() {
            return "dummy_lang";
        }
    }

    private final DummyLang dummyLang = new DummyLang();

    @Before
    public void beforeTest() {
        // clustering by id into two shards must assure that the two inserted
        // records reside on two different nodes configured in the test setup.
        // So then it would be possible to test that a function is created and
        // applied on all of nodes.
        Iterable<UserDefinedFunctionService> udfServices = cluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
    }

    @Test
    public void testCreateOverloadedFunction() throws Exception {
        execute("create table test (id long, str string) clustered by(id) into 2 shards");
        Object[][] rows = new Object[10][];
        for (int i = 0; i < 10; i++) {
            rows[i] = new Object[]{(long) i, String.valueOf(i)};
        }
        execute("insert into test (id, str) values (?, ?)", rows);
        refresh();
        try {
            execute("create function foo(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", List.of(DataTypes.LONG));

            execute("create function foo(string)" +
                " returns string language dummy_lang as 'function foo(x) { return x; }'");
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", List.of(DataTypes.STRING));

            execute("select foo(str) from test order by id asc");
            assertThat(response.rows()[0][0]).isEqualTo("DUMMY EATS text");

            execute("select foo(id) from test order by id asc");
            assertThat(response.rows()[0][0]).isEqualTo("DUMMY EATS bigint");
        } finally {
            dropFunction("foo", List.of(Literal.of(1L)));
            dropFunction("foo", List.of(Literal.of("dummy")));
        }
    }

    @Test
    public void testFunctionIsLookedUpInSearchPath() throws Exception {
        sqlExecutor.setSearchPath("firstschema", "secondschema");
        execute("create function secondschema.udf(integer) returns string language dummy_lang as '42'");
        assertFunctionIsCreatedOnAll("secondschema", "udf", List.of(DataTypes.INTEGER));

        execute("select udf(1::integer)");
        assertThat(response).hasRows("DUMMY EATS integer");
    }

    @Test
    public void testFunctionIsCreatedInThePgCatalogSchema() throws Exception {
        execute("create function pg_catalog.udf(integer) returns string language dummy_lang as '42'");
        assertFunctionIsCreatedOnAll("pg_catalog", "udf", List.of(DataTypes.INTEGER));

        execute("select udf(1::integer)");
        assertThat(response).hasRows("DUMMY EATS integer");
    }

    @Test
    public void testDropFunction() throws Exception {
        execute("create function custom(string) returns string language dummy_lang as 'DUMMY DUMMY DUMMY'");
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "custom", List.of(DataTypes.STRING));

        dropFunction("custom", List.of(Literal.of("foo")));
        assertFunctionIsDeletedOnAll(sqlExecutor.getCurrentSchema(), "custom", List.of(Literal.of("foo")));
    }

    @Test
    public void testNewSchemaWithFunction() throws Exception {
        execute("create function new_schema.custom() returns integer language dummy_lang as 'function custom() {return 1;}'");
        assertFunctionIsCreatedOnAll("new_schema", "custom", List.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response).hasRows("1");

        execute("drop function new_schema.custom()");
        assertFunctionIsDeletedOnAll("new_schema", "custom", List.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response).hasRows("0");
    }

    @Test
    public void testSelectFunctionsFromRoutines() throws Exception {
        try {
            execute("create function subtract_test(long, long, long) " +
                    "returns long language dummy_lang " +
                    "as 'function subtract_test(a, b, c) { return a - b - c; }'");
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(),
                "subtract_test",
                List.of(DataTypes.LONG, DataTypes.LONG, DataTypes.LONG)
            );

            execute("select routine_name, routine_body, data_type, routine_definition, routine_schema, specific_name" +
                    " from information_schema.routines " +
                    " where routine_type = 'FUNCTION' and routine_name = 'subtract_test'");
            assertThat(response).hasRowCount(1);
            assertThat(response.rows()[0][0]).isEqualTo("subtract_test");
            assertThat(response.rows()[0][1]).isEqualTo("dummy_lang");
            assertThat(response.rows()[0][2]).isEqualTo("bigint");
            assertThat(response.rows()[0][3]).isEqualTo("function subtract_test(a, b, c) { return a - b - c; }");
            assertThat(response.rows()[0][4]).isEqualTo(sqlExecutor.getCurrentSchema());
            assertThat(response.rows()[0][5]).isEqualTo("subtract_test(bigint, bigint, bigint)");
        } finally {
            execute("drop function if exists subtract_test(long, long, long)");
        }
    }

    @Test
    public void testConcurrentFunctionRegistering() throws Throwable {
        // This test creates a function which is executed repeatedly while another function
        // is created and dropped on the same schema. It proves that creating and dropping
        // functions doesn't affect already registered functions.
        execute("create function foo(long) returns string language dummy_lang as 'f doo()'");
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", List.of(DataTypes.LONG));

        final CountDownLatch latch = new CountDownLatch(50);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (latch.getCount() > 0) {
                try {
                    execute("create function bar(long) returns long language dummy_lang as 'dummy'");
                    assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "bar", List.of(DataTypes.LONG));
                    execute("drop function bar(long)");
                } catch (Exception e) {
                    lastThrowable.set(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            while (latch.getCount() > 0) {
                execute("select foo(5)");
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            execute("DROP FUNCTION foo(long)");
            execute("DROP FUNCTION IF EXISTS bar(long)");
            Throwable throwable = lastThrowable.get();
            if (throwable != null) {
                throw throwable;
            }
        }

    }

    private void dropFunction(String name, List<Symbol> arguments) throws Exception {
        var types = Symbols.typeView(arguments);
        execute(String.format(Locale.ENGLISH, "drop function %s(\"%s\")",
            name, types.stream().map(DataType::getName).collect(Collectors.joining(", "))));
        assertThat(response).hasRowCount(1);
        assertFunctionIsDeletedOnAll(sqlExecutor.getCurrentSchema(), name, arguments);
    }

    @Test
    public void test_pg_function_is_visible() throws Exception {
        Signature signature = Signature
            .builder()
            .kind(FunctionType.SCALAR)
            .name(new FunctionName(Schemas.DOC_SCHEMA_NAME, "my_func"))
            .argumentTypes(
                TypeSignature.parse("array(array(integer))"),
                TypeSignature.parse("integer"),
                TypeSignature.parse("text"))
            .returnType(TypeSignature.parse("text"))
            .build();
        int functionOid = OidHash.functionOid(signature);

        execute("select pg_function_is_visible(" + functionOid + ")");
        assertThat(response).hasRows("false");

        execute("create function doc.my_func(array(array(integer)), integer, text) returns text language dummy_lang as '42'");

        execute("select pg_function_is_visible(" + functionOid + ")");
        assertThat(response).hasRows("true");

        execute("drop function doc.my_func(array(array(integer)), integer, text)");
        execute("select pg_function_is_visible(" + functionOid + ")");
        assertThat(response).hasRows("false");
    }

    @Test
    public void test_pg_get_function_result() throws Exception {
        TypeSignature returnTypeSig = TypeSignature.parse("array(array(integer))");
        String returnType = returnTypeSig.toString();
        Signature signature = Signature
            .builder()
            .kind(FunctionType.SCALAR)
            .name(new FunctionName(Schemas.DOC_SCHEMA_NAME, "make_2d_array"))
            .argumentTypes(DataTypes.INTEGER.getTypeSignature())
            .returnType(returnTypeSig)
            .build();
        int functionOid = OidHash.functionOid(signature);

        execute("select pg_get_function_result(?)", new Object[]{functionOid});
        assertThat(response.rows()[0][0]).isNull();

        execute("create function doc.make_2d_array(integer) returns array(array(integer)) language dummy_lang as ?", new Object[]{returnType});

        execute("select pg_get_function_result(" + functionOid + ")");
        assertThat(response).hasRows(returnType);

        execute("drop function doc.make_2d_array(integer)");
        execute("select pg_get_function_result(" + functionOid + ")");
        assertThat(response.rows()[0][0]).isNull();
    }

    @Test
    public void test_pg_function_is_visible_when_oid_is_retrieved_from_column() throws Exception {
        Signature signature = Signature
            .builder()
            .kind(FunctionType.SCALAR)
            .name(new FunctionName(null, CurrentTimeFunction.NAME))
            .argumentTypes()
            .returnType(DataTypes.TIMETZ.getTypeSignature())
            .build();
        int functionOid = OidHash.functionOid(signature);

        execute("create table oid_test(oid integer)");
        execute("insert into oid_test values(" + functionOid + ")");
        execute("refresh table oid_test");
        execute("select pg_function_is_visible(t.oid) from oid_test t");
        assertThat(response).hasRows("true");
        execute("drop table oid_test");
    }

    @Test
    public void test_udf_used_inside_generated_column_definition_cannot_be_dropped() {
        execute("create function doc.foo(long) returns string language dummy_lang as" +
            " 'function foo(a) { return a; }'");
        execute("create table doc.t1 (id long, l as doc.foo(id))");

        Asserts.assertSQLError(() -> execute("drop function doc.foo(long)"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(BAD_REQUEST, 4000)
                .hasMessageContaining(
                        "Cannot drop function 'doc.foo(bigint)', it is still in use by 'doc.t1.l AS doc.foo(id)'");
    }
}
