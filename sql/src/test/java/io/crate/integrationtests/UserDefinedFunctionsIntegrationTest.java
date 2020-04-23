/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.script.ScriptException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class UserDefinedFunctionsIntegrationTest extends SQLTransportIntegrationTest {

    public static class DummyFunction<InputType> extends Scalar<String, InputType>  {

        private final FunctionInfo info;
        private final Signature signature;
        private final UserDefinedFunctionMetaData metaData;

        private DummyFunction(UserDefinedFunctionMetaData metaData,
                              Signature signature) {
            this.info = new FunctionInfo(new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()), DataTypes.STRING);
            this.signature = signature;
            this.metaData = metaData;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Nullable
        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<InputType>... args) {
            // dummy-lang functions simple print the type of the only argument
            return "DUMMY EATS " + metaData.argumentTypes().get(0).getName();
        }
    }

    public static class DummyLang implements UDFLanguage {

        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData,
                                                   Signature signature) throws ScriptException {
            return new DummyFunction<>(metaData, signature);
        }

        @Override
        public String validate(UserDefinedFunctionMetaData metadata) {
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
        Iterable<UserDefinedFunctionService> udfServices = internalCluster().getInstances(UserDefinedFunctionService.class);
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
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", ImmutableList.of(DataTypes.LONG));

            execute("create function foo(string)" +
                " returns string language dummy_lang as 'function foo(x) { return x; }'");
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", ImmutableList.of(DataTypes.STRING));

            execute("select foo(str) from test order by id asc");
            assertThat(response.rows()[0][0], is("DUMMY EATS text"));

            execute("select foo(id) from test order by id asc");
            assertThat(response.rows()[0][0], is("DUMMY EATS bigint"));
        } finally {
            dropFunction("foo", ImmutableList.of(DataTypes.LONG));
            dropFunction("foo", ImmutableList.of(DataTypes.STRING));
        }
    }

    @Test
    public void testFunctionIsLookedUpInSearchPath() throws Exception {
        sqlExecutor.setSearchPath("firstschema", "secondschema");
        execute("create function secondschema.udf(integer) returns string language dummy_lang as '42'");
        assertFunctionIsCreatedOnAll("secondschema", "udf", ImmutableList.of(DataTypes.INTEGER));

        execute("select udf(1::integer)");
        assertThat(response.rows()[0][0], is("DUMMY EATS integer"));
    }

    @Test
    public void testFunctionIsCreatedInThePgCatalogSchema() throws Exception {
        execute("create function pg_catalog.udf(integer) returns string language dummy_lang as '42'");
        assertFunctionIsCreatedOnAll("pg_catalog", "udf", ImmutableList.of(DataTypes.INTEGER));

        execute("select udf(1::integer)");
        assertThat(response.rows()[0][0], is("DUMMY EATS integer"));
    }

    @Test
    public void testDropFunction() throws Exception {
        execute("create function custom(string) returns string language dummy_lang as 'DUMMY DUMMY DUMMY'");
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "custom", ImmutableList.of(DataTypes.STRING));

        dropFunction("custom", ImmutableList.of(DataTypes.STRING));
        assertFunctionIsDeletedOnAll(sqlExecutor.getCurrentSchema(), "custom", ImmutableList.of(DataTypes.STRING));
    }

    @Test
    public void testNewSchemaWithFunction() throws Exception {
        execute("create function new_schema.custom() returns integer language dummy_lang as 'function custom() {return 1;}'");
        assertFunctionIsCreatedOnAll("new_schema", "custom", ImmutableList.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response.rows()[0][0], is(1L));

        execute("drop function new_schema.custom()");
        assertFunctionIsDeletedOnAll("new_schema", "custom", ImmutableList.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    public void testSelectFunctionsFromRoutines() throws Exception {
        try {
            execute("create function subtract_test(long, long, long) " +
                    "returns long language dummy_lang " +
                    "as 'function subtract_test(a, b, c) { return a - b - c; }'");
            assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(),
                "subtract_test",
                ImmutableList.of(DataTypes.LONG, DataTypes.LONG, DataTypes.LONG)
            );

            execute("select routine_name, routine_body, data_type, routine_definition, routine_schema, specific_name" +
                    " from information_schema.routines " +
                    " where routine_type = 'FUNCTION' and routine_name = 'subtract_test'");
            assertThat(response.rowCount(), is(1L));
            assertThat(response.rows()[0][0], is("subtract_test"));
            assertThat(response.rows()[0][1], is("dummy_lang"));
            assertThat(response.rows()[0][2], is("bigint"));
            assertThat(response.rows()[0][3], is("function subtract_test(a, b, c) { return a - b - c; }"));
            assertThat(response.rows()[0][4], is(sqlExecutor.getCurrentSchema()));
            assertThat(response.rows()[0][5], is("subtract_test(bigint, bigint, bigint)"));
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
        assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "foo", ImmutableList.of(DataTypes.LONG));

        final CountDownLatch latch = new CountDownLatch(50);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (latch.getCount() > 0) {
                try {
                    execute("create function bar(long) returns long language dummy_lang as 'dummy'");
                    assertFunctionIsCreatedOnAll(sqlExecutor.getCurrentSchema(), "bar", ImmutableList.of(DataTypes.LONG));
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

    private void dropFunction(String name, List<DataType> types) throws Exception {
        execute(String.format(Locale.ENGLISH, "drop function %s(\"%s\")",
            name, types.stream().map(DataType::getName).collect(Collectors.joining(", "))));
        assertThat(response.rowCount(), is(1L));
        assertFunctionIsDeletedOnAll(sqlExecutor.getCurrentSchema(), name, types);
    }
}
