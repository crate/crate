/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;


import io.crate.action.sql.SQLActionException;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;

import static org.hamcrest.core.Is.is;

public class PrivilegesValidationIntegrationTest extends BaseUsersIntegrationTest {

    private final DummyLang dummyLang = new DummyLang();

    @Before
    public void beforeTest() {
        Iterable<UserDefinedFunctionService> udfServices = internalCluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
    }

    //set and reset
    @Test
    public void testNormalUserSetGlobalThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "Missing 'DCL' Privilege for user 'normal'");
        executeAsNormalUser("set global persistent license.ident to lala");
    }

    @Test
    public void testNormalUserSetSessionThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "Missing 'DQL' Privilege for user 'normal'");
        executeAsNormalUser("set session search_path to doc");
    }

    @Test
    public void testSuperUserSetGlobalDoesntThrowException() throws Exception {
        executeAsSuperuser("set global persistent license.ident to lala");
        executeAsSuperuser("reset global license.ident");
    }

    //create table test
    @Test
    public void testSuperUserCreateTableDoesntThrowException() throws Exception {
        executeAsSuperuser("create table t1(x long)");
        executeAsSuperuser("drop table t1");
    }

    @Test
    public void testDDLUserCreateTableDoesntThrowException() throws Exception {
        executeAsDDLUser("create table t1(x long)");
        executeAsSuperuser("drop table t1");
    }

    //udf test
    @Test
    public void testDDLUserCreateFunctionDoesntThrowException() throws Exception {
        try {
            executeAsDDLUser("create function foo(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");
        } finally {
            executeAsDDLUser("drop function if exists foo(long)");
        }
    }

    @Test
    public void testDMLUserCreateFunctionThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "Missing 'DDL' Privilege for user 'dmlUser'");
        executeAsDMLUser("create function foo(long)" +
            " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");

    }

    //show create table test
    @Test
    public void testDMLUserShowCreateTableThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "Missing 'DQL' Privilege for user 'dmlUser'");
        executeAsSuperuser("create table t1(x long)");
        executeAsDMLUser("show create table doc.t1");
    }

    @Test
    public void testDMLUserShowCreateTableDoesntThrowException() throws Exception {
        executeAsSuperuser("create table t1(x long)");
        executeAsDQLUser("show create table doc.t1");
        executeAsSuperuser("drop table t1");
    }

    //show columns test
    @Test
    public void testDMLUserShowColumnsThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "Missing 'DQL' Privilege for user 'dmlUser'");
        executeAsSuperuser("create table t1(x long)");
        executeAsDMLUser("show columns in doc.t1");
    }

    @Test
    public void testDMLUserShowColumnsDoesntThrowException() throws Exception {
        executeAsSuperuser("create table t1(x long)");
        executeAsDQLUser("show columns in doc.t1");
    }

    private class DummyFunction<InputType> extends Scalar<BytesRef, InputType> {

        private final FunctionInfo info;
        private final UserDefinedFunctionMetaData metaData;

        private DummyFunction(UserDefinedFunctionMetaData metaData) {
            this.info = new FunctionInfo(new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()), DataTypes.STRING);
            this.metaData = metaData;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public BytesRef evaluate(Input<InputType>... args) {
            // dummy-lang functions simple print the type of the only argument
            return BytesRefs.toBytesRef("DUMMY EATS " + metaData.argumentTypes().get(0).getName());
        }
    }

    private class DummyLang implements UDFLanguage {

        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException {
            return new DummyFunction<>(metaData);
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
}
