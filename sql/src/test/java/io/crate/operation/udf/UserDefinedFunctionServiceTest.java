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

package io.crate.operation.udf;

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.script.ScriptException;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class UserDefinedFunctionServiceTest extends CrateUnitTest {

    private UserDefinedFunctionService udfService;

    private static final UDFLanguage DUMMY_LANG = new UDFLanguage() {
        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException {
            return null;
        }

        @Nullable
        @Override
        public String validate(UserDefinedFunctionMetaData metadata) {
            return null;
        }

        @Override
        public String name() {
            return "DummyScript";
        }
    };

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        udfService = new UserDefinedFunctionService(Settings.EMPTY, mock(ClusterService.class));
    }

    private final UserDefinedFunctionMetaData same1 = new UserDefinedFunctionMetaData(
        Schemas.DEFAULT_SCHEMA_NAME, "same", ImmutableList.of(), DataTypes.INTEGER,
        "dummy_lang", "function same(){ return 3; }"
    );
    private final UserDefinedFunctionMetaData same2 = new UserDefinedFunctionMetaData(
        Schemas.DEFAULT_SCHEMA_NAME, "same", ImmutableList.of(), DataTypes.INTEGER,
        "dummy_lang", "function same() { return 2; }"
    );
    private final UserDefinedFunctionMetaData different = new UserDefinedFunctionMetaData(
        Schemas.DEFAULT_SCHEMA_NAME, "different", ImmutableList.of(), DataTypes.INTEGER,
        "dummy_lang", "function different() { return 3; }"
    );

    @Test
    public void testRegisterLanguageUdfEnabled() throws Exception {
        Settings settings = Settings.builder()
            .put("udf.enabled", true)
            .build();
        UserDefinedFunctionService udfSrv = new UserDefinedFunctionService(settings, mock(ClusterService.class));
        udfSrv.registerLanguage(DUMMY_LANG);
        assertThat(udfSrv.getLanguage(DUMMY_LANG.name()), is(DUMMY_LANG));
    }

    @Test
    public void testRegisterLanguageUdfDisable() throws Exception {
        Settings settings = Settings.builder()
            .put("udf.enabled", false)
            .build();
        UserDefinedFunctionService udfSrv = new UserDefinedFunctionService(settings, mock(ClusterService.class));
        // registerLanguage() does not fail, because modules implementing a language should not care about setting
        udfSrv.registerLanguage(DUMMY_LANG);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'DummyScript' is not a valid UDF language");
        udfSrv.getLanguage(DUMMY_LANG.name());
    }
    @Test
    public void testFirstFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = udfService.putFunction(null, same1, true);
        assertThat(metaData.functionsMetaData(), hasSize(1));
        assertThat(metaData.functionsMetaData(), contains(same1));
    }

    @Test
    public void testReplaceExistingFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = udfService.putFunction(UserDefinedFunctionsMetaData.of(same1), same2, true);
        assertThat(metaData.functionsMetaData(), hasSize(1));
        assertThat(metaData.functionsMetaData(), contains(same2));
    }

    @Test
    public void testReplaceNotExistingFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData =
            udfService.putFunction(UserDefinedFunctionsMetaData.of(same1), different, true);
        assertThat(metaData.functionsMetaData(), hasSize(2));
        assertThat(metaData.functionsMetaData(), containsInAnyOrder(same1, different));
    }

    @Test
    public void testRemoveFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = UserDefinedFunctionsMetaData.of(same1);
        UserDefinedFunctionsMetaData newMetaData = udfService.removeFunction(metaData, same1.schema(), same1.name(), same1.argumentTypes(), false);
        assertThat(metaData, not(is(newMetaData))); // A new instance of metaData must be returned on a change
        assertThat(newMetaData.functionsMetaData().size(), is(0));
    }

    @Test
    public void testRemoveIfExistsEmptyMetaData() throws Exception {
        UserDefinedFunctionsMetaData newMetaData = udfService.removeFunction(null, same1.schema(), same1.name(), same1.argumentTypes(), true);
        assertThat(newMetaData, is(notNullValue()));
    }

    @Test
    public void testRemoveDoesNotExist() throws Exception {
        expectedException.expect(UserDefinedFunctionUnknownException.class);
        expectedException.expectMessage("Cannot resolve user defined function: 'doc.different()'");
        UserDefinedFunctionsMetaData metaData = UserDefinedFunctionsMetaData.of(same1);
        udfService.removeFunction(metaData, different.schema(), different.name(), different.argumentTypes(), false);
    }

    @Test
    public void testReplaceIsFalse() throws Exception {
        expectedException.expect(UserDefinedFunctionAlreadyExistsException.class);
        expectedException.expectMessage("User defined Function 'doc.same()' already exists.");
        udfService.putFunction(UserDefinedFunctionsMetaData.of(same1), same2, false);
    }
}
