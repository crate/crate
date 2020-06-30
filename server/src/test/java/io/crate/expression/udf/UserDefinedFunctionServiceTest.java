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

package io.crate.expression.udf;

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.Schemas;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class UserDefinedFunctionServiceTest extends UdfUnitTest {

    private final UserDefinedFunctionMetadata same1 = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "same", ImmutableList.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function same(){ return 3; }"
    );
    private final UserDefinedFunctionMetadata same2 = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "same", ImmutableList.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function same() { return 2; }"
    );
    private final UserDefinedFunctionMetadata different = new UserDefinedFunctionMetadata(
        Schemas.DOC_SCHEMA_NAME, "different", ImmutableList.of(), DataTypes.INTEGER,
        DUMMY_LANG.name(), "function different() { return 3; }"
    );

    @Test
    public void testRegisterLanguage() throws Exception {
        udfService.registerLanguage(DUMMY_LANG);
        assertThat(udfService.getLanguage(DUMMY_LANG.name()), is(DUMMY_LANG));
    }

    @Test
    public void testFirstFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = udfService.putFunction(null, same1, true);
        assertThat(metadata.functionsMetadata(), hasSize(1));
        assertThat(metadata.functionsMetadata(), contains(same1));
    }

    @Test
    public void testReplaceExistingFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), same2, true);
        assertThat(metadata.functionsMetadata(), hasSize(1));
        assertThat(metadata.functionsMetadata(), contains(same2));
    }

    @Test
    public void testReplaceNotExistingFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata =
            udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), different, true);
        assertThat(metadata.functionsMetadata(), hasSize(2));
        assertThat(metadata.functionsMetadata(), containsInAnyOrder(same1, different));
    }

    @Test
    public void testRemoveFunction() throws Exception {
        UserDefinedFunctionsMetadata metadata = UserDefinedFunctionsMetadata.of(same1);
        UserDefinedFunctionsMetadata newMetadata = udfService.removeFunction(metadata, same1.schema(), same1.name(), same1.argumentTypes(), false);
        assertThat(metadata, not(is(newMetadata))); // A new instance of metadata must be returned on a change
        assertThat(newMetadata.functionsMetadata().size(), is(0));
    }

    @Test
    public void testRemoveIfExistsEmptyMetadata() throws Exception {
        UserDefinedFunctionsMetadata newMetadata = udfService.removeFunction(null, same1.schema(), same1.name(), same1.argumentTypes(), true);
        assertThat(newMetadata, is(notNullValue()));
    }

    @Test
    public void testRemoveDoesNotExist() throws Exception {
        expectedException.expect(UserDefinedFunctionUnknownException.class);
        expectedException.expectMessage("Cannot resolve user defined function: 'doc.different()'");
        UserDefinedFunctionsMetadata metadata = UserDefinedFunctionsMetadata.of(same1);
        udfService.removeFunction(metadata, different.schema(), different.name(), different.argumentTypes(), false);
    }

    @Test
    public void testReplaceIsFalse() throws Exception {
        expectedException.expect(UserDefinedFunctionAlreadyExistsException.class);
        expectedException.expectMessage("User defined Function 'doc.same()' already exists.");
        udfService.putFunction(UserDefinedFunctionsMetadata.of(same1), same2, false);
    }
}
