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
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.operation.udf.UserDefinedFunctionService.putFunction;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class UserDefinedFunctionServiceTest extends CrateUnitTest {

    private final UserDefinedFunctionMetaData same1 = new UserDefinedFunctionMetaData(
        "same", "same", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 3}"
    );
    private final UserDefinedFunctionMetaData same2 = new UserDefinedFunctionMetaData(
        "same", "same", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 2}"
    );
    private final UserDefinedFunctionMetaData different = new UserDefinedFunctionMetaData(
        "differnt", "different", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 3}"
    );

    @Test
    public void testFirstFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = putFunction(null, same1, true);
        assertThat(metaData.functionsMetaData(), hasSize(1));
        assertThat(metaData.functionsMetaData(), contains(same1));
    }

    @Test
    public void testReplaceExistingFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = putFunction(UserDefinedFunctionsMetaData.of(same1), same2, true);
        assertThat(metaData.functionsMetaData(), hasSize(1));
        assertThat(metaData.functionsMetaData(), contains(same2));
    }

    @Test
    public void testReplaceNotExistingFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData =
            putFunction(UserDefinedFunctionsMetaData.of(same1), different, true);
        assertThat(metaData.functionsMetaData(), hasSize(2));
        assertThat(metaData.functionsMetaData(), containsInAnyOrder(same1, different));
    }

    @Test
    public void testReplaceIsFalse() throws Exception {
        expectedException.expect(UserDefinedFunctionAlreadyExistsException.class);
        expectedException.expectMessage("User defined Function 'same()' already exists.");
        putFunction(UserDefinedFunctionsMetaData.of(same1), same2, false);
    }
}
