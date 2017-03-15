/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.udf;

import com.google.common.collect.ImmutableList;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.operation.udf.UserDefinedFunctionService.putFunction;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public class UserDefinedFunctionServiceTest extends CrateUnitTest {

    UserDefinedFunctionMetaData same1 = new UserDefinedFunctionMetaData(
        "same", "same", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 3}"
    );
    UserDefinedFunctionMetaData same2 = new UserDefinedFunctionMetaData(
        "same", "same", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 2}"
    );
    UserDefinedFunctionMetaData different = new UserDefinedFunctionMetaData(
        "differnt", "different", ImmutableList.of(), DataTypes.INTEGER,
        "javascript", "function(){return 3}"
    );

    @Test
    public void testFirstFunction() throws Exception {
        UserDefinedFunctionsMetaData metaData = putFunction(null, same1, true);
        assertThat(metaData.functions(), hasSize(1));
        assertThat(metaData.functions(), contains(same1));
    }

    @Test
    public void testReplace() throws Exception {
        UserDefinedFunctionsMetaData metaData = putFunction(
            UserDefinedFunctionsMetaData.of(same1),
            same2,
            true
        );
        assertThat(metaData.functions(), hasSize(1));
        assertThat(metaData.functions(), contains(same2));
    }

    @Test
    public void testReplaceIsFalse() throws Exception {
        expectedException.expect(UserDefinedFunctionAlreadyExistsException.class);
        expectedException.expectMessage("User defined Function 'same()' already exists.");
        putFunction(
            UserDefinedFunctionsMetaData.of(same1),
            same2,
            false
        );
    }
}
