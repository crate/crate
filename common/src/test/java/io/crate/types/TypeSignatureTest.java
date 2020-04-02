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

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.List;

import static io.crate.types.TypeSignature.parseTypeSignature;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class TypeSignatureTest extends CrateUnitTest {

    @Test
    public void testParsingOfPrimitiveDataTypes() {
        for (var type : DataTypes.PRIMITIVE_TYPES) {
            assertThat(parseTypeSignature(type.getName()), is(type.getTypeSignature()));
        }
    }

    @Test
    public void testParsingOfArray() {
        ArrayType<Integer> integerArrayType = new ArrayType<>(IntegerType.INSTANCE);
        assertThat(parseTypeSignature("array(integer)"), is(integerArrayType.getTypeSignature()));
    }

    @Test
    public void testParsingOfObject() {
        var signature = parseTypeSignature("object(text, integer)");
        assertThat(signature.getBaseTypeName(), is(ObjectType.NAME));
        assertThat(
            signature.getParameters(),
            contains(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(DataTypes.INTEGER.getName())));
    }

    @Test
    public void testParsingOfNestedArray() {
        var signature = parseTypeSignature("array(object(text, array(integer)))");
        assertThat(signature.getBaseTypeName(), is(ArrayType.NAME));

        var innerObjectTypeSignature = signature.getParameters().get(0);
        assertThat(innerObjectTypeSignature.getBaseTypeName(), is(ObjectType.NAME));
        assertThat(
            innerObjectTypeSignature.getParameters(),
            contains(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(ArrayType.NAME, List.of(new TypeSignature(DataTypes.INTEGER.getName())))));
    }
}
