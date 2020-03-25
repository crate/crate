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

import static io.crate.types.TypeSignature.parseTypeSignature;
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
        ObjectType objectType = ObjectType.builder().setInnerType("V", IntegerType.INSTANCE).build();
        assertThat(parseTypeSignature("object(text, integer)"), is(objectType.getTypeSignature()));
    }

    @Test
    public void testParsingOfNestedArray() {
        var type = new ArrayType<>(
            ObjectType.builder()
                .setInnerType("x", new ArrayType<>(IntegerType.INSTANCE))
            .build()
        );
        assertThat(parseTypeSignature("array(object(text, array(integer)))"), is(type.getTypeSignature()));
    }
}
