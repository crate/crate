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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.List;

import static io.crate.common.collections.Lists2.getOnlyElement;
import static io.crate.types.TypeSignature.parseTypeSignature;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class TypeSignatureTest extends ESTestCase {

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

    @Test
    public void test_parse_record() {
        var signature = parseTypeSignature("record(text, integer)");

        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
        assertThat(
            signature.getParameters(),
            contains(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(DataTypes.INTEGER.getName())));
    }

    @Test
    public void test_parse_record_with_named_data_type() {
        var signature = parseTypeSignature("record(field1 text)");

        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.parameterName(), is("field1"));
        assertThat(innerSignature.getBaseTypeName(), is(DataTypes.STRING.getName()));
    }

    @Test
    public void test_parse_record_with_named_data_types_that_contain_whitespaces() {
        var signature = parseTypeSignature("record(field1 double precision)");

        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.parameterName(), is("field1"));
        assertThat(innerSignature.getBaseTypeName(), is(DataTypes.DOUBLE.getName()));
    }

    @Test
    public void test_parse_array_with_nested_record_type() {
        var signature = parseTypeSignature("array(record(double precision))");

        assertThat(signature.getBaseTypeName(), is(ArrayType.NAME));
        assertThat(
            signature.getParameters(),
            contains(
                new TypeSignature(
                    RowType.NAME,
                    List.of(new TypeSignature(DataTypes.DOUBLE.getName()))
                )
            )
        );
    }

    @Test
    public void test_parse_record_with_nested_named_record_type() {
        var signature = parseTypeSignature("record(field1 record(timestamp without time zone))");

        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.parameterName(), is("field1"));
        assertThat(innerSignature.getBaseTypeName(), is(RowType.NAME));
        assertThat(
            innerSignature.getParameters(),
            contains(new TypeSignature(DataTypes.TIMESTAMP.getName())));
    }

    @Test
    public void test_parse_text_type_signature_with_length_limit() {
        var signature = parseTypeSignature("text(12)");
        assertThat(signature.getBaseTypeName(), is("text"));
        assertThat(signature.getParameters(), contains(new IntegerLiteralTypeSignature(12)));
    }

    @Test
    public void test_create_type_signature_from_text_type_with_length_limit() {
        assertThat(StringType.of(11).getTypeSignature().toString(), is("text(11)"));
    }

    @Test
    public void test_parse_nested_named_text_type_signature_with_length_limit() {
        var signature = parseTypeSignature("object(name text(11))");
        assertThat(signature.getBaseTypeName(), is("object"));
        assertThat(signature.getParameters().size(), is(1));

        var textTypeSignature = signature.getParameters().get(0);
        assertThat(textTypeSignature.getBaseTypeName(), is("text"));
        assertThat(textTypeSignature.getParameters(), contains(new IntegerLiteralTypeSignature(11)));
    }

    @Test
    public void test_create_type_signature_from_nested_named_text_type_with_length_limit() {
        var objectType = ObjectType.builder()
            .setInnerType("name", StringType.of(1))
            .build();
        assertThat(objectType.getTypeSignature().toString(), is("object(text,name text(1))"));
    }

    @Test
    public void test_parse_numeric_type_signature_round_trip() {
        var signature = parseTypeSignature("numeric");
        assertThat(signature.getBaseTypeName(), is("numeric"));
        assertThat(signature.getParameters().size(), is(0));
        assertThat(signature.createType(), is(NumericType.INSTANCE));
    }

    @Test
    public void test_parse_numeric_type_signature_with_precision_round_trip() {
        var signature = parseTypeSignature("numeric(1)");
        assertThat(signature.getBaseTypeName(), is("numeric"));
        assertThat(signature.getParameters(), contains(new IntegerLiteralTypeSignature(1)));
        assertThat(signature.createType(), is(NumericType.of(1)));
    }

    @Test
    public void test_parse_numeric_type_signature_with_precision_and_scale_round_trip() {
        var signature = parseTypeSignature("numeric(1, 2)");
        assertThat(signature.getBaseTypeName(), is("numeric"));
        assertThat(
            signature.getParameters(),
            contains(new IntegerLiteralTypeSignature(1), new IntegerLiteralTypeSignature(2)));
        assertThat(signature.createType(), is(NumericType.of(1, 2)));
    }
}
