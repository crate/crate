/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.parser;

import static io.crate.common.collections.Lists.getOnlyElement;
import static io.crate.types.TypeSignature.parse;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerLiteralTypeSignature;
import io.crate.types.IntegerType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.ParameterTypeSignature;
import io.crate.types.RowType;
import io.crate.types.StringType;
import io.crate.types.TypeSignature;


public class TypeSignatureParserTest extends ESTestCase {

    @Test
    public void testParsingOfPrimitiveDataTypes() {
        for (var type : DataTypes.PRIMITIVE_TYPES) {
            assertThat(TypeSignature.parse(type.getName())).isEqualTo(type.getTypeSignature());
        }
    }

    @Test
    public void testParsingOfArray() {
        ArrayType<Integer> integerArrayType = new ArrayType<>(IntegerType.INSTANCE);
        assertThat(TypeSignature.parse("array(integer)")).isEqualTo(integerArrayType.getTypeSignature());
    }

    @Test
    public void testParsingOfObject() {
        var signature = TypeSignature.parse("object(text, integer)");
        assertThat(signature.getBaseTypeName()).isEqualTo(ObjectType.NAME);
        assertThat(signature.getParameters()).containsExactly(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(DataTypes.INTEGER.getName()));
    }

    @Test
    public void testParsingOfNestedArray() {
        var signature = TypeSignature.parse("array(object(text, array(integer)))");
        assertThat(signature.getBaseTypeName()).isEqualTo(ArrayType.NAME);

        var innerObjectTypeSignature = signature.getParameters().getFirst();
        assertThat(innerObjectTypeSignature.getBaseTypeName()).isEqualTo(ObjectType.NAME);
        assertThat(innerObjectTypeSignature.getParameters()).containsExactly(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(ArrayType.NAME, List.of(new TypeSignature(DataTypes.INTEGER.getName()))));
    }

    @Test
    public void test_parse_record() {
        var signature = TypeSignature.parse("record(text, integer)");

        assertThat(signature.getBaseTypeName()).isEqualTo(RowType.NAME);
        assertThat(signature.getParameters()).containsExactly(
                new TypeSignature(DataTypes.STRING.getName()),
                new TypeSignature(DataTypes.INTEGER.getName()));
    }

    @Test
    public void test_parse_record_with_named_data_type() {
        var signature = TypeSignature.parse("record(field1 text)");

        assertThat(signature.getBaseTypeName()).isEqualTo(RowType.NAME);
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.unescapedParameterName()).isEqualTo("field1");
        assertThat(innerSignature.getBaseTypeName()).isEqualTo(DataTypes.STRING.getName());
    }

    @Test
    public void test_parse_record_with_named_data_types_that_contain_whitespaces() {
        var signature = TypeSignature.parse("record(field1 double precision)");

        assertThat(signature.getBaseTypeName()).isEqualTo(RowType.NAME);
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.unescapedParameterName()).isEqualTo("field1");
        assertThat(innerSignature.getBaseTypeName()).isEqualTo(DataTypes.DOUBLE.getName());
    }

    @Test
    public void test_parse_array_with_nested_record_type() {
        var signature = TypeSignature.parse("array(record(double precision))");

        assertThat(signature.getBaseTypeName()).isEqualTo(ArrayType.NAME);
        assertThat(signature.getParameters()).containsExactly(
                new TypeSignature(
                    RowType.NAME,
                    List.of(new TypeSignature(DataTypes.DOUBLE.getName()))));
    }

    @Test
    public void test_parse_record_with_nested_named_record_type() {
        var signature = TypeSignature.parse("record(field1 record(timestamp without time zone))");

        assertThat(signature.getBaseTypeName()).isEqualTo(RowType.NAME);
        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
        assertThat(innerSignature.unescapedParameterName()).isEqualTo("field1");
        assertThat(innerSignature.getBaseTypeName()).isEqualTo(RowType.NAME);
        assertThat(innerSignature.getParameters()).containsExactly(new TypeSignature(DataTypes.TIMESTAMP.getName()));
    }

    @Test
    public void test_parse_text_type_signature_with_length_limit() {
        var signature = TypeSignature.parse("text(12)");
        assertThat(signature.getBaseTypeName()).isEqualTo("text");
        assertThat(signature.getParameters()).containsExactly(new IntegerLiteralTypeSignature(12));
    }

    @Test
    public void test_create_type_signature_from_text_type_with_length_limit() {
        assertThat(StringType.of(11).getTypeSignature().toString()).isEqualTo("text(11)");
    }

    @Test
    public void test_parse_nested_named_text_type_signature_with_length_limit() {
        var signature = TypeSignature.parse("object(name text(11))");
        assertThat(signature.getBaseTypeName()).isEqualTo("object");
        assertThat(signature.getParameters()).hasSize(1);

        var textTypeSignature = signature.getParameters().getFirst();
        assertThat(textTypeSignature.getBaseTypeName()).isEqualTo("text");
        assertThat(textTypeSignature.getParameters()).containsExactly(new IntegerLiteralTypeSignature(11));
    }

    @Test
    public void test_create_type_signature_from_nested_named_text_type_with_length_limit() {
        var objectType = ObjectType.builder()
            .setInnerType("name", StringType.of(1))
            .build();
        assertThat(objectType.getTypeSignature().toString()).isEqualTo("object(text,\"name\" text(1))");
    }

    @Test
    public void test_parse_numeric_type_signature_round_trip() {
        var signature = TypeSignature.parse("numeric");
        assertThat(signature.getBaseTypeName()).isEqualTo("numeric");
        assertThat(signature.getParameters()).isEmpty();
        assertThat(signature.createType()).isEqualTo(NumericType.INSTANCE);
    }

    @Test
    public void test_parse_numeric_type_signature_with_precision_round_trip() {
        var signature = TypeSignature.parse("numeric(1)");
        assertThat(signature.getBaseTypeName()).isEqualTo("numeric");
        assertThat(signature.getParameters()).containsExactly(new IntegerLiteralTypeSignature(1));
        assertThat(signature.createType()).isEqualTo(NumericType.of(1));
    }

    @Test
    public void test_parse_numeric_type_signature_with_precision_and_scale_round_trip() {
        var signature = TypeSignature.parse("numeric(1, 2)");
        assertThat(signature.getBaseTypeName()).isEqualTo("numeric");
        assertThat(signature.getParameters()).containsExactly(
            new IntegerLiteralTypeSignature(1), new IntegerLiteralTypeSignature(2));
        assertThat(signature.createType()).isEqualTo(NumericType.of(1, 2));
    }

    @Test
    public void test_create_and_parse_object_type_containing_parameter_name_with_spaces() {
        var type = ObjectType.builder()
            .setInnerType("first field", DataTypes.STRING)
            .build();
        var signature = type.getTypeSignature();
        assertThat(signature.toString()).isEqualTo("object(text,\"first field\" text)");
        var parsedSignature = TypeSignature.parse(signature.toString());
        assertThat(parsedSignature).isEqualTo(signature);
        assertThat(parsedSignature.createType()).isEqualTo(type);
    }

    @Test
    public void test_create_and_parse_object_type_containing_parameter_name_with_bracket() {
        var type = ObjectType.builder()
            .setInnerType("()))", DataTypes.STRING)
            .build();
        var signature = type.getTypeSignature();
        assertThat(signature.toString()).isEqualTo("object(text,\"()))\" text)");
        var parsedSignature = TypeSignature.parse(signature.toString());
        assertThat(parsedSignature).isEqualTo(signature);
        assertThat(parsedSignature.createType()).isEqualTo(type);
    }

    @Test
    public void test_create_and_parse_object_type_containing_parameter_name_with_spaces_and_brackets() {
        var type = ObjectType.builder()
            .setInnerType("foo ()))", DataTypes.STRING)
            .build();
        var signature = type.getTypeSignature();
        assertThat(signature.toString()).isEqualTo("object(text,\"foo ()))\" text)");
        var parsedSignature = TypeSignature.parse(signature.toString());
        assertThat(parsedSignature).isEqualTo(signature);
        assertThat(parsedSignature.createType()).isEqualTo(type);
    }

    @Test
    public void test_create_and_parse_object_type_containing_parameter_name_with_special_characters() {
        var type = ObjectType.builder()
            .setInnerType("foo # !!::\\n ''", DataTypes.STRING)
            .build();
        var signature = type.getTypeSignature();
        assertThat(signature.toString()).isEqualTo("object(text,\"foo # !!::\\n '\'\" text)");
        var parsedSignature = TypeSignature.parse(signature.toString());
        assertThat(parsedSignature).isEqualTo(signature);
        assertThat(parsedSignature.createType()).isEqualTo(type);
    }

    @Test
    public void test_create_and_parse_object_type_containing_parameter_name_with_spaces_and_quotes() {
        var type = ObjectType.builder()
            .setInnerType("first \" field", DataTypes.STRING)
            .build();
        var signature = type.getTypeSignature();
        assertThat(signature.toString()).isEqualTo("object(text,\"first \\\" field\" text)");
        var parsedSignature = parse(signature.toString());
        assertThat(parsedSignature).isEqualTo(signature);
        assertThat(parsedSignature.createType()).isEqualTo(type);
    }
}
