///*
// * Licensed to Crate.io GmbH ("Crate") under one or more contributor
// * license agreements.  See the NOTICE file distributed with this work for
// * additional information regarding copyright ownership.  Crate licenses
// * this file to you under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.  You may
// * obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * However, if you have executed another commercial license agreement
// * with Crate these terms will supersede the license and you may use the
// * software solely pursuant to the terms of the relevant commercial agreement.
// */
//
//package io.crate.types;
//
//import static io.crate.common.collections.Lists2.getOnlyElement;
//import static io.crate.types.TypeSignature.parseTypeSignature;;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.contains;
//import static org.hamcrest.Matchers.is;
//
//import java.util.List;
//
//import org.junit.Test;
//
//import io.crate.sql.parser.SqlParser;
//import io.crate.sql.tree.RecordTypeSignature;
//import io.crate.sql.tree.TypeParameter;
//import io.crate.sql.tree.TypeSignatureType;
//
//public class TypeSignatureParsingTest {
//
//
//
//    public static TypeSignature parseTypeSignature(String signature) {
//        return TypeSignature.parseTypeSignature(signature);
//    }
//
//    @Test
//    public void testParsingOfPrimitiveDataTypes() {
//        for (var type : DataTypes.PRIMITIVE_TYPES) {
//            TypeSignature antlr = parseAntlrTypeSignature(type.getName());
//            TypeSignature classic = parseTypeSignature(type.getName());
//            assertThat(antlr, is(classic));
//        }
//    }
//
//    @Test
//    public void testParsingOfArray() {
//        var input = "array(integer)";
//        TypeSignature antlr = parseAntlrTypeSignature(input);
//        TypeSignature classic = parseTypeSignature(input);
//        assertThat(antlr, is(classic));
//    }
//
//    @Test
//    public void testParsingOfObject() {
//        var signature = parseAntlrTypeSignature("object(text, integer)");
//        assertThat(signature.getBaseTypeName(), is(ObjectType.NAME));
//        assertThat(
//            signature.getParameters(),
//            contains(
//                new TypeSignature(DataTypes.STRING.getName()),
//                new TypeSignature(DataTypes.INTEGER.getName())));
//    }
//
//    @Test
//    public void testParsingOfNestedArray() {
//        var signature = parseAntlrTypeSignature("array(object(text, array(integer)))");
//        assertThat(signature.getBaseTypeName(), is(ArrayType.NAME));
//
//        var innerObjectTypeSignature = signature.getParameters().get(0);
//        assertThat(innerObjectTypeSignature.getBaseTypeName(), is(ObjectType.NAME));
//        assertThat(
//            innerObjectTypeSignature.getParameters(),
//            contains(
//                new TypeSignature(DataTypes.STRING.getName()),
//                new TypeSignature(ArrayType.NAME, List.of(new TypeSignature(DataTypes.INTEGER.getName())))));
//    }
//
//    @Test
//    public void test_parse_record_with_named_data_type() {
//        var signature = parseAntlrTypeSignature("record(field1 text)");
//
//        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
//        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
//        assertThat(innerSignature.parameterName(), is("field1"));
//        assertThat(innerSignature.getBaseTypeName(), is(DataTypes.STRING.getName()));
//    }
//
//    @Test
//    public void test_parse_record_with_named_data_types_that_contain_whitespaces() {
//        var signature = parseAntlrTypeSignature("record(field1 double precision)");
//
//        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
//        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
//        assertThat(innerSignature.parameterName(), is("field1"));
//        assertThat(innerSignature.getBaseTypeName(), is(DataTypes.DOUBLE.getName()));
//    }
//
//    @Test
//    public void test_parse_array_with_nested_record_type() {
//        var signature = parseAntlrTypeSignature("array(record(double precision))");
//
//        assertThat(signature.getBaseTypeName(), is(ArrayType.NAME));
//        assertThat(
//            signature.getParameters(),
//            contains(
//                new TypeSignature(
//                    RowType.NAME,
//                    List.of(new TypeSignature(DataTypes.DOUBLE.getName()))
//                )
//            )
//        );
//    }
//
//    @Test
//    public void test_parse_record_with_nested_named_record_type() {
//        var signature = parseAntlrTypeSignature("record(field1 record(timestamp without time zone))");
//
//        assertThat(signature.getBaseTypeName(), is(RowType.NAME));
//        var innerSignature = (ParameterTypeSignature) getOnlyElement(signature.getParameters());
//        assertThat(innerSignature.parameterName(), is("field1"));
//        assertThat(innerSignature.getBaseTypeName(), is(RowType.NAME));
//        assertThat(
//            innerSignature.getParameters(),
//            contains(new TypeSignature(DataTypes.TIMESTAMP.getName())));
//    }
//
//
//    @Test
//    public void test_parsing_nested_records() {
//        var signature = parseAntlrTypeSignature("record(field1 record(timestamp without time zone))");
//    }
//
//    @Test
//    public void test_parsing_records() {
//        var signature = parseAntlrTypeSignature("record(field1 double precision)");
//    }
//
//    @Test
//    public void testParsingOfObject1() {
//        var signature = parseAntlrTypeSignature("object(\"(\" record(timestamp without time zone))");
//    }
//}
