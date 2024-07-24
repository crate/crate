/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.functions;

import static io.crate.metadata.FunctionType.SCALAR;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import io.crate.execution.engine.aggregation.impl.NumericSumAggregation;
import io.crate.types.BitStringType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.RowType;
import io.crate.types.StringType;
import io.crate.types.TypeSignature;

public class SignatureBinderTest extends ESTestCase {

    private static Signature.Builder functionSignature() {
        return Signature.builder("function", SCALAR);
    }

    @Test
    public void testBasic() {
        Signature function = functionSignature()
            .typeVariableConstraints(List.of(typeVariable("T")))
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("T"))
            .build();

        assertThatSignature(function)
            .boundTo("bigint")
            .produces(new BoundVariables(Map.of("T", type("bigint"))));

        assertThatSignature(function)
            .boundTo("text")
            .produces(new BoundVariables(Map.of("T", type("text"))));

        assertThatSignature(function)
            .boundTo("text", "bigint")
            .fails();

        assertThatSignature(function)
            .boundTo("array(bigint)")
            .produces(new BoundVariables(Map.of("T", type("array(bigint)"))));
    }

    @Test
    public void testBindUnknownToConcreteArray() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("array(boolean)"))
            .build();

        assertThatSignature(function)
            .boundTo("undefined")
            .withCoercion()
            .succeeds();
    }

    @Test
    public void testBindTypeVariablesBasedOnTheSecondArgument() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("array(T)"), TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(function)
            .boundTo("undefined", "bigint")
            .withCoercion()
            .produces(new BoundVariables(Map.of("T", type("bigint"))));
    }

    @Test
    public void testBindParametricTypeParameterToUnknown() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("array(T)"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(function)
            .boundTo("undefined")
            .fails();

        assertThatSignature(function)
            .withCoercion()
            .boundTo("undefined")
            .succeeds();
    }

    @Test
    public void testBindUnknownToTypeParameter() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(function)
            .boundTo("undefined")
            .withCoercion()
            .produces(new BoundVariables(Map.of("T", type("undefined"))));
    }

    @Test
    public void testBindDoubleToBigint() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("double precision"), TypeSignature.parse("double precision"))
            .build();

        assertThatSignature(function)
            .boundTo("double precision", "bigint")
            .withCoercion()
            .succeeds();
    }

    @Test
    public void testMismatchedArgumentCount() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("bigint"), TypeSignature.parse("bigint"))
            .build();

        assertThatSignature(function)
            .boundTo("bigint", "bigint", "bigint")
            .fails();

        assertThatSignature(function)
            .boundTo("bigint")
            .fails();
    }

    @Test
    public void testArray() {
        Signature getFunction = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("array(T)"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(getFunction)
            .boundTo("array(bigint)")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))));

        assertThatSignature(getFunction)
            .boundTo("bigint")
            .withCoercion()
            .fails();

        Signature containsFunction = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("array(T)"), TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(containsFunction)
            .boundTo("array(bigint)", "bigint")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))));

        assertThatSignature(containsFunction)
            .boundTo("array(bigint)", "geo_point")
            .withCoercion()
            .fails();

        Signature castFunction = functionSignature()
            .returnType(TypeSignature.parse("array(T2)"))
            .argumentTypes(TypeSignature.parse("array(T1)"), TypeSignature.parse("array(T2)"))
            .typeVariableConstraints(List.of(typeVariable("T1"), typeVariable("T2")))
            .build();

        assertThatSignature(castFunction)
            .boundTo("array(undefined)", "array(bigint)")
            .withCoercion()
            .produces(new BoundVariables(
                Map.of(
                    "T1", type("undefined"),
                    "T2", type("bigint"))
            ));

        Signature fooFunction = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("array(T)"), TypeSignature.parse("array(T)"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(fooFunction)
            .boundTo("array(bigint)", "array(bigint)")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))
            ));

        assertThatSignature(fooFunction)
            .boundTo("array(bigint)", "array(geo_point)")
            .withCoercion()
            .fails();
    }

    @Test
    public void testMap() {
        Signature getValueFunction = functionSignature()
            .returnType(TypeSignature.parse("V"))
            .argumentTypes(TypeSignature.parse("object(K,V)"), TypeSignature.parse("K"))
            .typeVariableConstraints(List.of(typeVariable("K"), typeVariable("V")))
            .build();

        assertThatSignature(getValueFunction)
            .boundTo(
                ObjectType.builder()
                    .setInnerType("V", DataTypes.LONG).build(),
                DataTypes.STRING)
            .produces(new BoundVariables(
                Map.of(
                    "K", type("text"),
                    "V", type("bigint"))
            ));

        assertThatSignature(getValueFunction)
            .boundTo(
                ObjectType.builder()
                    .setInnerType("V", DataTypes.LONG).build(),
                DataTypes.LONG)
            .withoutCoercion()
            .fails();
    }

    @Test
    public void test_bind_record_type_signature_as_argument_type() {
        var signature = functionSignature()
            .returnType(TypeSignature.parse("T"))
            .argumentTypes(TypeSignature.parse("record(T)"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(signature)
            .boundTo(new RowType(List.of(DataTypes.LONG)))
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))));

        assertThatSignature(signature)
            .boundTo("bigint")
            .withCoercion()
            .fails();
    }

    @Test
    public void test_bind_type_text_types_with_limit_length_binds_type_with_highest_length() {
        var signature = functionSignature()
            .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("E"))
            .returnType(DataTypes.BOOLEAN.getTypeSignature())
            .typeVariableConstraints(List.of(typeVariable("E")))
            .build();

        assertThatSignature(signature)
            .boundTo(StringType.of(1), StringType.of(2))
            .produces(new BoundVariables(
                Map.of("E", type(StringType.of(2).getTypeSignature().toString()))));
    }

    @Test
    public void test_bind_type_character_types_with_limit_length_binds_type_with_highest_length() {
        var signature = functionSignature()
            .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("E"))
            .returnType(DataTypes.BOOLEAN.getTypeSignature())
            .typeVariableConstraints(List.of(typeVariable("E")))
            .build();

        assertThatSignature(signature)
            .boundTo(CharacterType.of(1), CharacterType.of(2))
            .produces(new BoundVariables(
                Map.of("E", type(CharacterType.of(2).getTypeSignature().toString()))));
    }

    @Test
    public void test_bind_type_bit_types_binds_type_with_highest_length() {
        var signature = functionSignature()
            .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("E"))
            .returnType(DataTypes.BOOLEAN.getTypeSignature())
            .typeVariableConstraints(List.of(typeVariable("E")))
            .build();

        assertThatSignature(signature)
            .boundTo(new BitStringType(1), new BitStringType(2))
            .produces(new BoundVariables(
                Map.of("E", type(new BitStringType(2).getTypeSignature().toString()))));
    }

    @Test
    public void test_bind_record_type_signature_as_return_type() {
        var signature = functionSignature()
            .returnType(TypeSignature.parse("record(col T)"))
            .argumentTypes(TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(signature)
            .boundTo("bigint")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))));
    }

    @Test
    public void testVariableArityGroup() {
        Signature mapFunction = functionSignature()
            .returnType(TypeSignature.parse("object(text, V)"))
            .argumentTypes(TypeSignature.parse("text"), TypeSignature.parse("V"))
            .typeVariableConstraints(List.of(typeVariable("V")))
            .variableArityGroup(List.of(TypeSignature.parse("text"), TypeSignature.parse("V")))
            .build();

        assertThatSignature(mapFunction)
            .boundTo("text", "integer")
            .produces(new BoundVariables(
                Map.of(
                    "V", type("integer"))
            ));

        assertThatSignature(mapFunction)
            .boundTo("text", "integer", "text", "integer")
            .produces(new BoundVariables(
                Map.of(
                    "V", type("integer"))
            ));

        assertThatSignature(mapFunction)
            .boundTo("text")
            .fails();

        assertThatSignature(mapFunction)
            .boundTo("text", "integer", "text")
            .fails();
    }

    @Test
    public void testVariableArityOfAnyTypeConstraint() {
        Signature fooFunction = functionSignature()
            .returnType(TypeSignature.parse("text"))
            .argumentTypes(TypeSignature.parse("text"), TypeSignature.parse("V"))
            .typeVariableConstraints(List.of(typeVariableOfAnyType("V")))
            .setVariableArity(true)
            .build();

        assertThatSignature(fooFunction)
            .boundTo("text", "integer")
            .produces(new BoundVariables(
                Map.of(
                    "V", type("integer")
                )
            ));

        assertThatSignature(fooFunction)
            .boundTo("text", "integer", "text", "geo_point")
            .produces(new BoundVariables(
                Map.of(
                    "V", type("integer"),
                    "_generated_V1", type("text"),
                    "_generated_V2", type("geo_point")
                )
            ));
    }

    @Test
    public void test_variable_arity_with_array_nested_variable_constraint_of_any_type() {
        Signature signature = functionSignature()
            .returnType(TypeSignature.parse("integer"))
            .argumentTypes(TypeSignature.parse("array(E)"))
            .typeVariableConstraints(List.of(typeVariableOfAnyType("E")))
            .setVariableArity(true)
            .build();

        // arity 1
        assertThatSignature(signature)
            .boundTo("array(text)")
            .produces(new BoundVariables(
                Map.of(
                    "E", type("text")
                )
            ));
        // arity 2
        assertThatSignature(signature)
            .boundTo("array(text)", "array(integer)")
            .produces(new BoundVariables(
                Map.of(
                    "E", type("text"),
                    "_generated_E1", type("integer")
                )
            ));
    }

    @Test
    public void test_variable_arity_with_multi_array_nested_variable_constraint_of_any_type() {
        Signature signature = functionSignature()
            .returnType(TypeSignature.parse("integer"))
            .argumentTypes(TypeSignature.parse("array(array(E))"))
            .typeVariableConstraints(List.of(typeVariableOfAnyType("E")))
            .setVariableArity(true)
            .build();

        // arity 1
        assertThatSignature(signature)
            .boundTo("array(array(text))")
            .produces(new BoundVariables(
                Map.of(
                    "E", type("text")
                )
            ));
        // arity 2
        assertThatSignature(signature)
            .boundTo("array(array(text))", "array(array(long))")
            .produces(new BoundVariables(
                Map.of(
                    "E", type("text"),
                    "_generated_E1", type("long")
                )
            ));
    }

    @Test
    public void testVarArgs() {
        Signature variableArityFunction = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .setVariableArity(true)
            .build();

        assertThatSignature(variableArityFunction)
            .boundTo("bigint")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))
            ));

        assertThatSignature(variableArityFunction)
            .boundTo("text")
            .produces(new BoundVariables(
                Map.of("T", type("text"))
            ));

        assertThatSignature(variableArityFunction)
            .boundTo("bigint", "bigint")
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))
            ));

        assertThatSignature(variableArityFunction)
            .boundTo(Collections.emptyList())
            .fails();

        assertThatSignature(variableArityFunction)
            .boundTo("bigint", "geo_point")
            .withCoercion()
            .fails();
    }

    @Test
    public void testCoercion() {
        Signature function = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("T"), TypeSignature.parse("double precision"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(function)
            .boundTo("double precision", "double precision")
            .withCoercion()
            .produces(new BoundVariables(
                Map.of("T", type("double"))
            ));

        assertThatSignature(function)
            .boundTo("bigint", "bigint")
            .withCoercion()
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))
            ));

        assertThatSignature(function)
            .boundTo("text", "bigint")
            .withCoercion()
            .produces(new BoundVariables(
                Map.of("T", type("text"))
            ));

        assertThatSignature(function)
            .boundTo("bigint", "geo_point")
            .withCoercion()
            .fails();
    }

    @Test
    public void testUnknownCoercion() {
        Signature foo = functionSignature()
            .returnType(TypeSignature.parse("boolean"))
            .argumentTypes(TypeSignature.parse("T"), TypeSignature.parse("T"))
            .typeVariableConstraints(List.of(typeVariable("T")))
            .build();

        assertThatSignature(foo)
            .boundTo("undefined", "undefined")
            .produces(new BoundVariables(
                Map.of("T", type("undefined"))
            ));

        assertThatSignature(foo)
            .boundTo("undefined", "bigint")
            .withCoercion()
            .produces(new BoundVariables(
                Map.of("T", type("bigint"))
            ));

        assertThatSignature(foo)
            .boundTo("geo_point", "bigint")
            .withCoercion()
            .fails();
    }

    @Test
    public void testNumericParameters() {
        NumericType dt = NumericType.of(10, 2);
        assertThatSignature(NumericSumAggregation.SIGNATURE)
            .boundTo(dt)
            .hasReturnType(dt);
    }

    @Test
    public void testNumericParametersWithDifferingScales() {
        Signature foo = functionSignature()
            .returnType(TypeSignature.parse("numeric"))
            .argumentTypes(TypeSignature.parse("numeric"), TypeSignature.parse("numeric"))
            .build();

        BoundSignature expected = new BoundSignature(
            List.of(NumericType.INSTANCE, NumericType.INSTANCE),
            NumericType.of(10, 2)
        );

        assertThatSignature(foo)
            .boundTo(NumericType.INSTANCE, NumericType.of(10, 2))
            .hasBoundSignature(expected);
    }

    private DataType<?> type(String signature) {
        TypeSignature typeSignature = TypeSignature.parse(signature);
        return typeSignature.createType();
    }

    private BindSignatureAssertion assertThatSignature(Signature function) {
        return new BindSignatureAssertion(function);
    }

    private static class BindSignatureAssertion {
        private final Signature function;
        private List<DataType<?>> argumentTypes;
        private boolean allowCoercion;

        private BindSignatureAssertion(Signature function) {
            this.function = function;
        }

        public BindSignatureAssertion withCoercion() {
            allowCoercion = true;
            return this;
        }

        public BindSignatureAssertion withoutCoercion() {
            allowCoercion = false;
            return this;
        }

        public BindSignatureAssertion boundTo(Object... arguments) {
            return boundTo(List.of(arguments));
        }

        public BindSignatureAssertion boundTo(List<Object> arguments) {
            ArrayList<DataType<?>> builder = new ArrayList<>(arguments.size());
            for (Object argument : arguments) {
                if (argument instanceof DataType<?> dataType) {
                    builder.add(dataType);
                } else if (argument instanceof String str) {
                    builder.add(TypeSignature.parse(str).createType());
                } else if (argument instanceof TypeSignature typeSig) {
                    builder.add(typeSig.createType());
                } else {
                    throw new IllegalArgumentException(format(
                        "argument is of type %s. It should be DataType, String or TypeSignature",
                        argument.getClass()));
                }
            }
            this.argumentTypes = Collections.unmodifiableList(builder);
            return this;
        }

        public void succeeds() {
            assertThat(bindVariables()).isNotNull();
        }

        public void fails() {
            assertThat(bindVariables()).isNull();
        }

        public void produces(BoundVariables expected) {
            BoundVariables actual = bindVariables();
            assertThat(actual).isEqualTo(expected);
        }

        public void hasReturnType(DataType<?> expected) {
            BoundSignature actual = bind();
            assertThat(actual).isNotNull();
            assertThat(actual.returnType()).isEqualTo(expected);
        }

        public void hasBoundSignature(BoundSignature expected) {
            BoundSignature actual = bind();
            assertThat(actual).isNotNull();
            assertThat(actual).isEqualTo(expected);
        }

        @Nullable
        private BoundVariables bindVariables() {
            var coercionType = allowCoercion ? SignatureBinder.CoercionType.FULL : SignatureBinder.CoercionType.NONE;
            assertThat(argumentTypes).isNotNull();
            SignatureBinder signatureBinder = new SignatureBinder(function, coercionType);
            return signatureBinder.bindVariables(argumentTypes);
        }

        private BoundSignature bind() {
            var coercionType = allowCoercion ? SignatureBinder.CoercionType.FULL : SignatureBinder.CoercionType.NONE;
            assertThat(argumentTypes).isNotNull();
            SignatureBinder signatureBinder = new SignatureBinder(function, coercionType);
            return signatureBinder.bind(argumentTypes);
        }
    }
}
