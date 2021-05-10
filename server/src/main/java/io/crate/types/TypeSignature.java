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

package io.crate.types;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.Character.isDigit;
import static java.lang.String.format;

public class TypeSignature implements Writeable {

    /**
     * Creates a type signature out of the given signature string.
     * A signature type string may contain parameters inside parenthesis:
     * <p>
     *   base_type_name(parameter [, parameter])
     * </p>
     *
     * Custom parameterized type handling must also be supported by {@link #createType()}.
     *
     * Some examples:
     * <p>
     *      integer
     *      array(integer)
     *      array(E)
     *      object(text, integer)
     *      object(text, V)
     * <p>
     */
    public static TypeSignature parseTypeSignature(String signature) {
        if (isDigit(signature.charAt(0)) && !signature.contains(" ")) {
            return TypeSignature.of(Integer.parseInt(signature));
        } else if (!signature.contains("(")) {
            return TypeSignature.of(signature, List.of());
        }

        String baseName = null;
        List<TypeSignature> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '(') {
                if (bracketCount == 0) {
                    assert baseName == null : "Expected baseName to be null";
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            } else if (c == ')') {
                bracketCount--;
                if (bracketCount == 0) {
                    assert parameterStart >= 0 : "Expected parameter start to be >= 0";
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return TypeSignature.of(baseName, parameters);
                    }
                }
            } else if (c == ',') {
                if (bracketCount == 1) {
                    assert parameterStart >= 0 : "Expected parameter start to be >= 0";
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format(Locale.ENGLISH, "Bad type signature: '%s'", signature));
    }

    protected static TypeSignature of(int parseInt) {
        return new IntegerLiteralTypeSignature(parseInt);
    }

    private static TypeSignature of(String signature, List<TypeSignature> parameters) {
        if (isNamedTypeSignature(signature)) {
            int split = signature.indexOf(" ");
            return new ParameterTypeSignature(
                signature.substring(0, split),
                new TypeSignature(signature.substring(split + 1), parameters));
        } else {
            return new TypeSignature(signature, parameters);
        }
    }


    private static boolean isNamedTypeSignature(String signature) {
        return !DataTypes.PRIMITIVE_TYPE_NAMES_WITH_SPACES.contains(signature)
               && signature.contains(" ");
    }

    private static TypeSignature parseTypeSignatureParameter(String signature, int begin, int end) {
        String parameterName = signature.substring(begin, end).trim();
        return parseTypeSignature(parameterName);
    }

    public static void toStream(TypeSignature typeSignature, StreamOutput out) throws IOException {
        out.writeVInt(typeSignature.type().ordinal());
        typeSignature.writeTo(out);
    }

    public static TypeSignature fromStream(StreamInput in) throws IOException {
        return TypeSignatureType.VALUES.get(in.readVInt()).newInstance(in);
    }


    private final String baseTypeName;
    private final List<TypeSignature> parameters;

    public TypeSignature(String baseTypeName) {
        this(baseTypeName, Collections.emptyList());
    }

    public TypeSignature(String baseTypeName, List<TypeSignature> parameters) {
        this.baseTypeName = baseTypeName;
        this.parameters = parameters;
    }

    public TypeSignature(StreamInput in) throws IOException {
        baseTypeName = in.readString();
        int numParams = in.readVInt();
        parameters = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            parameters.add(fromStream(in));
        }
    }

    public String getBaseTypeName() {
        return baseTypeName;
    }

    public List<TypeSignature> getParameters() {
        return parameters;
    }

    /**
     * Create the concrete {@link DataType} for this type signature.
     * Only `array` and `object` parameterized type signatures are supported.
     */
    public DataType<?> createType() {
        if (baseTypeName.equalsIgnoreCase(ArrayType.NAME)) {
            if (parameters.size() == 0) {
                return new ArrayType<>(UndefinedType.INSTANCE);
            }
            DataType<?> innerType = parameters.get(0).createType();
            return new ArrayType<>(innerType);
        } else if (baseTypeName.equalsIgnoreCase(ObjectType.NAME)) {
            var builder = ObjectType.builder();
            // Only build typed objects if we receive parameter key-value pairs which may not exist on generic
            // object signatures with type information only, no key strings
            if (parameters.size() > 1) {
                for (int i = 0; i < parameters.size() - 1; ) {
                    var valTypeSignature = parameters.get(i + 1);
                    if (valTypeSignature instanceof ParameterTypeSignature) {
                        var innerTypeName = ((ParameterTypeSignature) valTypeSignature).parameterName();
                        builder.setInnerType(innerTypeName, valTypeSignature.createType());
                    }
                    i += 2;
                }
            }
            return builder.build();
        } else if (baseTypeName.equalsIgnoreCase(RowType.NAME)) {
            ArrayList<String> fields = new ArrayList<>(parameters.size());
            ArrayList<DataType<?>> dataTypes = new ArrayList<>(parameters.size());
            for (int i = 0; i < parameters.size(); i++) {
                var parameterTypeSignature = parameters.get(i);
                if (parameterTypeSignature instanceof ParameterTypeSignature) {
                    fields.add(((ParameterTypeSignature) parameterTypeSignature).parameterName());
                    dataTypes.add(parameterTypeSignature.createType());
                } else {
                    // No named parameter found, row type is based on a signature without detailed field information
                    return RowType.EMPTY;
                }
            }
            return new RowType(dataTypes, fields);
        } else {
            var integerLiteralParameters = new ArrayList<Integer>(parameters.size());
            for (var parameter : parameters) {
                if (!parameter.type().equals(TypeSignatureType.INTEGER_LITERAL_SIGNATURE)) {
                    throw new IllegalArgumentException(
                        "The signature type of the based data type parameters can only be: "
                        + TypeSignatureType.INTEGER_LITERAL_SIGNATURE.toString());
                }
                integerLiteralParameters.add(((IntegerLiteralTypeSignature) parameter).value());
            }
            return DataTypes.of(baseTypeName, integerLiteralParameters);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(baseTypeName);
        out.writeVInt(parameters.size());
        for (TypeSignature parameter : parameters) {
            toStream(parameter, out);
        }
    }

    @Override
    public String toString() {
        if (parameters.isEmpty()) {
            return baseTypeName;
        }

        StringBuilder typeName = new StringBuilder(baseTypeName);
        typeName.append("(").append(parameters.get(0));
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(parameters.get(i));
        }
        typeName.append(")");
        return typeName.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null ||
            !(getClass() == o.getClass() || getClass() == ParameterTypeSignature.class)) {
            return false;
        }
        TypeSignature that = (TypeSignature) o;
        return equalsIgnoringObjectParameterSizeDifference(that);
    }

    private boolean equalsIgnoringObjectParameterSizeDifference(TypeSignature that) {
        if (baseTypeName.equals(ObjectType.NAME) && that.baseTypeName.equals(ObjectType.NAME)) {
            if (parameters.size() != that.parameters.size()) {
                return true;
            }
        }
        return baseTypeName.equals(that.baseTypeName) &&
               parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseTypeName, parameters);
    }

    public TypeSignatureType type() {
        return TypeSignatureType.TYPE_SIGNATURE;
    }
}
