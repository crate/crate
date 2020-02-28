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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.Character.isDigit;
import static java.lang.String.format;

public class TypeSignature {

    public static TypeSignature parseTypeSignature(String signature) {
        return parseTypeSignature(signature, new HashSet<>());
    }

    public static TypeSignature parseTypeSignature(String signature, Set<String> literalCalculationParameters) {
        if (!signature.contains("<") && !signature.contains("(")) {
            return new TypeSignature(signature);
        }

        String baseName = null;
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            // TODO: remove angle brackets support once ROW<TYPE>(name) will be dropped
            // Angle brackets here are checked not for the support of ARRAY<> and MAP<>
            // but to correctly parse ARRAY(row<BIGINT, BIGINT>('a','b'))
            if (c == '(' || c == '<') {
                if (bracketCount == 0) {
                    assert baseName == null : "Expected baseName to be null";
                    assert parameterStart == -1 : "Expected parameter start to be -1";
                    baseName = signature.substring(0, i);
                    //checkArgument(!literalCalculationParameters.contains(baseName), "Bad type signature: '%s'", signature);
                    parameterStart = i + 1;
                }
                bracketCount++;
            } else if (c == ')' || c == '>') {
                bracketCount--;
                assert bracketCount >= 0 : "Bad type signature: '" + signature + "'";
                if (bracketCount == 0) {
                    assert parameterStart >= 0 : "Bad type signature: '" + signature + "'";
                    parameters.add(parseTypeSignatureParameter(signature,
                                                               parameterStart,
                                                               i,
                                                               literalCalculationParameters));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters);
                    }
                }
            } else if (c == ',') {
                if (bracketCount == 1) {
                    assert parameterStart >= 0 : "Bad type signature: '" + signature + "'";
                    parameters.add(parseTypeSignatureParameter(signature,
                                                               parameterStart,
                                                               i,
                                                               literalCalculationParameters));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    private static TypeSignatureParameter parseTypeSignatureParameter(
        String signature,
        int begin,
        int end,
        Set<String> literalCalculationParameters) {
        String parameterName = signature.substring(begin, end).trim();
        if (isDigit(signature.charAt(begin))) {
            return TypeSignatureParameter.of(Long.parseLong(parameterName));
        } else if (literalCalculationParameters.contains(parameterName)) {
            return TypeSignatureParameter.of(parameterName);
        } else {
            return TypeSignatureParameter.of(parseTypeSignature(parameterName, literalCalculationParameters));
        }
    }

    public enum Mode {
        IN,
        VARIADIC
    }

    private final String base;
    private final List<TypeSignatureParameter> parameters;
    private final boolean calculated;
    private final Predicate<DataType<?>> predicate;
    private final Mode mode;
    private final Map<String, TypeSignature> captures;

    public TypeSignature(String base) {
        this(base, t -> true, Mode.IN);
    }

    public TypeSignature(String base, List<TypeSignatureParameter> parameters) {
        this(base, t -> true, Mode.IN, Collections.emptyMap(), parameters);
    }

    public TypeSignature(String base, Predicate<DataType<?>> predicate) {
        this(base, predicate, Mode.IN);
    }

    public TypeSignature(String base, Predicate<DataType<?>> predicate, Mode mode) {
        this(base, predicate, mode, Collections.emptyMap(), Collections.emptyList());
    }

    public TypeSignature(String base,
                         Predicate<DataType<?>> predicate,
                         Mode mode,
                         Map<String, TypeSignature> captures,
                         List<TypeSignatureParameter> parameters) {
        this.base = base;
        this.predicate = predicate;
        this.mode = mode;
        this.captures = captures;
        this.parameters = parameters;
        this.calculated = false;
    }

    public Mode mode() {
        return mode;
    }

    public String getBase() {
        return base;
    }

    public List<TypeSignatureParameter> getParameters() {
        return parameters;
    }

    public List<TypeSignature> getTypeParametersAsTypeSignatures() {
        List<TypeSignature> result = new ArrayList<>();
        for (TypeSignatureParameter parameter : parameters) {
            if (parameter.getKind() != ParameterKind.TYPE) {
                throw new IllegalStateException(
                    format("Expected all parameters to be TypeSignatures but [%s] was found", parameter.toString()));
            }
            result.add(parameter.getTypeSignature());
        }
        return result;
    }

    public boolean isCalculated() {
        return calculated;
    }

    public boolean match(DataType<?> type) {
        return predicate.test(type);
    }

    public DataType<?> apply(DataType<?> type) {
        return type;
    }

    public TypeSignature withCapture(String name, Function<DataType<?>, DataType<?>> capture) {
        HashMap<String, TypeSignature> captures = new HashMap<>(this.captures);
        captures.put(name, new WithCapture(name, capture));
        return new TypeSignature(name, predicate, Mode.IN, captures, parameters);
    }

    @Nullable
    public TypeSignature capture(String name) {
        return captures.get(name);
    }

    @Override
    public String toString() {
        if (parameters.isEmpty()) {
            return base;
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("(").append(parameters.get(0));
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(parameters.get(i));
        }
        typeName.append(")");
        return typeName.toString();
    }

    private static class WithCapture extends TypeSignature {

        private final Function<DataType<?>, DataType<?>> capture;

        public WithCapture(String name, Function<DataType<?>, DataType<?>> capture) {
            super(name, t -> true);
            this.capture = capture;
        }

        @Override
        public DataType<?> apply(DataType<?> type) {
            return capture.apply(type);
        }
    }
}
