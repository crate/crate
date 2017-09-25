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

package io.crate.metadata.functions.params;

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.FuncArg;
import io.crate.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Type handling for function parameters (FuncParams).
 *
 * A FuncParam is composed of zero, one, or multiple parameters ({@link Param}s).
 *
 * Parameters are linked with each other by passing the same {@link Param} more than once.
 *
 * For example:
 * {@code
 *
 *  Param a = Param.of(ANY);
 *  Param b = Param.of(INTEGER);
 *
 *  FuncParams.of(a, a, b).build();
 *  // => A function parameters definition with three parameters.
 *  // The first and the second parameter (a) can be of any type but must have the same type.
 *  // The third parameter (b) has to be of type integer.
 * }
 *
 * Matching
 *
 * Parameters are matched against an argument list by using the match method:
 * {@code
 *
 *  List<Symbol> args = Arrays.asList(Literal.of(1L), Literal.of(1), Literal.of(1))
 *
 *  List<DataType> signature = FuncParams.match(args);
 *  // => The resulting signature does not have to be of the same type as the original symbols
 *  // but the implementation assures that a valid conversion is derived from the given types.
 *  // This is done by checking convertibility and type precedence.
 * }
 *
 * Variable Arguments (VarArgs)
 *
 * Additional variable arguments can be specified on FuncParams.Builder:
 * {@code
 *
 *      FuncParams.builder().withVarArgs(Param.of(INTEGER)).build()
 *      // => Any number of integers as var args.
 *      FuncParams.of().withVarArgs(Param.of(STRING), Param.of(INTEGER)).build()
 *      // => Any number of String followed by an Integer parameter,
 *      // e.g. "a", 1, "b", 2, ...
 *
 *      FuncParams.builder().withVarArgs(2, Param.of(INTEGER)).build()
 *      // => Maximum of two variable integer arguments.
 *
 *      FuncParams.builder().withIndependentVarArgs(Param.of(ANY)).build()
 *      // => Unlimited number of arguments which don't have to be of the same type
 * }
 *
 *
 * The Param class is thread-safe and can be accessed by multiple threads at once.
 */
public class FuncParams {

    public static final FuncParams NONE = FuncParams.builder().build();
    public static final FuncParams SINGLE_ANY = FuncParams.builder(Param.ANY).build();
    public static final FuncParams SINGLE_NUMERIC = FuncParams.builder(Param.NUMERIC).build();
    public static final FuncParams ANY_VAR_ARGS_SAME_TYPE = FuncParams.builder().withVarArgs(Param.ANY).build();

    private static final int UNLIMITED_VAR_ARGS = -1;
    private static final int MAX_ARGS_TO_PRINT = 5;

    private final Param[] parameters;
    private final Param[] varArgParameters;
    private final int maxVarArgOccurrences;
    private final boolean varArgsIndependent;

    private FuncParams(Param[] parameters,
                       Param[] varArgParameters,
                       int maxVarArgOccurrences,
                       boolean varArgsIndependent) {
        this.parameters = parameters;
        this.varArgParameters = varArgParameters;
        this.maxVarArgOccurrences = maxVarArgOccurrences;
        this.varArgsIndependent = varArgsIndependent;
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class Builder {

        private final Param[] parameters;
        private Param[] varArgParameters;
        private int maxVarArgOccurrences;
        private boolean varArgsIndependent;

        public Builder(Param... parameters) {
            this.parameters = parameters;
            this.varArgParameters = new Param[]{};
            this.maxVarArgOccurrences = UNLIMITED_VAR_ARGS;
        }

        /**
         * Adds an optional and variable number of occurrences of the
         * following parameters.
         * @param parameters The types used in the var arg parameters.
         * @return FuncParams
         */
        public Builder withVarArgs(Param... parameters) {
            this.varArgParameters = parameters;
            return this;
        }

        /**
         * Adds an optional and a fixed upper number of occurrences of the
         * following parameters.
         * @param maxVarArgOccurrences The maximum number of occurrences of
         *                             the varArgs provided.
         * @return FuncParams
         */
        public Builder limitVarArgOccurrences(int maxVarArgOccurrences) {
            Preconditions.checkArgument(maxVarArgOccurrences > 0,
                "The minimum limit for the number of occurrences is 1");
            this.maxVarArgOccurrences = maxVarArgOccurrences;
            return this;
        }

        /**
         * One or multiple var args which operate completely independently of each other.
         * @param parameters The parameters which are independent of each other.
         * @return A Builder which can be used
         */
        public Builder withIndependentVarArgs(Param... parameters) {
            withVarArgs(parameters);
            this.varArgsIndependent = true;
            return this;
        }

        /**
         * Merges the current {@link DataType}s for the Params with additional types.
         * @param dataTypes The types to add to merge with the existing types.
         * @return A new FuncParams with additional types to the {@link Param}s.
         */
        public Builder mergeWithTypes(List<DataType> dataTypes) {
            Preconditions.checkArgument(dataTypes.size() == this.parameters.length,
                "The given DataTypes for the merge don't match the existing parameter length.");
            List<DataType> tmp = new ArrayList<>();
            for (int i = 0; i < parameters.length; i++) {
                tmp.addAll(parameters[i].getValidTypes());
                tmp.add(dataTypes.get(i));
                parameters[i] = Param.of(tmp);
                tmp.clear();
            }
            return this;
        }

        public FuncParams build() {
            return new FuncParams(parameters, varArgParameters, maxVarArgOccurrences, varArgsIndependent);
        }
    }

    /**
     * Create a new builder for a FuncParam with a {@link Param} signature.
     * @param parameters The {@link Param}s representing the parameters.
     * @return A builder which may be refined and built.
     */
    public static Builder builder(Param... parameters) {
        return new Builder(parameters);
    }

    /**
     * Create a new builder for a FuncParam with a list of {@link DataType}s
     * which represent the function signature.
     * @param parameterTypes The {@link Param}s representing the parameters.
     * @return A builder which may be refined and built.
     */
    public static Builder builder(List<DataType> parameterTypes) {
        Param[] params = new Param[parameterTypes.size()];
        for (int i = 0; i < params.length; i++) {
            params[i] = Param.of(parameterTypes.get(i));
        }
        return builder(params);
    }

    private void resetBoundTypes() {
        for (Param parameter : parameters) {
            parameter.unbind();
        }
        for (Param parameter : varArgParameters) {
            parameter.unbind();
        }
    }

    private void bindParams(List<? extends FuncArg> params) {
        int idx;
        idx = bindFixedParams(params);
        idx = bindVarParams(params, idx);
        checkForRemainingParams(params, idx);
    }

    private int bindFixedParams(List<? extends FuncArg> params) {
        for (int i = 0; i < parameters.length; i++) {
            FuncArg funcArg = params.get(i);
            parameters[i].bind(funcArg, false);
        }
        return parameters.length;
    }

    private int bindVarParams(List<? extends FuncArg> params, int arrayPos) {
        int numberOfParameters = params.size();
        while (varArgParameters.length > 0 && arrayPos + varArgParameters.length <= numberOfParameters) {
            if (maxVarArgOccurrences != UNLIMITED_VAR_ARGS && arrayPos - parameters.length >= maxVarArgOccurrences) {
                throw new IllegalArgumentException(
                    "Too many variable arguments provided. Only the follow number is allowed: " + maxVarArgOccurrences);
            }
            for (int k = 0; k < varArgParameters.length; k++, arrayPos++) {
                FuncArg funcArg = params.get(arrayPos);
                varArgParameters[k].bind(funcArg, varArgsIndependent);
            }
        }
        return arrayPos;
    }

    private void checkForRemainingParams(List<? extends FuncArg> params, int pos) {
        int numberOfParameters = params.size();
        if (pos != numberOfParameters) {
            final int numArgsToPrint;
            if (varArgParameters.length == 0) {
                numArgsToPrint = 1;
            } else if (maxVarArgOccurrences == UNLIMITED_VAR_ARGS) {
                numArgsToPrint = MAX_ARGS_TO_PRINT;
            } else {
                numArgsToPrint = Math.min(MAX_ARGS_TO_PRINT, maxVarArgOccurrences);
            }
            int[] possibleArgs = new int[numArgsToPrint];
            for (int i = 0; i < possibleArgs.length; i++) {
                possibleArgs[i] = parameters.length + i * varArgParameters.length;
            }
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "The number of arguments is incorrect. Provided: %s arguments. " +
                    "The following arguments are valid: %s",
                    numberOfParameters, Arrays.toString(possibleArgs)));
        }
    }

    private List<DataType> retrieveBoundParams(List<? extends FuncArg> params) {
        int numberOfParameters = params.size();
        List<DataType> newParams = new ArrayList<>(numberOfParameters);
        for (int i = 0; i < parameters.length; i++) {
            DataType boundType = parameters[i].getBoundType(false);
            newParams.add(boundType);
        }
        for (int i = parameters.length; i < numberOfParameters;) {
            for (int k = 0; k < varArgParameters.length; k++, i++) {
                DataType boundType = varArgParameters[k].getBoundType(varArgsIndependent);
                newParams.add(boundType);
            }
        }
        return newParams;
    }

    /**
     * Matches the provided argument types to the {@link Param} definitions of this class.
     * @param args A list of {@link FuncArg}s to match against.
     * @return A new list of {@link DataType}s which are derived from the given arguments.
     */
    public List<DataType> match(List<? extends FuncArg> args) {
        Objects.requireNonNull(args, "Supplied parameter types may not be null.");
        if (args.size() < parameters.length) {
            return null;
        }
        resetBoundTypes();
        bindParams(args);
        return retrieveBoundParams(args);
    }

}
