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

package io.crate.metadata;

import static io.crate.common.collections.Lists.getOnlyElement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.FunctionProvider.FunctionFactory;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.BoundVariables;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.SignatureBinder;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class Functions {

    private static final Logger LOGGER = LogManager.getLogger(Functions.class);

    private final Map<FunctionName, List<FunctionProvider>> udfFunctionImplementations = new ConcurrentHashMap<>();
    private final Map<FunctionName, List<FunctionProvider>> functionImplementations;

    public Functions copyOf() {
        var functions = new Functions(Map.copyOf(functionImplementations));
        functions.udfFunctionImplementations.putAll(udfFunctionImplementations);
        return functions;
    }

    @Inject
    public Functions(Map<FunctionName, List<FunctionProvider>> functionImplementationsBySignature) {
        this.functionImplementations = functionImplementationsBySignature;
    }

    public Iterable<Signature> signatures() {
        return () ->
            Stream.concat(
                functionImplementations.values().stream()
                    .flatMap(x -> x.stream())
                    .map(x -> x.getSignature()),
                udfFunctionImplementations.values().stream()
                    .flatMap(x -> x.stream())
                    .map(x -> x.getSignature())
            )
            .iterator();
    }

    public void registerUdfFunctionImplementationsForSchema(
        String schema, Map<FunctionName, List<FunctionProvider>> functions) {
        // remove deleted ones before re-registering all current ones for the given schema
        udfFunctionImplementations.entrySet()
            .removeIf(
                function ->
                    schema.equals(function.getKey().schema())
                    && functions.get(function.getKey()) == null);
        udfFunctionImplementations.putAll(functions);
    }

    public void deregisterUdfResolversForSchema(String schema) {
        udfFunctionImplementations.keySet()
            .removeIf(function -> schema.equals(function.schema()));
    }

    @Nullable
    private static Signature findSignatureByOid(Map<FunctionName, List<FunctionProvider>> functions, int oid) {
        for (Map.Entry<FunctionName, List<FunctionProvider>> func : functions.entrySet()) {
            for (FunctionProvider sig : func.getValue()) {
                if (Objects.equals(oid, OidHash.functionOid(sig.getSignature()))) {
                    return sig.getSignature();
                }
            }
        }
        return null;
    }

    @Nullable
    public Signature findFunctionSignatureByOid(int oid) {
        Signature sig = findSignatureByOid(udfFunctionImplementations, oid);
        return sig != null ? sig : findSignatureByOid(functionImplementations, oid);
    }

    /**
     * See {@link #get(String, String, List, List, SearchPath)}
     */
    public FunctionImplementation get(@Nullable String suppliedSchema,
                                      String functionName,
                                      List<Symbol> arguments,
                                      SearchPath searchPath) {
        return get(suppliedSchema, functionName, Symbols.typeView(arguments), arguments, searchPath);
    }

    /**
     * Return a function that matches the name/arguments.
     *
     * <pre>
     * {@code
     * Lookup logic:
     *     No schema:   Built-ins -> Function or UDFs in searchPath
     *     With Schema: Function or UDFs in schema
     * }
     * </pre>
     *
     * @throws UnsupportedFunctionException if the function wasn't found
     */
    private FunctionImplementation get(@Nullable String suppliedSchema,
                                       String functionName,
                                       List<DataType<?>> argumentTypes,
                                       List<Symbol> arguments,
                                       SearchPath searchPath) {
        FunctionName fqnName = new FunctionName(suppliedSchema, functionName);
        FunctionImplementation func = resolveFunctionBySignature(
            fqnName,
            argumentTypes,
            arguments,
            searchPath,
            functionImplementations,
            udfFunctionImplementations
        );
        if (func == null) {
            raiseUnknownFunction(suppliedSchema, functionName, arguments, List.of());
        }
        return func;
    }


    @Nullable
    private FunctionImplementation get(Signature signature,
                                       List<DataType<?>> actualArgumentTypes,
                                       DataType<?> actualReturnType,
                                       Map<FunctionName, List<FunctionProvider>> candidatesByName) {
        var candidates = candidatesByName.get(signature.getName());
        if (candidates == null) {
            return null;
        }
        for (var candidate : candidates) {
            if (candidate.getSignature().equals(signature)) {
                var boundSignature = new BoundSignature(actualArgumentTypes, actualReturnType);
                return candidate.getFactory().apply(signature, boundSignature);
            }
        }
        return null;
    }

    @Nullable
    private static FunctionImplementation resolveFunctionBySignature(FunctionName name,
                                                                     List<DataType<?>> argumentTypes,
                                                                     List<Symbol> arguments,
                                                                     SearchPath searchPath,
                                                                     Map<FunctionName, List<FunctionProvider>> builtInFunctions,
                                                                     Map<FunctionName, List<FunctionProvider>> udfs) {
        var candidates = builtInFunctions.get(name);
        if (candidates == null && name.schema() == null) {
            for (String pathSchema : searchPath) {
                FunctionName searchPathFunctionName = new FunctionName(pathSchema, name.name());
                candidates = builtInFunctions.get(searchPathFunctionName);
                if (candidates == null) {
                    candidates = udfs.get(searchPathFunctionName);
                }
                if (candidates != null) {
                    break;
                }
            }
        }
        if (candidates == null) {
            candidates = udfs.get(name);
            if (candidates == null) {
                return null;
            }
        }
        final var finalCandidates = candidates;

        assert candidates.stream().allMatch(f -> f.getSignature().getBindingInfo() != null) :
            "Resolving/Matching of signatures can only be done with non-null signature's binding info";

        // First lets try exact candidates, no generic type variables, no coercion allowed.
        Iterable<FunctionProvider> exactCandidates = () -> finalCandidates.stream()
            .filter(function -> function.getSignature().getBindingInfo().getTypeVariableConstraints().isEmpty())
            .iterator();
        var match = matchFunctionCandidates(exactCandidates, argumentTypes, SignatureBinder.CoercionType.NONE);
        if (match != null) {
            return match;
        }

        // Second, try candidates with generic type variables, still no coercion allowed.
        Iterable<FunctionProvider> genericCandidates = () -> finalCandidates.stream()
            .filter(function -> !function.getSignature().getBindingInfo().getTypeVariableConstraints().isEmpty())
            .iterator();
        match = matchFunctionCandidates(genericCandidates, argumentTypes, SignatureBinder.CoercionType.NONE);
        if (match != null) {
            return match;
        }

        // Third, try all candidates which allow coercion with precedence based coercion.
        Iterable<FunctionProvider> candidatesAllowingCoercion = () -> finalCandidates.stream()
            .filter(function -> function.getSignature().getBindingInfo().isCoercionAllowed())
            .iterator();
        match = matchFunctionCandidates(
            candidatesAllowingCoercion,
            argumentTypes,
            SignatureBinder.CoercionType.PRECEDENCE_ONLY
        );
        if (match != null) {
            return match;
        }

        // Last, try all candidates which allow coercion with full coercion.
        match = matchFunctionCandidates(candidatesAllowingCoercion, argumentTypes, SignatureBinder.CoercionType.FULL);

        if (match == null) {
            raiseUnknownFunction(name.schema(), name.name(), arguments, candidates);
        }
        return match;
    }

    @Nullable
    private static FunctionImplementation matchFunctionCandidates(Iterable<FunctionProvider> candidates,
                                                                  List<DataType<?>> arguments,
                                                                  SignatureBinder.CoercionType coercionType) {
        List<ApplicableFunction> applicableFunctions = new ArrayList<>();
        for (FunctionProvider candidate : candidates) {
            BoundSignature boundSignature = new SignatureBinder(candidate.getSignature(), coercionType)
                .bind(arguments);
            if (boundSignature != null) {
                applicableFunctions.add(
                    new ApplicableFunction(
                        candidate.getSignature(),
                        boundSignature,
                        candidate.getFactory()
                    )
                );
            }
        }

        if (coercionType != SignatureBinder.CoercionType.NONE) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, arguments);
            if (LOGGER.isDebugEnabled() && applicableFunctions.isEmpty()) {
                LOGGER.debug("At least single function must be left after selecting most specific one");
            }
        }

        if (applicableFunctions.size() == 1) {
            return getOnlyElement(applicableFunctions).get();
        }
        if (applicableFunctions.size() > 1) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Multiple candidates match! " + applicableFunctions);
            }
        }

        return null;
    }

    public FunctionImplementation getQualified(io.crate.expression.symbol.Function function) {
        var signature = function.signature();
        return getQualified(signature, Symbols.typeView(function.arguments()), function.valueType());
    }

    public FunctionImplementation getQualified(io.crate.expression.symbol.Aggregation function) {
        var signature = function.signature();
        return getQualified(signature, Symbols.typeView(function.inputs()), function.boundSignatureReturnType());
    }

    /**
     * Returns the function implementation for the given function signature.
     * First look up function in built-ins then fallback to user-defined functions.
     *
     * @param signature The function signature.
     * @return The function implementation.
     * @throws UnsupportedFunctionException if no implementation is found.
     */
    public FunctionImplementation getQualified(Signature signature,
                                               List<DataType<?>> actualArgumentTypes,
                                               DataType<?> actualReturnType) throws UnsupportedFunctionException {
        FunctionImplementation impl = get(signature, actualArgumentTypes, actualReturnType, functionImplementations);
        if (impl == null) {
            impl = get(signature, actualArgumentTypes, actualReturnType, udfFunctionImplementations);
        }
        return impl;
    }

    @VisibleForTesting
    static void raiseUnknownFunction(@Nullable String suppliedSchema,
                                     String name,
                                     List<Symbol> arguments,
                                     List<FunctionProvider> candidates) {
        List<DataType<?>> argumentTypes = Symbols.typeView(arguments);
        var function = new io.crate.expression.symbol.Function(
            Signature.builder()
                .name(new FunctionName(suppliedSchema, name))
                .argumentTypes(Lists.map(argumentTypes, DataType::getTypeSignature))
                .returnType(DataTypes.UNDEFINED.getTypeSignature())
                .kind(FunctionType.SCALAR)
                .build(),
            arguments,
            DataTypes.UNDEFINED
        );

        var message = "Unknown function: " + function.toString(Style.QUALIFIED);
        if (candidates.isEmpty() == false) {
            if (arguments.isEmpty() == false) {
                message = message + ", no overload found for matching argument types: "
                          + "(" + Lists.joinOn(", ", argumentTypes, DataType::toString) + ").";
            } else {
                message = message + ".";
            }
            message = message + " Possible candidates: "
                      + Lists.joinOn(
                          ", ",
                          candidates,
                          c -> c.getSignature().getName().displayName()
                               + "("
                               + Lists.joinOn(
                              ", ",
                              c.getSignature().getArgumentTypes(),
                              TypeSignature::toString)
                               + "):" + c.getSignature().getReturnType().toString())
                      ;
        }

        throw new UnsupportedFunctionException(message, suppliedSchema);
    }

    private static List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions,
                                                                        List<DataType<?>> arguments) {
        if (applicableFunctions.isEmpty()) {
            return applicableFunctions;
        }

        // Find most specific by number of exact argument type matches
        List<TypeSignature> argumentTypeSignatures = Lists.map(arguments, DataType::getTypeSignature);
        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(
            applicableFunctions,
            (l, r) -> hasMoreExactTypeMatches(l, r, argumentTypeSignatures));
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        // Find most specific by type precedence
        mostSpecificFunctions = selectMostSpecificFunctions(mostSpecificFunctions, Functions::isMoreSpecificThan);
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        // If the return type for all the selected function is the same
        // all the functions are semantically the same. We can return just any of those.
        //
        // Second, if all given arguments are UNDEFINED, we also return the first one.
        // This may not be 100% correct, but e.g. for
        //
        //   `concat(null, null)`
        //
        // we currently have no other option as we cannot specify which one should match by signatures.
        //
        //     `concat(text, text):text`
        //     `concat(array(E), array(E)):array(E)`
        //
        if (returnTypeIsTheSame(mostSpecificFunctions)
            || arguments.stream().allMatch(s -> s.id() == DataTypes.UNDEFINED.id())) {
            ApplicableFunction selectedFunction = mostSpecificFunctions.stream()
                .sorted(Comparator.comparing(Objects::toString))
                .iterator().next();

            return List.of(selectedFunction);
        }
        return mostSpecificFunctions;
    }

    private static List<ApplicableFunction> selectMostSpecificFunctions(
        List<ApplicableFunction> candidates,
        BiFunction<ApplicableFunction, ApplicableFunction, Boolean> isMoreSpecific) {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecific.apply(current, representative)) {
                    representatives.clear();
                    representatives.add(current);
                    found = true;
                    break;
                } else if (isMoreSpecific.apply(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives;
    }

    /**
     * One method is more specific than another if invocation handled by the first method
     * could be passed on to the other one.
     * Additionally possible variadic type signatures are taken into account,
     * an exact amount of declared type signatures is more specific than expanded variadic type signatures.
     */
    private static boolean isMoreSpecificThan(ApplicableFunction left,
                                              ApplicableFunction right) {
        List<DataType<?>> resolvedTypes = left.getBoundSignature().argTypes();
        BoundVariables boundVariables = SignatureBinder.withPrecedenceOnly(right.getDeclaredSignature())
            .bindVariables(resolvedTypes);
        if (boundVariables == null) {
            return false;
        }

        int leftArgsCount = left.getDeclaredSignature().getArgumentTypes().size();
        int rightArgsCount = right.getDeclaredSignature().getArgumentTypes().size();
        return leftArgsCount >= rightArgsCount;
    }

    private static boolean hasMoreExactTypeMatches(ApplicableFunction left,
                                                   ApplicableFunction right,
                                                   List<TypeSignature> actualArgumentTypes) {
        int leftExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            left.getDeclaredSignature().getArgumentTypes()
        );
        int rightExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            right.getDeclaredSignature().getArgumentTypes()
        );
        return leftExactMatches > rightExactMatches;
    }

    private static boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions) {
        Set<DataType<?>> returnTypes = applicableFunctions.stream()
            .map(function -> function.getBoundSignature().returnType())
            .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private static int numberOfExactTypeMatches(List<TypeSignature> actualArgumentTypes,
                                                List<TypeSignature> declaredArgumentTypes) {
        int cnt = 0;
        for (int i = 0; i < actualArgumentTypes.size(); i++) {
            if (declaredArgumentTypes.size() > i
                && actualArgumentTypes.get(i).equalsIgnoringParameters(declaredArgumentTypes.get(i))) {
                cnt++;
            }
        }
        return cnt;
    }

    private static class ApplicableFunction implements Supplier<FunctionImplementation> {

        private final Signature declaredSignature;
        private final BoundSignature boundSignature;
        private final FunctionFactory factory;

        public ApplicableFunction(Signature declaredSignature,
                                  BoundSignature boundSignature,
                                  FunctionFactory factory) {
            this.declaredSignature = declaredSignature;
            this.boundSignature = boundSignature;
            this.factory = factory;
        }

        public Signature getDeclaredSignature() {
            return declaredSignature;
        }

        public BoundSignature getBoundSignature() {
            return boundSignature;
        }

        @Override
        public FunctionImplementation get() {
            return factory.apply(
                declaredSignature,
                boundSignature
            );
        }

        @Override
        public String toString() {
            return "ApplicableFunction{" +
                   "declaredSignature=" + declaredSignature +
                   ", boundSignature=" + boundSignature +
                   '}';
        }
    }
}
