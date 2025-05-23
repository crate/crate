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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.CompositeClassLoader;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

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
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class Functions {

    private static final Logger LOGGER = LogManager.getLogger(Functions.class);

    private volatile Map<FunctionName, List<FunctionProvider>> udfFunctionImplementations = Map.of();
    private final Map<FunctionName, List<FunctionProvider>> functionImplementations;

    public static class Builder {

        private HashMap<FunctionName, ArrayList<FunctionProvider>> providersByName = new HashMap<>();

        public void add(Signature signature, FunctionFactory factory) {
            List<FunctionProvider> functionProviders = providersByName.computeIfAbsent(
                signature.getName(),
                k -> new ArrayList<>()
            );
            functionProviders.add(new FunctionProvider(signature, factory));
        }

        public Functions build() {
            for (ArrayList<FunctionProvider> providers : providersByName.values()) {
                providers.trimToSize();
            }
            return new Functions(Map.copyOf(providersByName));
        }
    }

    public static Functions load(Settings settings,
                                 SessionSettingRegistry sessionSettingRegistry,
                                 ClassLoader ... classLoaders) {
        Builder builder = new Builder();
        CompositeClassLoader compositeLoader = new CompositeClassLoader(
            FunctionsProvider.class.getClassLoader(),
            List.of(classLoaders)
        );
        for (var provider : ServiceLoader.load(FunctionsProvider.class, compositeLoader)) {
            provider.addFunctions(settings, sessionSettingRegistry, builder);
        }
        return builder.build();
    }

    private Functions(Map<FunctionName, List<FunctionProvider>> functionProvidersByName) {
        this.functionImplementations = functionProvidersByName;
    }

    public Iterable<Signature> signatures() {
        return () ->
            Stream.concat(
                functionImplementations.values().stream()
                    .flatMap(x -> x.stream())
                    .map(x -> x.signature()),
                udfFunctionImplementations.values().stream()
                    .flatMap(x -> x.stream())
                    .map(x -> x.signature())
            )
            .iterator();
    }

    public void setUDFs(Map<FunctionName, List<FunctionProvider>> functions) {
        udfFunctionImplementations = functions;
    }

    @Nullable
    public Signature findFunctionSignatureByOid(int oid) {
        for (var signature : signatures()) {
            if (oid == OidHash.functionOid(signature)) {
                return signature;
            }
        }
        return null;
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
            if (candidate.signature().equals(signature)) {
                var boundSignature = new BoundSignature(actualArgumentTypes, actualReturnType);
                return candidate.factory().apply(signature, boundSignature);
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

        assert candidates.stream().allMatch(f -> f.signature().getBindingInfo() != null) :
            "Resolving/Matching of signatures can only be done with non-null signature's binding info";

        // First lets try exact candidates, no generic type variables, no coercion allowed.
        Iterable<FunctionProvider> exactCandidates = () -> finalCandidates.stream()
            .filter(function -> function.signature().getBindingInfo().getTypeVariableConstraints().isEmpty())
            .iterator();
        var match = matchFunctionCandidates(exactCandidates, argumentTypes, SignatureBinder.CoercionType.NONE);
        if (match != null) {
            return match;
        }

        // Second, try candidates with generic type variables, still no coercion allowed.
        Iterable<FunctionProvider> genericCandidates = () -> finalCandidates.stream()
            .filter(function -> !function.signature().getBindingInfo().getTypeVariableConstraints().isEmpty())
            .iterator();
        match = matchFunctionCandidates(genericCandidates, argumentTypes, SignatureBinder.CoercionType.NONE);
        if (match != null) {
            return match;
        }

        // Third, try all candidates which allow coercion with precedence based coercion.
        Iterable<FunctionProvider> candidatesAllowingCoercion = () -> finalCandidates.stream()
            .filter(function -> function.signature().getBindingInfo().isCoercionAllowed())
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
            BoundSignature boundSignature = new SignatureBinder(candidate.signature(), coercionType)
                .bind(arguments);
            if (boundSignature != null) {
                applicableFunctions.add(
                    new ApplicableFunction(
                        candidate.signature(),
                        boundSignature,
                        candidate.factory()
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
            Signature.builder(new FunctionName(suppliedSchema, name), FunctionType.SCALAR)
                .argumentTypes(Lists.map(argumentTypes, DataType::getTypeSignature))
                .returnType(DataTypes.UNDEFINED.getTypeSignature())
                .build(),
            arguments,
            DataTypes.UNDEFINED
        );

        StringBuilder sb = new StringBuilder();
        if (candidates.isEmpty()) {
            sb.append("Unknown function: ");
            sb.append(function.toString(Style.QUALIFIED));
        } else {
            sb.append("Invalid arguments in: ");
            sb.append(function.toString(Style.QUALIFIED));
            if (arguments.isEmpty()) {
                sb.append(".");
            } else {
                sb.append(" with (");
                Iterator<DataType<?>> argTypesIt = argumentTypes.iterator();
                while (argTypesIt.hasNext()) {
                    sb.append(argTypesIt.next().toString());
                    if (argTypesIt.hasNext()) {
                        sb.append(", ");
                    }
                }
                sb.append(").");
            }
            sb.append(" Valid types: ");
            Iterator<FunctionProvider> candidatesIt = candidates.iterator();
            while (candidatesIt.hasNext()) {
                FunctionProvider candidate = candidatesIt.next();
                Signature signature = candidate.signature();
                sb.append("(");
                sb.append(Lists.joinOn(", ", signature.getArgumentTypes(), TypeSignature::toString));
                sb.append(")");
                if (candidatesIt.hasNext()) {
                    sb.append(", ");
                }
            }
        }
        throw new UnsupportedFunctionException(sb.toString(), suppliedSchema);
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
            boolean addCandidate = true;
            var it = representatives.iterator();
            while (it.hasNext()) {
                ApplicableFunction representative = it.next();
                if (representative.equals(current)) {
                    continue;
                }
                if (isMoreSpecific.apply(current, representative)) {
                    it.remove();
                    addCandidate = true;
                } else if (isMoreSpecific.apply(representative, current)) {
                    addCandidate = false;
                }
            }

            if (addCandidate) {
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
        List<DataType<?>> resolvedTypes = left.boundSignature().argTypes();
        BoundVariables boundVariables = SignatureBinder.withPrecedenceOnly(right.signature())
            .bindVariables(resolvedTypes);
        if (boundVariables == null) {
            return false;
        }

        int leftArgsCount = left.signature().getArgumentTypes().size();
        int rightArgsCount = right.signature().getArgumentTypes().size();
        return leftArgsCount >= rightArgsCount;
    }

    private static boolean hasMoreExactTypeMatches(ApplicableFunction left,
                                                   ApplicableFunction right,
                                                   List<TypeSignature> actualArgumentTypes) {
        int leftExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            left.signature().getArgumentTypes()
        );
        int rightExactMatches = numberOfExactTypeMatches(
            actualArgumentTypes,
            right.signature().getArgumentTypes()
        );
        return leftExactMatches > rightExactMatches;
    }

    private static boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions) {
        Set<DataType<?>> returnTypes = applicableFunctions.stream()
            .map(function -> function.boundSignature().returnType())
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

    private static record ApplicableFunction(
            Signature signature,
            BoundSignature boundSignature,
            FunctionFactory factory) implements Supplier<FunctionImplementation> {

        @Override
        public FunctionImplementation get() {
            return factory.apply(signature, boundSignature);
        }

        @Override
        public String toString() {
            return "ApplicableFunction{" +
                   "signature=" + signature +
                   ", boundSignature=" + boundSignature +
                   '}';
        }
    }
}
