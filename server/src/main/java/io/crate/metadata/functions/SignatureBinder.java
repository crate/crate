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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;
import static io.crate.types.TypeCompatibility.getCommonType;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ParameterTypeSignature;
import io.crate.types.TypeSignature;
import io.crate.types.UndefinedType;


/**
 * Determines whether, and how, a callsite matches a generic function signature.
 * Which is equivalent to finding assignments for the variables in the generic signature,
 * such that all of the function's declared parameters are super types of the corresponding
 * arguments, and also satisfy the declared constraints (such as a given type parameter must
 * be of the same type or not)
 */
public class SignatureBinder {
    // 4 is chosen arbitrarily here. This limit is set to avoid having infinite loops in iterative solving.
    private static final int SOLVE_ITERATION_LIMIT = 4;

    private static final Logger LOGGER = LogManager.getLogger(SignatureBinder.class);

    public static SignatureBinder withPrecedenceOnly(Signature declaredSignature) {
        return new SignatureBinder(declaredSignature, CoercionType.PRECEDENCE_ONLY);
    }

    private final Signature declaredSignature;
    private final CoercionType coercionType;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    /**
     * Types where we should ignore precision details while signature matching.
     * This is done because functions are registered without precision details.
     *
     * E.g.  `foo(text)` should also match on `foo(x)` where `x` has `varchar(10)`
     **/
    private static final Set<String> ALLOW_BASENAME_MATCH = Set.of(
        DataTypes.NUMERIC.getName(),
        DataTypes.STRING.getName(),
        DataTypes.CHARACTER.getName(),
        BitStringType.NAME
    );

    public SignatureBinder(Signature declaredSignature, CoercionType coercionType) {
        this.declaredSignature = declaredSignature;
        this.coercionType = coercionType;
        this.typeVariableConstraints = declaredSignature.getBindingInfo().getTypeVariableConstraints().stream()
            .collect(toMap(TypeVariableConstraint::getName, identity()));
    }

    @Nullable
    public BoundSignature bind(List<DataType<?>> actualArgumentTypes) {
        BoundVariables boundVariables = bindVariables(actualArgumentTypes);
        if (boundVariables == null) {
            return null;
        }
        int arity = actualArgumentTypes.size();
        List<TypeSignature> argumentSignatures;
        var bindingInfo = declaredSignature.getBindingInfo();
        assert bindingInfo != null : "Expecting the signature's binding info to be not null";
        if (bindingInfo.isVariableArity()) {
            argumentSignatures = expandVarargFormalTypeSignature(
                declaredSignature.getArgumentTypes(),
                bindingInfo.getVariableArityGroup(),
                typeVariableConstraints,
                arity);
            if (argumentSignatures == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Size of argument types does not match a multiple of the defined variable arguments");
                }
                return null;
            }
        } else {
            if (declaredSignature.getArgumentTypes().size() != arity) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Size of argument types does not match given arity");
                }
                return null;
            }
            argumentSignatures = declaredSignature.getArgumentTypes();
        }
        List<TypeSignature> boundArgumentSignatures = applyBoundVariables(argumentSignatures, boundVariables, true);
        TypeSignature boundReturnTypeSignature = applyBoundVariables(declaredSignature.getReturnType(), boundVariables, false);

        return new BoundSignature(
            Lists.map(boundArgumentSignatures, TypeSignature::createType),
            boundReturnTypeSignature.createType()
        );
    }

    @Nullable
    public BoundVariables bindVariables(List<DataType<?>> actualArgumentTypes) {
        ArrayList<TypeConstraintSolver> constraintSolvers = new ArrayList<>();
        if (!appendConstraintSolversForArguments(constraintSolvers, actualArgumentTypes)) {
            return null;
        }

        return iterativeSolve(Collections.unmodifiableList(constraintSolvers));
    }

    private static List<TypeSignature> applyBoundVariables(List<TypeSignature> typeSignatures,
                                                           BoundVariables boundVariables,
                                                           boolean onlyBindGenericTypes) {
        ArrayList<TypeSignature> builder = new ArrayList<>();
        for (TypeSignature typeSignature : typeSignatures) {
            builder.add(applyBoundVariables(typeSignature, boundVariables, onlyBindGenericTypes));
        }
        return Collections.unmodifiableList(builder);
    }

    private static TypeSignature applyBoundVariables(TypeSignature typeSignature, BoundVariables boundVariables, boolean onlyBindGenericTypes) {
        String baseType = typeSignature.getBaseTypeName();
        if (boundVariables.containsTypeVariable(baseType)) {
            if (typeSignature.getParameters().isEmpty() == false) {
                throw new IllegalStateException("Type parameters cannot have parameters");
            }
            var boundTS = boundVariables.getTypeVariable(baseType).getTypeSignature();
            if (onlyBindGenericTypes && Objects.equals(boundTS.getBaseTypeName(), baseType)) {
                return typeSignature;
            }
            if (typeSignature instanceof ParameterTypeSignature p) {
                return new ParameterTypeSignature(p.unescapedParameterName(), boundTS);
            }
            return boundTS;
        }

        List<TypeSignature> parameters = Lists.map(
            typeSignature.getParameters(),
            typeSignatureParameter -> applyBoundVariables(typeSignatureParameter, boundVariables, onlyBindGenericTypes));

        if (typeSignature instanceof ParameterTypeSignature p) {
            return new ParameterTypeSignature(
                p.unescapedParameterName(),
                new TypeSignature(baseType, parameters)
            );
        }
        return new TypeSignature(baseType, parameters);
    }

    private boolean appendConstraintSolversForArguments(List<TypeConstraintSolver> resultBuilder,
                                                        List<DataType<?>> argumentTypes) {
        var declaredBindingInfo = declaredSignature.getBindingInfo();
        assert declaredBindingInfo != null : "Expecting the signature's binding info to be not null";
        boolean variableArity = declaredBindingInfo.isVariableArity();
        List<TypeSignature> formalTypeSignatures = declaredSignature.getArgumentTypes();
        if (variableArity) {
            int variableGroupCount = declaredBindingInfo.getVariableArityGroup().size();
            int variableArgumentCount = variableGroupCount > 0 ? variableGroupCount : 1;
            if (argumentTypes.size() <= formalTypeSignatures.size() - variableArgumentCount) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                        "Given signature size {} is not smaller than minimum variableArity of formal signature size {}",
                        argumentTypes.size(),
                        formalTypeSignatures.size() - variableArgumentCount);
                }
                return false;
            }
            formalTypeSignatures = expandVarargFormalTypeSignature(
                formalTypeSignatures,
                declaredBindingInfo.getVariableArityGroup(),
                typeVariableConstraints,
                argumentTypes.size());
            if (formalTypeSignatures == null) {
                // var args expanding detected a no-match
                return false;
            }
        }

        if (formalTypeSignatures.size() != argumentTypes.size()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Given signature size {} does not match formal signature size {}",
                             argumentTypes.size(), formalTypeSignatures.size());
            }
            return false;
        }

        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            appendTypeRelationshipConstraintSolver(resultBuilder,
                                                   formalTypeSignatures.get(i),
                                                   argumentTypes.get(i),
                                                   coercionType);
        }

        return appendConstraintSolvers(resultBuilder, formalTypeSignatures, argumentTypes, coercionType);
    }

    private boolean appendConstraintSolvers(List<TypeConstraintSolver> resultBuilder,
                                            List<? extends TypeSignature> formalTypeSignatures,
                                            List<DataType<?>> actualArgumentTypes,
                                            CoercionType coercionType) {
        if (formalTypeSignatures.size() != actualArgumentTypes.size()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Given signature size {} does not match formal signature size {}",
                             actualArgumentTypes.size(), formalTypeSignatures.size());
            }
            return false;
        }
        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            if (!appendConstraintSolvers(resultBuilder,
                                         formalTypeSignatures.get(i),
                                         actualArgumentTypes.get(i),
                                         coercionType)) {
                return false;
            }
        }
        return true;
    }

    private boolean appendConstraintSolvers(List<TypeConstraintSolver> resultBuilder,
                                            TypeSignature formalTypeSignature,
                                            DataType<?> actualType,
                                            CoercionType coercionType) {
        if (formalTypeSignature.getParameters().isEmpty()) {
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(formalTypeSignature.getBaseTypeName());
            if (typeVariableConstraint == null) {
                // No generic type parameter, but we may have a type with numeric params on it
                var actualTypeSignature = actualType.getTypeSignature();
                if (Objects.equals(formalTypeSignature.getBaseTypeName(), actualTypeSignature.getBaseTypeName())
                    && actualTypeSignature.hasNumericParameters()) {
                    resultBuilder.add(new TypeParameterSolver(formalTypeSignature.getBaseTypeName(), actualType));
                }
                return true;
            }
            resultBuilder.add(new TypeParameterSolver(formalTypeSignature.getBaseTypeName(), actualType));
            return true;
        }

        List<DataType<?>> actualTypeParameters;
        if (UndefinedType.ID == actualType.id()) {
            actualTypeParameters = Collections.nCopies(formalTypeSignature.getParameters().size(), UndefinedType.INSTANCE);
        } else {
            actualTypeParameters = actualType.getTypeParameters();
        }
        return appendConstraintSolvers(
            resultBuilder,
            Collections.unmodifiableList(formalTypeSignature.getParameters()),
            actualTypeParameters,
            coercionType);
    }

    private void appendTypeRelationshipConstraintSolver(List<TypeConstraintSolver> resultBuilder,
                                                        TypeSignature formalTypeSignature,
                                                        DataType<?> actualType,
                                                        CoercionType coercionType) {
        Set<String> typeVariables = typeVariablesOf(formalTypeSignature);
        resultBuilder.add(new TypeRelationshipConstraintSolver(
            formalTypeSignature,
            typeVariables,
            actualType,
            coercionType));
    }

    private Set<String> typeVariablesOf(TypeSignature typeSignature) {
        if (typeVariableConstraints.containsKey(typeSignature.getBaseTypeName())) {
            return Set.of(typeSignature.getBaseTypeName());
        }
        HashSet<String> variables = new HashSet<>();
        for (TypeSignature parameter : typeSignature.getParameters()) {
            variables.addAll(typeVariablesOf(parameter));
        }

        return variables;
    }

    @Nullable
    private BoundVariables iterativeSolve(List<TypeConstraintSolver> constraints) {
        BoundVariables.Builder boundVariablesBuilder = BoundVariables.builder();
        for (int i = 0; true; i++) {
            if (i == SOLVE_ITERATION_LIMIT) {
                throw new IllegalStateException(format(
                    Locale.ENGLISH,
                    "SignatureBinder.iterativeSolve does not converge after %d iterations.",
                    SOLVE_ITERATION_LIMIT));
            }
            SolverReturnStatusMerger statusMerger = new SolverReturnStatusMerger();
            for (TypeConstraintSolver constraint : constraints) {
                var constraintStatus = constraint.update(boundVariablesBuilder);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Status after updating constraint={}: {}", constraint, constraintStatus);
                }
                statusMerger.add(constraintStatus);
                if (statusMerger.getCurrent() == SolverReturnStatus.UNSOLVABLE) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Status merger resulted in UNSOLVABLE state");
                    }
                    return null;
                }
            }
            switch (statusMerger.getCurrent()) {
                case UNCHANGED_SATISFIED:
                    break;
                case UNCHANGED_NOT_SATISFIED:
                    return null;
                case CHANGED:
                    continue;
                default:
                case UNSOLVABLE:
                    throw new UnsupportedOperationException("Signature binding unsolvable");
            }
            break;
        }

        BoundVariables boundVariables = boundVariablesBuilder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Not all variables are bound. Defined variables={}, bound={}",
                             typeVariableConstraints,
                             boundVariables);
            }
            return null;
        }
        return boundVariables;
    }

    private boolean allTypeVariablesBound(BoundVariables boundVariables) {
        // This is a 'containsAll' rather than an 'equals' because BoundVariables
        // may contain parametrized type mappings, eg numeric=>numeric(10) as well
        // as type variable mappings, eg E=>integer
        return boundVariables.getTypeVariableNames().containsAll(typeVariableConstraints.keySet());
    }

    @Nullable
    private static List<TypeSignature> expandVarargFormalTypeSignature(List<TypeSignature> formalTypeSignatures,
                                                                       List<TypeSignature> variableArityGroup,
                                                                       Map<String, TypeVariableConstraint> typeVariableConstraints,
                                                                       int actualArity) {
        int variableArityGroupCount = variableArityGroup.size();
        if (variableArityGroupCount > 0 && actualArity % variableArityGroupCount != 0) {
            // no match
            return null;
        }
        int arityCountIncludedInsideFormalSignature = variableArityGroupCount == 0 ? 1 : variableArityGroupCount;
        int variableArityArgumentsCount = actualArity - formalTypeSignatures.size() + arityCountIncludedInsideFormalSignature;
        if (variableArityArgumentsCount == 0) {
            return formalTypeSignatures.subList(0, formalTypeSignatures.size() - arityCountIncludedInsideFormalSignature);
        }
        if (variableArityArgumentsCount == arityCountIncludedInsideFormalSignature) {
            return formalTypeSignatures;
        }
        if (variableArityArgumentsCount > arityCountIncludedInsideFormalSignature && formalTypeSignatures.isEmpty()) {
            throw new IllegalArgumentException("Found variable argument(s) but list of formal type signatures is empty");
        }

        ArrayList<TypeSignature> builder = new ArrayList<>(formalTypeSignatures);
        if (variableArityGroup.isEmpty()) {
            TypeSignature lastTypeSignature = formalTypeSignatures.get(formalTypeSignatures.size() - 1);
            for (int i = 1; i < variableArityArgumentsCount; i++) {
                addVarArgTypeSignature(lastTypeSignature, typeVariableConstraints, builder, i);
            }
        } else {
            for (int i = 0; i < variableArityArgumentsCount - formalTypeSignatures.size(); ) {
                i += variableArityGroupCount;
                for (var typeSignature : variableArityGroup) {
                    addVarArgTypeSignature(typeSignature, typeVariableConstraints, builder, i);
                }
            }
        }
        return Collections.unmodifiableList(builder);
    }

    private static void addVarArgTypeSignature(TypeSignature typeSignature,
                                               Map<String, TypeVariableConstraint> typeVariableConstraints,
                                               List<TypeSignature> builder,
                                               int actualArity) {
        TypeVariableConstraint typeVariableConstraint = resolveTypeVariableConstraint(
            typeSignature,
            typeVariableConstraints
        );
        if (typeVariableConstraint != null && typeVariableConstraint.isAnyAllowed()) {
            // Type variables defaults to be bound to the same type.
            // To support independent variable type arguments, each vararg must be bound to a dedicated type variable.
            var newConstraintName = "_generated_" + typeVariableConstraint + actualArity;
            var newTypeSignature = replaceTypeVariable(
                typeSignature,
                typeVariableConstraint.getName(),
                newConstraintName
            );
            typeVariableConstraints.put(newConstraintName, typeVariableOfAnyType(newConstraintName));
            builder.add(newTypeSignature);
        } else {
            builder.add(typeSignature);
        }
    }

    @Nullable
    private static TypeVariableConstraint resolveTypeVariableConstraint(
        TypeSignature signature,
        Map<String, TypeVariableConstraint> constraints) {

        if (signature.getParameters().isEmpty()) {
            return constraints.get(signature.getBaseTypeName());
        } else {
            for (var parameterSignature : signature.getParameters()) {
                var constraint = resolveTypeVariableConstraint(parameterSignature, constraints);
                if (constraint != null) {
                    return constraint;
                }
            }
            return null;
        }
    }

    private static TypeSignature replaceTypeVariable(TypeSignature signature, String oldVar, String newVar) {
        if (signature.getBaseTypeName().equalsIgnoreCase(oldVar)) {
            return new TypeSignature(newVar, signature.getParameters());
        } else {
            ArrayList<TypeSignature> parameters = new ArrayList<>();
            for (var parameter : signature.getParameters()) {
                parameters.add(replaceTypeVariable(parameter, oldVar, newVar));
            }
            return new TypeSignature(signature.getBaseTypeName(), parameters);
        }
    }

    private static boolean satisfiesCoercion(CoercionType coercionType,
                                             DataType<?> fromType,
                                             TypeSignature toTypeSignature) {
        switch (coercionType) {
            case FULL:
                return fromType.isConvertableTo(toTypeSignature.createType(), false);
            case PRECEDENCE_ONLY:
                var toType = toTypeSignature.createType();
                if (checkParametrizedTypes(fromType.getTypeSignature(), toTypeSignature)) {
                    return true;
                }
                return fromType.equals(toType)
                       || (fromType.isConvertableTo(toTypeSignature.createType(), false)
                          && toType.precedes(fromType));
            case NONE:
            default:
                var fromTypeSignature = fromType.getTypeSignature();
                if (checkParametrizedTypes(fromTypeSignature, toTypeSignature)) {
                    return true;
                }
                return fromTypeSignature.equals(toTypeSignature);
        }
    }

    private static boolean checkParametrizedTypes(TypeSignature fromTypeSignature, TypeSignature toTypeSignature) {
        // We always register numeric and text arguments without precision and scale thus the parameters
        // should not be checked while signature matching.
        String baseTypeName = fromTypeSignature.getBaseTypeName();
        return ALLOW_BASENAME_MATCH.contains(baseTypeName) && baseTypeName.equals(toTypeSignature.getBaseTypeName());
    }

    private interface TypeConstraintSolver {
        SolverReturnStatus update(BoundVariables.Builder bindings);
    }

    private enum SolverReturnStatus {
        UNCHANGED_SATISFIED,
        UNCHANGED_NOT_SATISFIED,
        CHANGED,
        UNSOLVABLE,
    }

    private static class SolverReturnStatusMerger {
        // This class gives the overall status when multiple status are seen from different parts.
        // The logic can be summarized as finding the right most item (based on the list below) seen so far:
        //   UNCHANGED_SATISFIED, UNCHANGED_NOT_SATISFIED, CHANGED, UNSOLVABLE
        // If no item was seen ever, it provides UNCHANGED_SATISFIED.

        private SolverReturnStatus current = SolverReturnStatus.UNCHANGED_SATISFIED;

        public void add(SolverReturnStatus newStatus) {
            if (newStatus.ordinal() > current.ordinal()) {
                current = newStatus;
            }
        }

        public SolverReturnStatus getCurrent() {
            return current;
        }
    }

    private static class TypeParameterSolver implements TypeConstraintSolver {
        private final String typeParameter;
        private final DataType<?> actualType;

        public TypeParameterSolver(String typeParameter,
                                   DataType<?> actualType) {
            this.typeParameter = typeParameter;
            this.actualType = actualType;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings) {
            if (!bindings.containsTypeVariable(typeParameter)) {
                bindings.setTypeVariable(typeParameter, actualType);
                return SolverReturnStatus.CHANGED;
            }
            DataType<?> originalType = bindings.getTypeVariable(typeParameter);
            DataType<?> commonType = getCommonType(originalType, actualType);
            if (commonType == null) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (commonType.equals(originalType)) {
                return SolverReturnStatus.UNCHANGED_SATISFIED;
            }
            bindings.setTypeVariable(typeParameter, commonType);
            return SolverReturnStatus.CHANGED;
        }

        @Override
        public String toString() {
            return "TypeParameterSolver{" +
                   "typeParameter='" + typeParameter + "'" +
                   ", actualType=" + actualType +
                   '}';
        }
    }

    private static class TypeRelationshipConstraintSolver implements TypeConstraintSolver {
        private final TypeSignature superTypeSignature;
        private final Set<String> typeVariables;
        private final DataType<?> actualType;
        private final CoercionType coercionType;

        public TypeRelationshipConstraintSolver(TypeSignature superTypeSignature,
                                                Set<String> typeVariables,
                                                DataType<?> actualType,
                                                CoercionType coercionType) {
            this.superTypeSignature = superTypeSignature;
            this.typeVariables = typeVariables;
            this.actualType = actualType;
            this.coercionType = coercionType;
        }

        @Override
        public SolverReturnStatus update(BoundVariables.Builder bindings) {
            for (String variable : typeVariables) {
                if (!bindings.containsTypeVariable(variable)) {
                    return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                }
            }

            TypeSignature boundSignature = applyBoundVariables(superTypeSignature, bindings.build(), false);
            if (satisfiesCoercion(coercionType, actualType, boundSignature)) {
                return SolverReturnStatus.UNCHANGED_SATISFIED;
            }
            return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
        }

        @Override
        public String toString() {
            return "TypeRelationshipConstraintSolver{" +
                   "superTypeSignature=" + superTypeSignature +
                   ", typeVariables=" + typeVariables +
                   ", actualType=" + actualType +
                   ", allowCoercion=" + coercionType +
                   '}';
        }
    }

    public enum CoercionType {
        NONE,
        PRECEDENCE_ONLY,
        FULL
    }
}
