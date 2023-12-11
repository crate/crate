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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.FunctionToQuery;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.RoleLookup;

/**
 * Base class for Scalar functions in crate.
 * A Scalar function is a function which has zero or more arguments and returns a value. (not rows).
 * <p>
 * Argument types and return types are restricted to the types supported by Crate (see {@link io.crate.types.DataType})
 * </p>
 *
 * <p>
 *     Usually functions are registered as deterministic (See {@link Feature}.
 *     If this is the case the function must be a pure function. Meaning that given the same input it must always produce
 *     the same output.
 *
 *     Functions also MUST NOT have any internal state that influences the result of future calls.
 *     Functions are used as singletons.
 *     An exception is if {@link #compile(List, String, RoleLookup)} returns a NEW instance.
 * </p>
 *
 * To implement scalar functions, you may want to use one of the following abstractions:
 *
 * <ul>
 *     <li>{@link io.crate.expression.scalar.UnaryScalar}</li>
 *     <li>{@link io.crate.expression.scalar.arithmetic.BinaryScalar}</li>
 *     <li>{@link io.crate.expression.scalar.DoubleScalar}</li>
 *     <li>{@link io.crate.expression.scalar.TripleScalar}</li>
 * </ul>
 *
 * @param <ReturnType> the class of the returned value
 */
public abstract class Scalar<ReturnType, InputType> implements FunctionImplementation, FunctionToQuery {

    public static final Set<Feature> NO_FEATURES = Set.of();
    public static final Set<Feature> DETERMINISTIC_ONLY = EnumSet.of(Feature.DETERMINISTIC);
    public static final Set<Feature> DETERMINISTIC_AND_COMPARISON_REPLACEMENT = EnumSet.of(
        Feature.DETERMINISTIC, Feature.COMPARISON_REPLACEMENT);

    protected final Signature signature;
    protected final BoundSignature boundSignature;

    protected Scalar(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    /**
     * Evaluate the function using the provided arguments
     */
    public abstract ReturnType evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<InputType>... args);

    /**
     * Called to return a "optimized" version of a scalar implementation.
     *
     * The returned instance will only be used in the context of a single query
     * (or rather, a subset of a single query if executed distributed).
     *
     * @param arguments arguments in symbol form. If any symbols are literals, any arguments passed to
     *                  {@link #evaluate(TransactionContext, NodeContext, Input[])} will have the same
     *                  value as those literals. (Within the scope of a single operation)
     */
    public Scalar<ReturnType, InputType> compile(List<Symbol> arguments, String currentUser, RoleLookup userLookup) {
        return this;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        try {
            return evaluateIfLiterals(this, txnCtx, nodeCtx, symbol);
        } catch (Throwable t) {
            return symbol;
        }
    }

    protected static boolean anyNonLiterals(Collection<? extends Symbol> arguments) {
        for (Symbol symbol : arguments) {
            if (!symbol.symbolType().isValueSymbol()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will evaluate the function using the given scalar if all arguments are literals.
     * Otherwise it will return the function as is or NULL in case it contains a null literal
     */
    protected static <ReturnType, InputType> Symbol evaluateIfLiterals(Scalar<ReturnType, InputType> scalar,
                                                                       TransactionContext txnCtx,
                                                                       NodeContext nodeCtx,
                                                                       Function function) {
        List<Symbol> arguments = function.arguments();
        for (Symbol argument : arguments) {
            if (!(argument instanceof Input)) {
                return function;
            }
        }
        Input[] inputs = new Input[arguments.size()];
        int idx = 0;
        for (Symbol arg : arguments) {
            inputs[idx] = (Input<?>) arg;
            idx++;
        }
        //noinspection unchecked
        return Literal.ofUnchecked(function.valueType(), scalar.evaluate(txnCtx, nodeCtx, inputs));
    }

    public enum Feature {
        DETERMINISTIC,
        /**
         * If this feature is set, for this function it is possible to replace the containing
         * comparison while preserving the truth value for all used operators
         * with or without an operator mapping.
         * <p>
         * It describes the following:
         * <p>
         * say we have a comparison-query like this:
         * <p>
         * col > 10.5
         * <p>
         * then a function f, for which comparisons are replaceable, can be applied so
         * that:
         * <p>
         * f(col) > f(10.5)
         * <p>
         * for all col for which col > 10.5 is true. Maybe > needs to be mapped to another
         * operator, but this may not depend on the actual values used here.
         * <p>
         * Fun facts:
         * <p>
         * * This property holds for the = comparison operator for all functions f.
         * * This property is transitive so if f and g are replaceable,
         * then f(g(x)) also is
         * * it is possible to replace:
         * <p>
         * col > 10.5
         * <p>
         * with:
         * <p>
         * f(col) > f(10.5)
         * <p>
         * for every operator (possibly mapped) and the query is still equivalent.
         * <p>
         * Example 1:
         * <p>
         * if f is defined as f(v) = v + 1
         * then col + 1 > 11.5 must be true for all col > 10.5.
         * This is indeed true.
         * <p>
         * So f is replaceable for >.
         * <p>
         * Fun fact: for f all comparison operators =, >, <, >=,<= are replaceable
         * <p>
         * Example 2 (a rounding function):
         * <p>
         * if f is defined as f(v) = ceil(v)
         * then ceil(col) > 11 for all col > 10.5.
         * But there is 10.8 for which f is 11 and
         * 11 > 11 is false.
         * <p>
         * Here a simple mapping of the operator will do the trick:
         * <p>
         * > -> >=
         * < -> <=
         * <p>
         * So for f comparisons are replaceable using the mapping above.
         * <p>
         * Example 3:
         * <p>
         * if f is defined as f(v) = v % 5
         * then col % 5 > 0.5 for all col > 10.5
         * but there is 20 for which
         * f is 0 and
         * 0 > 0.5 is false.
         * <p>
         * So for f comparisons cannot be replaced.
         */
        COMPARISON_REPLACEMENT,
        LAZY_ATTRIBUTES,
        /**
         * If this feature is set, the function will return for null argument(s) as result null.
         */
        NULLABLE,
        /**
         * If this feature is set, the function will never return null.
         */
        NON_NULLABLE
    }
}
