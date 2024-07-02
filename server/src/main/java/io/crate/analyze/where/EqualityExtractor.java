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

package io.crate.analyze.where;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.CartesianList;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.Operators;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class EqualityExtractor {

    private static final int MAX_ITERATIONS = 10_000;
    private static final Function NULL_MARKER = new Function(
        Signature.scalar("null_marker", DataTypes.UNDEFINED.getTypeSignature())
            .withFeature(Scalar.Feature.DETERMINISTIC),
        List.of(),
        DataTypes.UNDEFINED
    );
    private static final EqProxy NULL_MARKER_PROXY = new EqProxy(NULL_MARKER);

    private final EvaluatingNormalizer normalizer;

    public EqualityExtractor(EvaluatingNormalizer normalizer) {
        this.normalizer = normalizer;
    }

    public record EqMatches(@Nullable List<List<Symbol>> matches, Set<Symbol> unknowns) {

        public static final EqMatches NONE = new EqMatches(null, Set.of());
    }

    @VisibleForTesting
    protected int maxIterations() {
        return MAX_ITERATIONS;
    }

    public EqMatches extractParentMatches(List<ColumnIdent> columns, Symbol symbol, @Nullable TransactionContext coordinatorTxnCtx) {
        return extractMatches(columns, symbol, false, coordinatorTxnCtx);
    }

    /**
     * @return Matches of the {@code columns} within {@code symbol}.
     *
     * <pre>
     * Example for exact matches:
     *
     * columns: [x]
     *      x = ?                                           ->  [[?]]
     *      x = 1 or x = 2                                  ->  [[1], [2]]
     *      x = 1 and some_scalar(z, ...) = 2               -> [[1]]
     *      x = 1 or (x = 2 and z = 20)                     -> [[1], [2]]
     *
     * columns: [x, y]
     *      x = $1 and y = $2                               -> [[$1, $2]]
     *
     *      (x = 1 and y = 2) or (x = 2 and y = 3)          -> [[1, 2], [2, 3]]
     *
     *
     * columns: [x]
     *      x = 10 or y = 20                                -> null (y not inside columns)
     *      x = 10 and match(z, ...) = 'foo'                -> null (MATCH predicate can only be applied on lucene)
     * </pre>
     */
    public EqMatches extractMatches(List<ColumnIdent> columns,
                                    Symbol symbol,
                                    TransactionContext txnCtx) {
        return extractMatches(columns, symbol, true, txnCtx);
    }

    private EqMatches extractMatches(Collection<ColumnIdent> columns,
                                     Symbol query,
                                     boolean shortCircuitOnMatchPredicateUnknown,
                                     TransactionContext txnCtx) {
        var context = new ProxyInjectingVisitor.Context(columns);
        // Normalize the query so that any casts on literals are evaluated
        var normalizedQuery = normalizer.normalize(query, txnCtx);
        Symbol proxiedTree = normalizedQuery.accept(ProxyInjectingVisitor.INSTANCE, context);

        // Match cannot execute without Lucene
        if (shortCircuitOnMatchPredicateUnknown && context.unknowns.size() == 1) {
            Symbol unknown = context.unknowns.iterator().next();
            if (unknown instanceof MatchPredicate || unknown instanceof Function fn && fn.name().equals(io.crate.expression.predicate.MatchPredicate.NAME)) {
                return EqMatches.NONE;
            }
        }

        List<List<EqProxy>> comparisons = context.comparisonValues();
        List<List<EqProxy>> cp = CartesianList.of(comparisons);

        List<List<Symbol>> result = new ArrayList<>();
        int iterations = 0;
        for (List<EqProxy> proxies : cp) {
            // Protect against large queries, where the number of combinations to check grows large
            if (++iterations >= maxIterations()) {
                break;
            }
            boolean anyNull = false;
            for (EqProxy proxy : proxies) {
                if (proxy != NULL_MARKER_PROXY) {
                    proxy.setTrue();
                } else {
                    anyNull = true;
                }
            }
            Symbol normalized = normalizer.normalize(proxiedTree, txnCtx);
            if (normalized == Literal.BOOLEAN_TRUE) {
                if (anyNull) {
                    return EqMatches.NONE;
                }
                if (!proxies.isEmpty()) {
                    List<Symbol> row = new ArrayList<>(proxies.size());
                    for (EqProxy proxy : proxies) {
                        proxy.reset();
                        row.add(proxy.origin.arguments().get(1));
                    }
                    result.add(row);
                }
            } else {
                for (EqProxy proxy : proxies) {
                    proxy.reset();
                }
            }
        }
        return new EqMatches(result.isEmpty() ? null : result, context.unknowns);
    }

    /**
     * Wraps any_= functions and exhibits the same logical semantics like
     * OR-chained comparisons.
     * <p>
     * creates EqProxies for every single any array element
     * and shares pre existing proxies - so true value can be set on
     * this "parent" if a shared comparison is "true"
     */
    private static class AnyEqProxy extends EqProxy implements Iterable<EqProxy> {

        private Map<Function, EqProxy> proxies;
        @Nullable
        private ChildEqProxy delegate = null;

        private AnyEqProxy(Function compared, Map<Function, EqProxy> existingProxies) {
            super(compared);
            initProxies(existingProxies);
        }

        private void initProxies(Map<Function, EqProxy> existingProxies) {
            Symbol left = origin.arguments().getFirst();
            var signature = origin.signature();
            assert signature != null : "Expecting non-null signature while analyzing";
            Literal<?> arrayLiteral = (Literal<?>) origin.arguments().get(1);

            proxies = new HashMap<>();
            for (Literal<?> arrayElem : Literal.explodeCollection(arrayLiteral)) {
                Function f = new Function(EqOperator.SIGNATURE, Arrays.asList(left, arrayElem), Operator.RETURN_TYPE);
                EqProxy existingProxy = existingProxies.get(f);
                if (existingProxy == null) {
                    existingProxy = new ChildEqProxy(f, this);
                } else if (existingProxy instanceof ChildEqProxy childEqProxy) {
                    childEqProxy.addParent(this);
                }
                proxies.put(f, existingProxy);
            }
        }

        @Override
        @NotNull
        public Iterator<EqProxy> iterator() {
            return proxies.values().iterator();
        }

        @Override
        public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
            if (delegate != null) {
                return delegate.accept(visitor, context);
            }
            return super.accept(visitor, context);
        }

        private void setDelegate(@Nullable ChildEqProxy childEqProxy) {
            delegate = childEqProxy;
        }

        private void cleanDelegate() {
            delegate = null;
        }

        private static class ChildEqProxy extends EqProxy {

            private final List<AnyEqProxy> parentProxies = new ArrayList<>();

            private ChildEqProxy(Function origin, AnyEqProxy parent) {
                super(origin);
                this.addParent(parent);
            }

            private void addParent(AnyEqProxy parentProxy) {
                parentProxies.add(parentProxy);
            }

            @Override
            public void setTrue() {
                super.setTrue();
                for (AnyEqProxy parent : parentProxies) {
                    parent.setTrue();
                    parent.setDelegate(this);
                }
            }

            @Override
            public void reset() {
                super.reset();
                for (AnyEqProxy parent : parentProxies) {
                    parent.reset();
                    parent.cleanDelegate();
                }
            }
        }
    }

    static class EqProxy implements Symbol {

        protected Symbol current;
        protected final Function origin;

        private Function origin() {
            return origin;
        }

        EqProxy(Function origin) {
            this.origin = origin;
            this.current = origin;
        }

        public void reset() {
            this.current = origin;
        }

        public void setTrue() {
            this.current = Literal.BOOLEAN_TRUE;
        }

        @Override
        public SymbolType symbolType() {
            return current.symbolType();
        }

        @Override
        public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
            return current.accept(visitor, context);
        }

        @Override
        public DataType<?> valueType() {
            return current.valueType();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("writeTo not supported for " +
                EqProxy.class.getSimpleName());
        }

        @Override
        public String toString(Style style) {
            if (this == NULL_MARKER_PROXY) {
                return "NULL";
            }
            StringBuilder sb = new StringBuilder()
                .append("(")
                .append(origin.arguments().get(0).toString(style))
                .append("=")
                .append(origin.arguments().get(1).toString(style))
                .append(")");

            if (current != origin) {
                sb.append(" TRUE");
            }
            return sb.toString();
        }

        @Override
        public long ramBytesUsed() {
            return origin.ramBytesUsed();
        }
    }

    private static class ProxyInjectingVisitor extends SymbolVisitor<ProxyInjectingVisitor.Context, Symbol> {

        public static final ProxyInjectingVisitor INSTANCE = new ProxyInjectingVisitor();

        private ProxyInjectingVisitor() {}

        private static class Comparison {
            final HashMap<Function, EqProxy> proxies = new HashMap<>();

            public Comparison() {
                proxies.put(NULL_MARKER, NULL_MARKER_PROXY);
            }

            public EqProxy add(Function compared) {
                if (compared.name().equals(AnyEqOperator.NAME)) {
                    AnyEqProxy anyEqProxy = new AnyEqProxy(compared, proxies);
                    for (EqProxy proxiedProxy : anyEqProxy) {
                        if (!proxies.containsKey(proxiedProxy.origin())) {
                            proxies.put(proxiedProxy.origin(), proxiedProxy);
                        }
                    }
                    return anyEqProxy;
                }
                return proxies.computeIfAbsent(compared, EqProxy::new);
            }

        }

        private static class Context {

            private final LinkedHashMap<ColumnIdent, Comparison> comparisons;
            private boolean proxyBelow;
            private final Set<Symbol> unknowns = new HashSet<>();
            private boolean isUnderNotPredicate = false;
            private boolean foundPKColumnUnderNot = false;
            private boolean isUnderOrOperator = false;
            private boolean foundNonPKColumnUnderOr = false;
            private boolean foundPKColumnUnderOr = false;

            private Context(Collection<ColumnIdent> references) {
                comparisons = LinkedHashMap.newLinkedHashMap(references.size());
                for (ColumnIdent reference : references) {
                    comparisons.put(reference, new Comparison());
                }
            }

            private List<List<EqProxy>> comparisonValues() {
                List<List<EqProxy>> comps = new ArrayList<>(comparisons.size());
                for (Comparison comparison : comparisons.values()) {
                    comps.add(List.copyOf(comparison.proxies.values()));
                }
                return comps;
            }
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context ctx) {
            return symbol;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Context ctx) {
            ctx.unknowns.add(matchPredicate);
            return Literal.BOOLEAN_TRUE;
        }

        @Override
        public Symbol visitReference(Reference ref, Context ctx) {
            if (ctx.comparisons.containsKey(ref.column())) {
                if (ctx.isUnderOrOperator) {
                    ctx.foundPKColumnUnderOr = true;
                }
                if (ctx.isUnderNotPredicate) {
                    ctx.foundPKColumnUnderNot = true;
                }
            } else {
                if (ctx.isUnderOrOperator) {
                    ctx.foundNonPKColumnUnderOr = true;
                }
                ctx.unknowns.add(ref);
            }
            return super.visitReference(ref, ctx);
        }

        @Override
        public Symbol visitFunction(Function function, Context ctx) {
            String functionName = function.name();
            List<Symbol> arguments = function.arguments();
            boolean prevIsUnderNotPredicate = ctx.isUnderNotPredicate;

            if (functionName.equals(EqOperator.NAME)) {
                Symbol firstArg = arguments.get(0).accept(this, ctx);
                if (firstArg instanceof Reference ref && arguments.get(1).any(Symbol.IS_COLUMN) == false) {
                    Comparison comparison = ctx.comparisons.get(ref.column());
                    if (comparison != null) {
                        ctx.proxyBelow = true;
                        return comparison.add(function);
                    }
                }
            } else if (functionName.equals(AnyEqOperator.NAME) && arguments.get(1).symbolType().isValueSymbol()) {
                Symbol firstArg = arguments.get(0).accept(this, ctx);
                // ref = any ([1,2,3])
                if (firstArg instanceof Reference ref) {
                    Comparison comparison = ctx.comparisons.get(ref.column());
                    if (comparison != null) {
                        ctx.proxyBelow = true;
                        return comparison.add(function);
                    }
                }
            } else if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
                switch (functionName) {
                    case OrOperator.NAME -> ctx.isUnderOrOperator = true;
                    case NotPredicate.NAME -> ctx.isUnderNotPredicate = true;
                    case AndOperator.NAME -> {
                        ctx.isUnderOrOperator = false;
                        ctx.foundNonPKColumnUnderOr = false;
                        ctx.foundPKColumnUnderOr = false;
                    }
                    default -> throw new IllegalStateException("Unexpected function: " + functionName);
                }
                boolean proxyBelowPre = ctx.proxyBelow;
                boolean proxyBelowPost = proxyBelowPre;
                ArrayList<Symbol> newArgs = new ArrayList<>(arguments.size());
                for (Symbol arg : arguments) {
                    ctx.proxyBelow = proxyBelowPre;
                    newArgs.add(arg.accept(this, ctx));
                    proxyBelowPost = ctx.proxyBelow || proxyBelowPost;
                }
                if ((ctx.foundPKColumnUnderOr && ctx.foundNonPKColumnUnderOr) || ctx.foundPKColumnUnderNot) {
                    return null;
                }
                ctx.isUnderNotPredicate = prevIsUnderNotPredicate;
                ctx.proxyBelow = proxyBelowPost;
                if (!ctx.proxyBelow && function.valueType().equals(DataTypes.BOOLEAN)) {
                    return Literal.BOOLEAN_TRUE;
                }
                return new Function(function.signature(), newArgs, function.valueType());
            }
            ctx.unknowns.add(function);
            return Literal.BOOLEAN_TRUE;
        }
    }
}
