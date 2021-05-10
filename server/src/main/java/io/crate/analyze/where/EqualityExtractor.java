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

import com.google.common.collect.Sets;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.Operators;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
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

public class EqualityExtractor {

    private static final Function NULL_MARKER = new Function(
        Signature.scalar("null_marker", DataTypes.UNDEFINED.getTypeSignature()),
        List.of(),
        DataTypes.UNDEFINED
    );
    private static final EqProxy NULL_MARKER_PROXY = new EqProxy(NULL_MARKER);

    private EvaluatingNormalizer normalizer;

    public EqualityExtractor(EvaluatingNormalizer normalizer) {
        this.normalizer = normalizer;
    }

    public List<List<Symbol>> extractParentMatches(List<ColumnIdent> columns, Symbol symbol, @Nullable TransactionContext coordinatorTxnCtx) {
        return extractMatches(columns, symbol, false, coordinatorTxnCtx);
    }

    /**
     * @return Exact matches of the {@code columns} within {@code symbol}.
     *         Null if there are no exact matches
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
    @Nullable
    public List<List<Symbol>> extractExactMatches(List<ColumnIdent> columns,
                                                  Symbol symbol,
                                                  @Nullable TransactionContext transactionContext) {
        return extractMatches(columns, symbol, true, transactionContext);
    }

    @Nullable
    private List<List<Symbol>> extractMatches(Collection<ColumnIdent> columns,
                                              Symbol symbol,
                                              boolean exact,
                                              @Nullable TransactionContext transactionContext) {
        EqualityExtractor.ProxyInjectingVisitor.Context context =
            new EqualityExtractor.ProxyInjectingVisitor.Context(columns, exact);
        Symbol proxiedTree = symbol.accept(ProxyInjectingVisitor.INSTANCE, context);

        // bail out if we have any unknown part in the tree
        if (context.exact && context.seenUnknown) {
            return null;
        }

        List<Set<EqProxy>> comparisons = context.comparisonSet();
        Set<List<EqProxy>> cp = Sets.cartesianProduct(comparisons);

        List<List<Symbol>> result = new ArrayList<>();
        for (List<EqProxy> proxies : cp) {
            boolean anyNull = false;
            for (EqProxy proxy : proxies) {
                if (proxy != NULL_MARKER_PROXY) {
                    proxy.setTrue();
                } else {
                    anyNull = true;
                }
            }
            Symbol normalized = normalizer.normalize(proxiedTree, transactionContext);
            if (normalized == Literal.BOOLEAN_TRUE) {
                if (anyNull) {
                    return null;
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
        return result.isEmpty() ? null : result;

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
            Symbol left = origin.arguments().get(0);
            var signature = origin.signature();
            assert signature != null : "Expecting non-null signature while analyzing";
            Literal<?> arrayLiteral = (Literal<?>) origin.arguments().get(1);

            proxies = new HashMap<>();
            for (Literal<?> arrayElem : Literal.explodeCollection(arrayLiteral)) {
                Function f = new Function(EqOperator.SIGNATURE, Arrays.asList(left, arrayElem), Operator.RETURN_TYPE);
                EqProxy existingProxy = existingProxies.get(f);
                if (existingProxy == null) {
                    existingProxy = new ChildEqProxy(f, this);
                } else if (existingProxy instanceof ChildEqProxy) {
                    ((ChildEqProxy) existingProxy).addParent(this);
                }
                proxies.put(f, existingProxy);
            }
        }

        @Override
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

            private List<AnyEqProxy> parentProxies = new ArrayList<>();

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

    static class EqProxy extends Symbol {

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
        public DataType valueType() {
            return current.valueType();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("writeTo not supported for " +
                                                    EqProxy.class.getSimpleName());
        }

        public String forDisplay() {
            if (this == NULL_MARKER_PROXY) {
                return "NULL";
            }
            String s = "(" + ((Reference) origin.arguments().get(0)).column().fqn() + "=" +
                       ((Literal) origin.arguments().get(1)).value() + ")";
            if (current != origin) {
                s += " TRUE";
            }
            return s;
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
    }

    static class ProxyInjectingVisitor extends SymbolVisitor<ProxyInjectingVisitor.Context, Symbol> {

        public static final ProxyInjectingVisitor INSTANCE = new ProxyInjectingVisitor();

        private ProxyInjectingVisitor() {
        }

        static class Comparison {
            final HashMap<Function, EqProxy> proxies = new HashMap<>();

            public Comparison() {
                proxies.put(NULL_MARKER, NULL_MARKER_PROXY);
            }

            public EqProxy add(Function compared) {
                if (compared.name().equals(AnyOperators.Type.EQ.opName())) {
                    AnyEqProxy anyEqProxy = new AnyEqProxy(compared, proxies);
                    for (EqProxy proxiedProxy : anyEqProxy) {
                        if (!proxies.containsKey(proxiedProxy.origin())) {
                            proxies.put(proxiedProxy.origin(), proxiedProxy);
                        }
                    }
                    return anyEqProxy;
                }
                EqProxy proxy = proxies.get(compared);
                if (proxy == null) {
                    proxy = new EqProxy(compared);
                    proxies.put(compared, proxy);
                }
                return proxy;
            }

        }

        static class Context {

            private LinkedHashMap<ColumnIdent, Comparison> comparisons;
            private boolean proxyBelow;
            private boolean seenUnknown = false;
            private boolean ignoreUnknown = false;
            private final boolean exact;

            private Context(Collection<ColumnIdent> references, boolean exact) {
                this.exact = exact;
                comparisons = new LinkedHashMap<>(references.size());
                for (ColumnIdent reference : references) {
                    comparisons.put(reference, new Comparison());
                }
            }

            private List<Set<EqProxy>> comparisonSet() {
                List<Set<EqProxy>> comps = new ArrayList<>(comparisons.size());
                for (Comparison comparison : comparisons.values()) {
                    // TODO: probably create a view instead of a separate set
                    comps.add(new HashSet<>(comparison.proxies.values()));
                }
                return comps;
            }
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context context) {
            return symbol;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Context context) {
            context.seenUnknown = true;
            return Literal.BOOLEAN_TRUE;
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            if (!context.comparisons.containsKey(symbol.column())) {
                context.seenUnknown = true;
            }
            return super.visitReference(symbol, context);
        }

        public Symbol visitFunction(Function function, Context context) {

            String functionName = function.name();
            List<Symbol> arguments = function.arguments();
            Symbol firstArg = arguments.get(0);

            if (functionName.equals(EqOperator.NAME)) {
                firstArg = Symbols.unwrapReferenceFromCast(firstArg);
                if (firstArg instanceof Reference && SymbolVisitors.any(Symbols.IS_COLUMN, arguments.get(1)) == false) {
                    Comparison comparison = context.comparisons.get(
                        ((Reference) firstArg).column());
                    if (comparison != null) {
                        context.proxyBelow = true;
                        return comparison.add(function);
                    }
                }
            } else if (functionName.equals(AnyOperators.Type.EQ.opName()) && arguments.get(1).symbolType().isValueSymbol()) {
                // ref = any ([1,2,3])
                firstArg = Symbols.unwrapReferenceFromCast(firstArg);
                if (firstArg instanceof Reference) {
                    Reference reference = (Reference) firstArg;
                    Comparison comparison = context.comparisons.get(reference.column());
                    if (comparison != null) {
                        context.proxyBelow = true;
                        return comparison.add(function);
                    }
                }
            } else if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
                boolean proxyBelowPre = context.proxyBelow;
                boolean proxyBelowPost = proxyBelowPre;
                context.ignoreUnknown = context.ignoreUnknown || functionName.equals(AndOperator.NAME);
                ArrayList<Symbol> newArgs = new ArrayList<>(arguments.size());
                for (Symbol arg : arguments) {
                    context.proxyBelow = proxyBelowPre;
                    newArgs.add(arg.accept(this, context));
                    proxyBelowPost = context.proxyBelow || proxyBelowPost;
                }
                context.proxyBelow = proxyBelowPost;
                if (!context.proxyBelow && function.valueType().equals(DataTypes.BOOLEAN)) {
                    return Literal.BOOLEAN_TRUE;
                }
                return new Function(function.signature(), newArgs, function.valueType());
            }
            if (context.ignoreUnknown == false
                || functionName.equals(io.crate.expression.predicate.MatchPredicate.NAME)) {
                context.seenUnknown = true;
            }
            return Literal.BOOLEAN_TRUE;
        }
    }
}
