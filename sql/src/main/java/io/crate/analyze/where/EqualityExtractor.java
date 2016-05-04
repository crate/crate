/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.*;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class EqualityExtractor {

    public static final Function NULL_MARKER = new Function(
            new FunctionInfo(
                    new FunctionIdent("null_marker", ImmutableList.<DataType>of()),
                    DataTypes.UNDEFINED), ImmutableList.<Symbol>of());
    public static final EqProxy NULL_MARKER_PROXY = new EqProxy(NULL_MARKER);

    private EvaluatingNormalizer normalizer;

    public EqualityExtractor(EvaluatingNormalizer normalizer) {
        this.normalizer = normalizer;
    }

    public List<List<Symbol>> extractParentMatches(List<ColumnIdent> columns, Symbol symbol){
        return extractMatches(columns, symbol, false);
    }
    public List<List<Symbol>> extractExactMatches(List<ColumnIdent> columns, Symbol symbol) {
        return extractMatches(columns, symbol, true);
    }

    private List<List<Symbol>> extractMatches(Collection<ColumnIdent> columns, Symbol symbol, boolean exact){
        EqualityExtractor.ProxyInjectingVisitor.Context context =
                new EqualityExtractor.ProxyInjectingVisitor.Context(columns, exact);
        Symbol proxiedTree = ProxyInjectingVisitor.INSTANCE.process(symbol, context);

        // bail out if we have any unknown part in the tree
        if (context.exact && context.seenUnknown){
            return null;
        }

        List<Set<EqProxy>> comparisons = context.comparisonSet();
        Set<List<EqProxy>> cp = Sets.cartesianProduct(comparisons);

        List<List<Symbol>> result = new ArrayList<>();
        for (List<EqProxy> proxies : cp) {
            boolean anyNull = false;
            for (EqProxy proxy : proxies) {
                if (proxy != NULL_MARKER_PROXY){
                    proxy.setTrue();
                } else {
                    anyNull = true;
                }
            }
            Symbol normalized = normalizer.normalize(proxiedTree);
            if (normalized == Literal.BOOLEAN_TRUE){
                if (anyNull){
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
     *
     * creates EqProxies for every single any array element
     * and shares pre existing proxies - so true value can be set on
     * this "parent" if a shared comparison is "true"
     */
    static class AnyEqProxy extends EqProxy implements Iterable<EqProxy> {

        private Map<Function, EqProxy> proxies;
        @Nullable
        private ChildEqProxy delegate = null;

        public AnyEqProxy(Function compared, Map<Function, EqProxy> existingProxies) {
            super(compared);
            initProxies(existingProxies);
        }

        private void initProxies(Map<Function, EqProxy> existingProxies) {
            Symbol left = origin.arguments().get(0);
            DataType leftType = origin.info().ident().argumentTypes().get(0);
            DataType rightType = ((CollectionType)origin.info().ident().argumentTypes().get(1)).innerType();
            FunctionInfo eqInfo = new FunctionInfo(
                    new FunctionIdent(
                            EqOperator.NAME,
                            ImmutableList.of(leftType, rightType)
                    ),
                    DataTypes.BOOLEAN
            );
            Literal arrayLiteral = (Literal)origin.arguments().get(1);
            proxies = new HashMap<>();
            for (Literal arrayElem : Literal.explodeCollection(arrayLiteral)) {
                Function f = new Function(eqInfo, Arrays.asList(left, arrayElem));
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

        public void setDelegate(@Nullable ChildEqProxy childEqProxy) {
            delegate = childEqProxy;
        }

        public void cleanDelegate() {
            delegate = null;
        }

        static class ChildEqProxy extends EqProxy {

            private List<AnyEqProxy> parentProxies = new ArrayList<>();

            ChildEqProxy(Function origin, AnyEqProxy parent) {
                super(origin);
                this.addParent(parent);
            }

            public void addParent(AnyEqProxy parentProxy) {
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

        public Function origin() {
            return origin;
        }

        EqProxy(Function origin) {
            this.origin = origin;
            this.current = origin;
        }

        public void reset(){
            this.current = origin;
        }

        public void setTrue(){
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
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        public String forDisplay() {
            if (this == NULL_MARKER_PROXY) {
                return "NULL";
            }
            String s = "(" + ((Reference)origin.arguments().get(0)).ident().columnIdent().fqn() + "=" +
                    ((Literal) origin.arguments().get(1)).value() +  ")";
            if (current!=origin){
                s += " TRUE";
            }
            return s;
        }

        @Override
        public String toString() {
            return "EqProxy{" + forDisplay() + "}";
        }
    }

    static class ProxyInjectingVisitor extends SymbolVisitor<ProxyInjectingVisitor.Context, Symbol> {

        public static final ProxyInjectingVisitor INSTANCE = new ProxyInjectingVisitor();

        private ProxyInjectingVisitor() {
        }

        static class Comparison{
            final HashMap<Function, EqProxy> proxies = new HashMap<>();

            public Comparison() {
                proxies.put(NULL_MARKER, NULL_MARKER_PROXY);
            }

            public EqProxy add(Function compared){
                if (compared.info().ident().name().equals(AnyEqOperator.NAME)) {
                    AnyEqProxy anyEqProxy = new AnyEqProxy(compared, proxies);
                    for (EqProxy proxiedProxy : anyEqProxy) {
                        if (!proxies.containsKey(proxiedProxy.origin())) {
                            proxies.put(proxiedProxy.origin(), proxiedProxy);
                        }
                    }
                    return anyEqProxy;
                }
                EqProxy proxy = proxies.get(compared);
                if (proxy==null){
                    proxy = new EqProxy(compared);
                    proxies.put(compared, proxy);
                }
                return proxy;
            }

        }

        static class Context {
            LinkedHashMap<ColumnIdent, Comparison> comparisons;
            public boolean proxyBelow;
            public boolean seenUnknown = false;
            private final boolean exact;

            public Context(Collection<ColumnIdent> references, boolean exact) {
                this.exact = exact;
                comparisons = new LinkedHashMap<>(references.size());
                for (ColumnIdent reference : references) {
                    comparisons.put(reference, new Comparison());
                }
            }

            public List<Set<EqProxy>> comparisonSet(){
                List<Set<EqProxy>> comps = new ArrayList<>(comparisons.size());
                for (Comparison comparison : comparisons.values()) {
                    // TODO: probably create a view instead of a seperate set
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
            if (!context.comparisons.containsKey(symbol.ident().columnIdent())){
                context.seenUnknown = true;
            }
            return super.visitReference(symbol, context);
        }

        public Symbol visitFunction(Function function, Context context) {

            String functionName = function.info().ident().name();
            if (functionName.equals(EqOperator.NAME)) {
                if (function.arguments().get(0) instanceof Reference) {
                    Comparison comparison = context.comparisons.get(
                            ((Reference) function.arguments().get(0)).ident().columnIdent());
                    if (comparison != null) {
                        context.proxyBelow = true;
                        EqProxy x = comparison.add(function);
                        return x;
                    }
                }
            } else if (functionName.equals(AnyEqOperator.NAME) && function.arguments().get(1).symbolType().isValueSymbol()) {
                // ref = any ([1,2,3])
                if (function.arguments().get(0) instanceof Reference) {
                    Reference reference = (Reference) function.arguments().get(0);
                    Comparison comparison = context.comparisons.get(
                            reference.ident().columnIdent());
                    if (comparison != null) {
                        context.proxyBelow = true;
                        EqProxy x = comparison.add(function);
                        return x;
                    }
                }
            }
            boolean proxyBelowPre = context.proxyBelow;
            boolean proxyBelowPost = proxyBelowPre;
            ArrayList<Symbol> args = new ArrayList<>(function.arguments().size());
            for (Symbol arg : function.arguments()) {
                context.proxyBelow = proxyBelowPre;
                args.add(process(arg, context));
                proxyBelowPost = context.proxyBelow || proxyBelowPost;
            }
            context.proxyBelow = proxyBelowPost;
            if (!context.proxyBelow && function.valueType().equals(DataTypes.BOOLEAN)){
                return Literal.BOOLEAN_TRUE;
            }
            return new Function(function.info(), args);
        }
    }

}
