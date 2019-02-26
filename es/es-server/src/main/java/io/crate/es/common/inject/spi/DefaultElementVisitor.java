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

package io.crate.es.common.inject.spi;

import io.crate.es.common.inject.Binding;

/**
 * No-op visitor for subclassing. All interface methods simply delegate to
 * {@link #visitOther(Element)}, returning its result.
 *
 * @param <V> any type to be returned by the visit method. Use {@link Void} with
 *            {@code return null} if no return type is needed.
 * @author sberlin@gmail.com (Sam Berlin)
 * @since 2.0
 */
public abstract class DefaultElementVisitor<V> implements ElementVisitor<V> {

    /**
     * Default visit implementation. Returns {@code null}.
     */
    protected V visitOther(Element element) {
        return null;
    }

    @Override
    public V visit(Message message) {
        return visitOther(message);
    }

    @Override
    public <T> V visit(Binding<T> binding) {
        return visitOther(binding);
    }

    @Override
    public V visit(ScopeBinding scopeBinding) {
        return visitOther(scopeBinding);
    }

    @Override
    public V visit(TypeConverterBinding typeConverterBinding) {
        return visitOther(typeConverterBinding);
    }

    @Override
    public <T> V visit(ProviderLookup<T> providerLookup) {
        return visitOther(providerLookup);
    }

    @Override
    public V visit(InjectionRequest injectionRequest) {
        return visitOther(injectionRequest);
    }

    @Override
    public V visit(StaticInjectionRequest staticInjectionRequest) {
        return visitOther(staticInjectionRequest);
    }

    @Override
    public V visit(PrivateElements privateElements) {
        return visitOther(privateElements);
    }

    @Override
    public <T> V visit(MembersInjectorLookup<T> lookup) {
        return visitOther(lookup);
    }

    @Override
    public V visit(TypeListenerBinding binding) {
        return visitOther(binding);
    }
}
