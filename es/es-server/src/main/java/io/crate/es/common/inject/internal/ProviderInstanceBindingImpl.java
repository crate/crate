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

package io.crate.es.common.inject.internal;

import io.crate.es.common.inject.Binder;
import io.crate.es.common.inject.Injector;
import io.crate.es.common.inject.Key;
import io.crate.es.common.inject.Provider;
import io.crate.es.common.inject.spi.BindingTargetVisitor;
import io.crate.es.common.inject.spi.Dependency;
import io.crate.es.common.inject.spi.HasDependencies;
import io.crate.es.common.inject.spi.InjectionPoint;
import io.crate.es.common.inject.spi.ProviderInstanceBinding;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public final class ProviderInstanceBindingImpl<T> extends BindingImpl<T>
        implements ProviderInstanceBinding<T> {

    final Provider<? extends T> providerInstance;
    final Set<InjectionPoint> injectionPoints;

    public ProviderInstanceBindingImpl(Injector injector, Key<T> key,
                                       Object source, InternalFactory<? extends T> internalFactory, Scoping scoping,
                                       Provider<? extends T> providerInstance,
                                       Set<InjectionPoint> injectionPoints) {
        super(injector, key, source, internalFactory, scoping);
        this.providerInstance = providerInstance;
        this.injectionPoints = injectionPoints;
    }

    public ProviderInstanceBindingImpl(Object source, Key<T> key, Scoping scoping,
                                       Set<InjectionPoint> injectionPoints, Provider<? extends T> providerInstance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.providerInstance = providerInstance;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Provider<? extends T> getProviderInstance() {
        return providerInstance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return providerInstance instanceof HasDependencies
                ? unmodifiableSet(new HashSet<>((((HasDependencies) providerInstance).getDependencies())))
                : Dependency.forInjectionPoints(injectionPoints);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new ProviderInstanceBindingImpl<>(
                getSource(), getKey(), scoping, injectionPoints, providerInstance);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new ProviderInstanceBindingImpl<>(
                getSource(), key, getScoping(), injectionPoints, providerInstance);
    }

    @Override
    public void applyTo(Binder binder) {
        getScoping().applyTo(
                binder.withSource(getSource()).bind(getKey()).toProvider(getProviderInstance()));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ProviderInstanceBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("scope", getScoping())
                .add("provider", providerInstance)
                .toString();
    }
}
