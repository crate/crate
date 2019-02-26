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
import io.crate.es.common.inject.spi.InstanceBinding;
import io.crate.es.common.inject.util.Providers;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class InstanceBindingImpl<T> extends BindingImpl<T> implements InstanceBinding<T> {

    final T instance;
    final Provider<T> provider;
    final Set<InjectionPoint> injectionPoints;

    public InstanceBindingImpl(Injector injector, Key<T> key, Object source,
                               InternalFactory<? extends T> internalFactory, Set<InjectionPoint> injectionPoints,
                               T instance) {
        super(injector, key, source, internalFactory, Scoping.UNSCOPED);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    public InstanceBindingImpl(Object source, Key<T> key, Scoping scoping,
                               Set<InjectionPoint> injectionPoints, T instance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    @Override
    public Provider<T> getProvider() {
        return this.provider;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public T getInstance() {
        return instance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return instance instanceof HasDependencies
                ? unmodifiableSet(new HashSet<>((((HasDependencies) instance).getDependencies())))
                : Dependency.forInjectionPoints(injectionPoints);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new InstanceBindingImpl<>(getSource(), getKey(), scoping, injectionPoints, instance);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new InstanceBindingImpl<>(getSource(), key, getScoping(), injectionPoints, instance);
    }

    @Override
    public void applyTo(Binder binder) {
        // instance bindings aren't scoped
        binder.withSource(getSource()).bind(getKey()).toInstance(instance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(InstanceBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("instance", instance)
                .toString();
    }
}
