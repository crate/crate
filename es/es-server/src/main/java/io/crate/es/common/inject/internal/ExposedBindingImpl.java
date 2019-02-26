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
import io.crate.es.common.inject.spi.BindingTargetVisitor;
import io.crate.es.common.inject.spi.Dependency;
import io.crate.es.common.inject.spi.ExposedBinding;
import io.crate.es.common.inject.spi.PrivateElements;

import java.util.Set;

import static java.util.Collections.singleton;

public class ExposedBindingImpl<T> extends BindingImpl<T> implements ExposedBinding<T> {

    private final PrivateElements privateElements;

    public ExposedBindingImpl(Injector injector, Object source, Key<T> key,
                              InternalFactory<T> factory, PrivateElements privateElements) {
        super(injector, key, source, factory, Scoping.UNSCOPED);
        this.privateElements = privateElements;
    }

    public ExposedBindingImpl(Object source, Key<T> key, Scoping scoping,
                              PrivateElements privateElements) {
        super(source, key, scoping);
        this.privateElements = privateElements;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return singleton(Dependency.get(Key.get(Injector.class)));
    }

    @Override
    public PrivateElements getPrivateElements() {
        return privateElements;
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new ExposedBindingImpl<>(getSource(), getKey(), scoping, privateElements);
    }

    @Override
    public ExposedBindingImpl<T> withKey(Key<T> key) {
        return new ExposedBindingImpl<>(getSource(), key, getScoping(), privateElements);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ExposedBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("privateElements", privateElements)
                .toString();
    }

    @Override
    public void applyTo(Binder binder) {
        throw new UnsupportedOperationException("This element represents a synthetic binding.");
    }
}
