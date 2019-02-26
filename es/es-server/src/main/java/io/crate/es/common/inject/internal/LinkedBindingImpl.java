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
import io.crate.es.common.inject.spi.LinkedKeyBinding;

public final class LinkedBindingImpl<T> extends BindingImpl<T> implements LinkedKeyBinding<T> {

    final Key<? extends T> targetKey;

    public LinkedBindingImpl(Injector injector, Key<T> key, Object source,
                             InternalFactory<? extends T> internalFactory, Scoping scoping,
                             Key<? extends T> targetKey) {
        super(injector, key, source, internalFactory, scoping);
        this.targetKey = targetKey;
    }

    public LinkedBindingImpl(Object source, Key<T> key, Scoping scoping, Key<? extends T> targetKey) {
        super(source, key, scoping);
        this.targetKey = targetKey;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Key<? extends T> getLinkedKey() {
        return targetKey;
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new LinkedBindingImpl<>(getSource(), getKey(), scoping, targetKey);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new LinkedBindingImpl<>(getSource(), key, getScoping(), targetKey);
    }

    @Override
    public void applyTo(Binder binder) {
        getScoping().applyTo(binder.withSource(getSource()).bind(getKey()).to(getLinkedKey()));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(LinkedKeyBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("scope", getScoping())
                .add("target", targetKey)
                .toString();
    }
}
