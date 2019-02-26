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

package io.crate.es.common.inject;

import io.crate.es.common.inject.internal.Errors;
import io.crate.es.common.inject.internal.ErrorsException;
import io.crate.es.common.inject.internal.InternalContext;
import io.crate.es.common.inject.internal.InternalFactory;
import io.crate.es.common.inject.internal.SourceProvider;
import io.crate.es.common.inject.spi.Dependency;

import java.util.Objects;

/**
 * @author crazybob@google.com (Bob Lee)
 */
class InternalFactoryToProviderAdapter<T> implements InternalFactory<T> {

    private final Initializable<Provider<? extends T>> initializable;
    private final Object source;

    InternalFactoryToProviderAdapter(Initializable<Provider<? extends T>> initializable) {
        this(initializable, SourceProvider.UNKNOWN_SOURCE);
    }

    InternalFactoryToProviderAdapter(
            Initializable<Provider<? extends T>> initializable, Object source) {
        this.initializable = Objects.requireNonNull(initializable, "provider");
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public T get(Errors errors, InternalContext context, Dependency<?> dependency)
            throws ErrorsException {
        try {
            return errors.checkForNull(initializable.get(errors).get(), source, dependency);
        } catch (RuntimeException userException) {
            throw errors.withSource(source).errorInProvider(userException).toException();
        }
    }

    @Override
    public String toString() {
        return initializable.toString();
    }
}
