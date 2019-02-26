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
import io.crate.es.common.inject.spi.Element;
import io.crate.es.common.inject.spi.MembersInjectorLookup;
import io.crate.es.common.inject.spi.ProviderLookup;

import java.util.ArrayList;
import java.util.List;

/**
 * Returns providers and members injectors that haven't yet been initialized. As a part of injector
 * creation it's necessary to {@link #initialize initialize} these lookups.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class DeferredLookups implements Lookups {
    private final InjectorImpl injector;
    private final List<Element> lookups = new ArrayList<>();

    DeferredLookups(InjectorImpl injector) {
        this.injector = injector;
    }

    /**
     * Initialize the specified lookups, either immediately or when the injector is created.
     */
    public void initialize(Errors errors) {
        injector.lookups = injector;
        new LookupProcessor(errors).process(injector, lookups);
    }

    @Override
    public <T> Provider<T> getProvider(Key<T> key) {
        ProviderLookup<T> lookup = new ProviderLookup<>(key, key);
        lookups.add(lookup);
        return lookup.getProvider();
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> type) {
        MembersInjectorLookup<T> lookup = new MembersInjectorLookup<>(type, type);
        lookups.add(lookup);
        return lookup.getMembersInjector();
    }
}
