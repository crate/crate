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

package io.crate.expression.reference;

import java.util.function.Consumer;

import io.crate.data.Input;
import io.crate.metadata.ScopedRef;

public class GatheringRefResolver<E extends Input<?>> implements ReferenceResolver<E> {

    private final Consumer<E> consumer;
    private final ReferenceResolver<E> delegate;

    public GatheringRefResolver(Consumer<E> consumer, ReferenceResolver<E> delegate) {
        this.consumer = consumer;
        this.delegate = delegate;
    }

    @Override
    public E getImplementation(ScopedRef ref) {
        E implementation = delegate.getImplementation(ref);
        if (implementation == null) {
            return null;
        }
        consumer.accept(implementation);
        return implementation;
    }
}
