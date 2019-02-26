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
package io.crate.es.test;

import io.crate.es.Version;
import io.crate.es.common.io.stream.Streamable;
import io.crate.es.common.io.stream.Writeable;

import java.io.IOException;

public abstract class AbstractStreamableTestCase<T extends Streamable> extends AbstractWireTestCase<T> {

    @Override
    protected final T copyInstance(T instance, Version version) throws IOException {
        return copyStreamable(instance, getNamedWriteableRegistry(), this::createBlankInstance, version);
    }

    @Override
    protected final Writeable.Reader<T> instanceReader() {
        return Streamable.newWriteableReader(this::createBlankInstance);
    }

    /**
     * Creates an empty instance to use when deserialising the
     * {@link Streamable}. This usually returns an instance created using the
     * zer-arg constructor
     */
    protected abstract T createBlankInstance();
}
