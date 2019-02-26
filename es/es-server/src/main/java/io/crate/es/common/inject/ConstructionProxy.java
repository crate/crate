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

import io.crate.es.common.inject.spi.InjectionPoint;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Proxies calls to a {@link java.lang.reflect.Constructor} for a class
 * {@code T}.
 *
 * @author crazybob@google.com (Bob Lee)
 */
interface ConstructionProxy<T> {

    /**
     * Constructs an instance of {@code T} for the given arguments.
     */
    T newInstance(Object... arguments) throws InvocationTargetException;

    /**
     * Returns the injection point for this constructor.
     */
    InjectionPoint getInjectionPoint();

    /**
     * Returns the injected constructor. If the injected constructor is synthetic (such as generated
     * code for method interception), the natural constructor is returned.
     */
    Constructor<T> getConstructor();
}
