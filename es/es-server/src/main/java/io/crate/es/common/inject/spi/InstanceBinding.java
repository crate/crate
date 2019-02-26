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

import java.util.Set;

/**
 * A binding to a single instance. The same instance is returned for every injection.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface InstanceBinding<T> extends Binding<T>, HasDependencies {

    /**
     * Returns the user-supplied instance.
     */
    T getInstance();

    /**
     * Returns the field and method injection points of the instance, injected at injector-creation
     * time only.
     *
     * @return a possibly empty set
     */
    Set<InjectionPoint> getInjectionPoints();

}
