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

import io.crate.es.common.inject.binder.AnnotatedElementBuilder;

/**
 * Returns a binder whose configuration information is hidden from its environment by default. See
 * {@link PrivateModule PrivateModule} for details.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface PrivateBinder extends Binder {

    /**
     * Makes the binding for {@code key} available to the enclosing environment
     */
    void expose(Key<?> key);

    /**
     * Makes a binding for {@code type} available to the enclosing environment. Use {@link
     * AnnotatedElementBuilder#annotatedWith(Class) annotatedWith()} to expose {@code type} with a
     * binding annotation.
     */
    AnnotatedElementBuilder expose(Class<?> type);

    /**
     * Makes a binding for {@code type} available to the enclosing environment. Use {@link
     * AnnotatedElementBuilder#annotatedWith(Class) annotatedWith()} to expose {@code type} with a
     * binding annotation.
     */
    AnnotatedElementBuilder expose(TypeLiteral<?> type);

    @Override
    PrivateBinder withSource(Object source);

    @Override
    PrivateBinder skipSources(Class... classesToSkip);
}
