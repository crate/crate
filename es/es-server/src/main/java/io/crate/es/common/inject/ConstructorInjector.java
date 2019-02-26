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

import io.crate.es.common.inject.internal.ConstructionContext;
import io.crate.es.common.inject.internal.Errors;
import io.crate.es.common.inject.internal.ErrorsException;
import io.crate.es.common.inject.internal.InternalContext;
import io.crate.es.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * Creates instances using an injectable constructor. After construction, all injectable fields and
 * methods are injected.
 *
 * @author crazybob@google.com (Bob Lee)
 */
class ConstructorInjector<T> {

    private final Set<InjectionPoint> injectableMembers;
    private final SingleParameterInjector<?>[] parameterInjectors;
    private final ConstructionProxy<T> constructionProxy;
    private final MembersInjectorImpl<T> membersInjector;

    ConstructorInjector(Set<InjectionPoint> injectableMembers,
                        ConstructionProxy<T> constructionProxy,
                        SingleParameterInjector<?>[] parameterInjectors,
                        MembersInjectorImpl<T> membersInjector)
            throws ErrorsException {
        this.injectableMembers = injectableMembers;
        this.constructionProxy = constructionProxy;
        this.parameterInjectors = parameterInjectors;
        this.membersInjector = membersInjector;
    }

    public Set<InjectionPoint> getInjectableMembers() {
        return injectableMembers;
    }

    ConstructionProxy<T> getConstructionProxy() {
        return constructionProxy;
    }

    /**
     * Construct an instance. Returns {@code Object} instead of {@code T} because
     * it may return a proxy.
     */
    Object construct(Errors errors, InternalContext context, Class<?> expectedType)
            throws ErrorsException {
        ConstructionContext<T> constructionContext = context.getConstructionContext(this);

        // We have a circular reference between constructors. Return a proxy.
        if (constructionContext.isConstructing()) {
            // TODO (crazybob): if we can't proxy this object, can we proxy the other object?
            return constructionContext.createProxy(errors, expectedType);
        }

        // If we're re-entering this factory while injecting fields or methods,
        // return the same instance. This prevents infinite loops.
        T t = constructionContext.getCurrentReference();
        if (t != null) {
            return t;
        }

        try {
            // First time through...
            constructionContext.startConstruction();
            try {
                Object[] parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
                t = constructionProxy.newInstance(parameters);
                constructionContext.setProxyDelegates(t);
            } finally {
                constructionContext.finishConstruction();
            }

            // Store reference. If an injector re-enters this factory, they'll get the same reference.
            constructionContext.setCurrentReference(t);

            membersInjector.injectMembers(t, errors, context);
            membersInjector.notifyListeners(t, errors);

            return t;
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null
                    ? userException.getCause()
                    : userException;
            throw errors.withSource(constructionProxy.getInjectionPoint())
                    .errorInjectingConstructor(cause).toException();
        } finally {
            constructionContext.removeCurrentReference();
        }
    }
}
