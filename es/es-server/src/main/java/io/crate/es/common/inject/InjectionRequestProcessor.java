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
import io.crate.es.common.inject.spi.InjectionPoint;
import io.crate.es.common.inject.spi.InjectionRequest;
import io.crate.es.common.inject.spi.StaticInjectionRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Handles {@link Binder#requestInjection} and {@link Binder#requestStaticInjection} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * @author mikeward@google.com (Mike Ward)
 */
class InjectionRequestProcessor extends AbstractProcessor {

    private final List<StaticInjection> staticInjections = new ArrayList<>();
    private final Initializer initializer;

    InjectionRequestProcessor(Errors errors, Initializer initializer) {
        super(errors);
        this.initializer = initializer;
    }

    @Override
    public Boolean visit(StaticInjectionRequest request) {
        staticInjections.add(new StaticInjection(injector, request));
        return true;
    }

    @Override
    public Boolean visit(InjectionRequest request) {
        Set<InjectionPoint> injectionPoints;
        try {
            injectionPoints = request.getInjectionPoints();
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            injectionPoints = e.getPartialValue();
        }

        initializer.requestInjection(
                injector, request.getInstance(), request.getSource(), injectionPoints);
        return true;
    }

    public void validate() {
        for (StaticInjection staticInjection : staticInjections) {
            staticInjection.validate();
        }
    }

    public void injectMembers() {
        for (StaticInjection staticInjection : staticInjections) {
            staticInjection.injectMembers();
        }
    }

    /**
     * A requested static injection.
     */
    private class StaticInjection {
        final InjectorImpl injector;
        final Object source;
        final StaticInjectionRequest request;
        List<SingleMemberInjector> memberInjectors;

        StaticInjection(InjectorImpl injector, StaticInjectionRequest request) {
            this.injector = injector;
            this.source = request.getSource();
            this.request = request;
        }

        void validate() {
            Errors errorsForMember = errors.withSource(source);
            Set<InjectionPoint> injectionPoints;
            try {
                injectionPoints = request.getInjectionPoints();
            } catch (ConfigurationException e) {
                errors.merge(e.getErrorMessages());
                injectionPoints = e.getPartialValue();
            }
            memberInjectors = injector.membersInjectorStore.getInjectors(
                    injectionPoints, errorsForMember);
        }

        void injectMembers() {
            try {
                injector.callInContext(new ContextualCallable<Void>() {
                    @Override
                    public Void call(InternalContext context) {
                        for (SingleMemberInjector injector : memberInjectors) {
                            injector.inject(errors, context, null);
                        }
                        return null;
                    }
                });
            } catch (ErrorsException e) {
                throw new AssertionError();
            }
        }
    }
}
