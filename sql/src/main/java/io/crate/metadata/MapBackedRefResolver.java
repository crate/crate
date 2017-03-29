/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata;

import io.crate.operation.reference.ReferenceResolver;

import java.util.Map;

public final class MapBackedRefResolver implements ReferenceResolver<ReferenceImplementation<?>> {

    private final Map<ReferenceIdent, ReferenceImplementation> implByIdent;

    public MapBackedRefResolver(Map<ReferenceIdent, ReferenceImplementation> implByIdent) {
        this.implByIdent = implByIdent;
    }

    @Override
    public ReferenceImplementation getImplementation(Reference ref) {
        return lookupMapWithChildTraversal(implByIdent, ref.ident());
    }

    static ReferenceImplementation lookupMapWithChildTraversal(Map<ReferenceIdent, ReferenceImplementation> implByIdent,
                                                               ReferenceIdent ident) {
        if (ident.isColumn()) {
            return implByIdent.get(ident);
        }
        ReferenceImplementation<?> impl = implByIdent.get(ident.columnReferenceIdent());
        if (impl == null) {
            return null;
        }
        return ReferenceImplementation.findInChildImplementations(impl, ident.columnIdent().path());
    }
}
