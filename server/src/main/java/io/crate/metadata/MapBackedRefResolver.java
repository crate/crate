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

package io.crate.metadata;

import java.util.Map;

import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;

public final class MapBackedRefResolver implements ReferenceResolver<NestableInput<?>> {

    private Map<ColumnIdent, NestableInput<?>> implByColumn;

    public MapBackedRefResolver(Map<ColumnIdent, NestableInput<?>> implByColumn) {
        this.implByColumn = implByColumn;
    }

    @Override
    public NestableInput<?> getImplementation(Reference ref) {
        return lookupMapWithChildTraversal(implByColumn, ref.column());
    }

    static NestableInput<?> lookupMapWithChildTraversal(Map<ColumnIdent, NestableInput<?>> implByColumn, ColumnIdent column) {
        if (column.isRoot()) {
            return implByColumn.get(column);
        }
        NestableInput<?> rootImpl = implByColumn.get(column.getRoot());
        if (rootImpl == null) {
            return implByColumn.get(column);
        }
        return NestableInput.getChildByPath(rootImpl, column.path());
    }
}
