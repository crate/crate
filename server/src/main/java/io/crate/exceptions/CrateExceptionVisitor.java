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

package io.crate.exceptions;

import org.jspecify.annotations.Nullable;

public class CrateExceptionVisitor<C, R> {

    public R process(CrateException exception, @Nullable C context) {
        return exception.accept(this, context);
    }

    protected R visitCrateException(CrateException e, C context) {
        return null;
    }

    protected R visitTableScopeException(TableScopeException e, C context) {
        return visitCrateException(e, context);
    }

    protected R visitSchemaScopeException(SchemaScopeException e, C context) {
        return visitCrateException(e, context);
    }

    protected R visitUnsupportedFunctionException(UnsupportedFunctionException e, C context) {
        return visitCrateException(e, context);
    }

    protected R visitClusterScopeException(ClusterScopeException e, C context) {
        return visitCrateException(e, context);
    }

    protected R visitUnscopedException(UnscopedException e, C context) {
        return visitCrateException(e, context);
    }

    protected R visitColumnUnknownException(ColumnUnknownException e, C context) {
        return visitCrateException(e, context);
    }
}
