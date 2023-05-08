/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractAssert;

import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;

public final class ProjectionAssert extends AbstractAssert<ProjectionAssert, Projection> {

    public ProjectionAssert(Projection actual) {
        super(actual, ProjectionAssert.class);
    }

    public ProjectionAssert isLimitAndOffset(int expectedLimit, int expectedOffset) {
        isNotNull();
        isExactlyInstanceOf(LimitAndOffsetProjection.class);
        assertThat(((LimitAndOffsetProjection) actual).limit()).isEqualTo(expectedLimit);
        assertThat(((LimitAndOffsetProjection) actual).offset()).isEqualTo(expectedOffset);
        return this;
    }

    public SymbolAssert isFilterWithQuery() {
        isNotNull();
        isExactlyInstanceOf(FilterProjection.class);
        Symbol query = ((FilterProjection) actual).query();
        return new SymbolAssert(query);
    }
}
