/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.job;

import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import org.elasticsearch.common.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class RowContextDocLevelReferenceResolver implements DocLevelReferenceResolver<RowContextCollectorExpression<?, ?>> {

    public static final RowContextDocLevelReferenceResolver INSTANCE = new RowContextDocLevelReferenceResolver();

    private static final Map<ReferenceIdent, RowContextCollectorExpression<?, ?>> implementations = new HashMap<>();

    static {
        for (RowContextCollectorExpression<?, ?> implementation : SysJobExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
    }

    /**
     * This is a singleton Use the static INSTANCE attribute
     */
    private RowContextDocLevelReferenceResolver() {

    }

    @Override
    public RowContextCollectorExpression<?, ?> getImplementation(ReferenceInfo info) {
        return implementations.get(info.ident());
    }

}
