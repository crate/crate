/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.file;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.collect.files.LineCollectorExpression;
import io.crate.operation.reference.DocLevelReferenceResolver;

import java.util.Map;

public class FileLineReferenceResolver implements DocLevelReferenceResolver<LineCollectorExpression<?>> {

    public static final FileLineReferenceResolver INSTANCE = new FileLineReferenceResolver();

    private static final Map<String, LineCollectorExpression<?>> implementations =
            ImmutableMap.<String, LineCollectorExpression<?>>of(
                    SourceLineExpression.COLUMN_NAME, new SourceLineExpression());

    private FileLineReferenceResolver() {
    }

    public LineCollectorExpression<?> getImplementation(ReferenceInfo info) {
        LineCollectorExpression<?> expression = implementations.get(info.ident().columnIdent().name());
        if (expression != null) {
            return expression;
        }
        return new ColumnExtractingLineExpression(info.ident().columnIdent());
    }
}
