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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.operation.collect.files.LineCollectorExpression;

import java.util.Map;
import java.util.function.Supplier;

public final class FileLineReferenceResolver {

    // need to create a new instance here so that each collector will have its own instance.
    // otherwise multiple collectors would share the same state.
    private static final Map<String, Supplier<LineCollectorExpression<?>>> EXPRESSION_BUILDER =
        ImmutableMap.of(
            SourceLineExpression.COLUMN_NAME, SourceLineExpression::new,
            SourceAsMapLineExpression.COLUMN_NAME, SourceAsMapLineExpression::new);

    private FileLineReferenceResolver() {
    }

    public static LineCollectorExpression<?> getImplementation(Reference refInfo) {
        ColumnIdent columnIdent = refInfo.column();
        Supplier<LineCollectorExpression<?>> supplier = EXPRESSION_BUILDER.get(columnIdent.name());
        if (supplier == null) {
            return new ColumnExtractingLineExpression(columnIdent);
        }
        return supplier.get();
    }
}
