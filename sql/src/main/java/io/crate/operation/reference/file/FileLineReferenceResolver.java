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
import io.crate.metadata.Reference;
import io.crate.operation.collect.files.LineCollectorExpression;
import io.crate.operation.reference.ReferenceResolver;

import java.util.Map;

public class FileLineReferenceResolver implements ReferenceResolver<LineCollectorExpression<?>> {

    public static final FileLineReferenceResolver INSTANCE = new FileLineReferenceResolver();

    // need to create a new instance here so that each collector will have its own instance.
    // otherwise multiple collectors would share the same state.
    private static final Map<String, ExpressionBuilder> expressionBuilder =
            ImmutableMap.of(
                    SourceLineExpression.COLUMN_NAME, new ExpressionBuilder() {
                        @Override
                        public LineCollectorExpression<?> create() {
                            return new SourceLineExpression();
                        }
                    },
                    SourceAsMapLineExpression.COLUMN_NAME, new ExpressionBuilder() {
                        @Override
                        public LineCollectorExpression<?> create() {
                            return new SourceAsMapLineExpression();
                        }
                    });
    private FileLineReferenceResolver() {
    }

    public LineCollectorExpression<?> getImplementation(Reference refInfo) {
        ExpressionBuilder builder = expressionBuilder.get(refInfo.ident().columnIdent().name());
        if (builder != null) {
            return builder.create();
        }
        return new ColumnExtractingLineExpression(refInfo.ident().columnIdent());
    }


    interface ExpressionBuilder {
        LineCollectorExpression<?> create();
    }
}
