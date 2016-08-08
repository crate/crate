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

package io.crate.operation.reference.doc.blob;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Reference;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.operation.collect.blobs.BlobCollectorExpression;
import io.crate.operation.reference.ReferenceResolver;

import java.util.Map;

public class BlobReferenceResolver implements ReferenceResolver<BlobCollectorExpression<?>> {

    public static final BlobReferenceResolver INSTANCE = new BlobReferenceResolver();

    private static final Map<String, ExpressionBuilder> expressionBuilder =
            ImmutableMap.of(
                    BlobDigestExpression.COLUMN_NAME, new ExpressionBuilder() {
                        @Override
                        public BlobCollectorExpression<?> create() {
                            return new BlobDigestExpression();
                        }
                    },
                    BlobLastModifiedExpression.COLUMN_NAME, new ExpressionBuilder() {
                        @Override
                        public BlobCollectorExpression<?> create() {
                            return new BlobLastModifiedExpression();
                        }
                    }
            );

    private BlobReferenceResolver() {}

    @Override
    public BlobCollectorExpression<?> getImplementation(Reference refInfo) {
        assert (BlobSchemaInfo.NAME.equals(refInfo.ident().tableIdent().schema()));
        ExpressionBuilder builder = expressionBuilder.get(refInfo.ident().columnIdent().name());
        if (builder != null) {
            return builder.create();
        }
        return null;
    }

    interface ExpressionBuilder {
        BlobCollectorExpression<?> create();
    }
}
