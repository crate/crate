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

package io.crate.expression.reference;

import io.crate.common.collections.Maps;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

import java.util.List;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

/**
 * ReferenceResolver implementation which can be used to retrieve {@link CollectExpression}s to extract values from {@link Doc}
 *
 */
public final class DocRefResolver implements ReferenceResolver<CollectExpression<Doc, ?>> {

    private final List<ColumnIdent> partitionedByColumns;

    public DocRefResolver(List<ColumnIdent> partitionedByColumns) {
        this.partitionedByColumns = partitionedByColumns;
    }

    @Override
    public CollectExpression<Doc, ?> getImplementation(Reference ref) {
        ColumnIdent columnIdent = ref.column();
        String fqn = columnIdent.fqn();
        switch (fqn) {
            case DocSysColumns.Names.VERSION:
                return forFunction(Doc::getVersion);

            case DocSysColumns.Names.SEQ_NO:
                return forFunction(Doc::getSeqNo);

            case DocSysColumns.Names.PRIMARY_TERM:
                return forFunction(Doc::getPrimaryTerm);

            case DocSysColumns.Names.ID:
                return NestableCollectExpression.forFunction(Doc::getId);

            case DocSysColumns.Names.DOCID:
                return forFunction(Doc::docId);

            case DocSysColumns.Names.RAW:
                return forFunction(Doc::getRaw);

            case DocSysColumns.Names.DOC:
                return forFunction(Doc::getSource);

            default:
                for (int i = 0; i < partitionedByColumns.size(); i++) {
                    var pColumn = partitionedByColumns.get(i);
                    if (pColumn.equals(columnIdent)) {
                        final int idx = i;
                        return forFunction(
                            getResp -> ref.valueType().implicitCast(
                                PartitionName.fromIndexOrTemplate(getResp.getIndex()).values().get(idx))
                        );
                    } else if (pColumn.isChildOf(columnIdent)) {
                        final int idx = i;
                        return forFunction(response -> {
                            if (response == null) {
                                return null;
                            }
                            var partitionName = PartitionName.fromIndexOrTemplate(response.getIndex());
                            var partitionValue = partitionName.values().get(idx);
                            var source = response.getSource();
                            Maps.mergeInto(source, pColumn.name(), pColumn.path(), partitionValue);
                            return ref.valueType().implicitCast(ValueExtractors.fromMap(source, columnIdent));
                        });
                    }
                }

                return forFunction(response -> {
                    if (response == null) {
                        return null;
                    }
                    return ref.valueType().implicitCast(ValueExtractors.fromMap(response.getSource(), ref.column()));
                });
        }
    }
}
