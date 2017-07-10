/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.task.elasticsearch;

import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.reference.ReferenceResolver;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.Map;
import java.util.function.Consumer;

/**
 * ReferenceResolver implementation which can be used to retrieve {@link CollectExpression}s to extract values from
 * {@link GetResponse}s
 */
public class GetResponseRefResolver implements ReferenceResolver<CollectExpression<GetResponse, ?>> {

    private final Consumer<ColumnIdent> columnConsumer;
    private final DocTableInfo docTableInfo;
    private final Map<String, DocKeys.DocKey> ids2Keys;

    GetResponseRefResolver(Consumer<ColumnIdent> columnConsumer,
                           DocTableInfo docTableInfo,
                           Map<String, DocKeys.DocKey> ids2Keys) {
        this.columnConsumer = columnConsumer;
        this.docTableInfo = docTableInfo;
        this.ids2Keys = ids2Keys;
    }

    @Override
    public CollectExpression<GetResponse, ?> getImplementation(Reference ref) {
        ColumnIdent columnIdent = ref.ident().columnIdent();
        columnConsumer.accept(columnIdent);
        String fqn = columnIdent.fqn();
        switch (fqn) {
            case DocSysColumns.Names.VERSION:
                return RowContextCollectorExpression.forFunction(GetResponse::getVersion);

            case DocSysColumns.Names.ID:
                return RowContextCollectorExpression.objToBytesRef(GetResponse::getId);

            case DocSysColumns.Names.RAW:
                return RowContextCollectorExpression.forFunction(r -> r.getSourceAsBytesRef().toBytesRef());

            case DocSysColumns.Names.DOC:
                return RowContextCollectorExpression.forFunction(GetResponse::getSource);

        }
        if (docTableInfo.isPartitioned() && docTableInfo.partitionedBy().contains(columnIdent)) {
            int pkPos = docTableInfo.primaryKey().indexOf(columnIdent);
            if (pkPos >= 0) {
                return RowContextCollectorExpression.forFunction(
                    response -> ValueSymbolVisitor.VALUE.process(ids2Keys.get(response.getId()).values().get(pkPos)));
            }
        }

        return RowContextCollectorExpression.forFunction(response -> {
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            return ref.valueType().value(XContentMapValues.extractValue(fqn, sourceAsMap));
        });
    }
}
