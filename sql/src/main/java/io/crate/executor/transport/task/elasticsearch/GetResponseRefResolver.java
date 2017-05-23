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
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.reference.ReferenceResolver;
import org.apache.lucene.util.BytesRef;
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
    private final Map<ColumnIdent, Integer> pkMapping;
    private final Map<String, DocKeys.DocKey> ids2Keys;


    public GetResponseRefResolver(Consumer<ColumnIdent> columnConsumer,
                                  Map<ColumnIdent, Integer> pkMapping,
                                  Map<String, DocKeys.DocKey> ids2Keys) {
        this.columnConsumer = columnConsumer;
        this.pkMapping = pkMapping;
        this.ids2Keys = ids2Keys;
    }

    @Override
    public CollectExpression<GetResponse, ?> getImplementation(Reference ref) {
        ColumnIdent columnIdent = ref.ident().columnIdent();
        columnConsumer.accept(columnIdent);
        String fqn = columnIdent.fqn();
        switch (fqn) {
            case DocSysColumns.Names.VERSION:
                return CollectExpression.of(GetResponse::getVersion);

            case DocSysColumns.Names.ID:
                return CollectExpression.of(r -> new BytesRef(r.getId().getBytes()));

            case DocSysColumns.Names.RAW:
                return CollectExpression.of(r -> r.getSourceAsBytesRef().toBytesRef());

            case DocSysColumns.Names.DOC:
                return CollectExpression.of(GetResponse::getSource);

        }
        Integer pkPos = pkMapping.get(columnIdent);
        if (pkPos != null) {
            return CollectExpression.of(
                response -> ValueSymbolVisitor.VALUE.process(ids2Keys.get(response.getId()).values().get(pkPos)));
        }

        return CollectExpression.of(response -> {
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            return ref.valueType().value(XContentMapValues.extractValue(fqn, sourceAsMap));
        });
    }
}
