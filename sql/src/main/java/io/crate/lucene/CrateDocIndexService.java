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

package io.crate.lucene;

import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;


@Singleton  // per index
public class CrateDocIndexService {

    private final CollectInputSymbolVisitor<? extends Input<?>> docInputSymbolVisitor;

    @Inject
    public CrateDocIndexService(Index index, Functions functions, MapperService mapperService) {
        if (BlobIndices.isBlobIndex(index.name())) {
            docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, BlobReferenceResolver.INSTANCE);
        } else {
            ReferenceResolver<? extends Input<?>> resolver = new LuceneReferenceResolver(mapperService);
            docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, resolver);
        }
    }

    public CollectInputSymbolVisitor<?> docInputSymbolVisitor() {
        return docInputSymbolVisitor;
    }
}
