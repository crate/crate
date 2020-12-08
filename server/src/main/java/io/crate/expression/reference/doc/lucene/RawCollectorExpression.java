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

package io.crate.expression.reference.doc.lucene;

import io.crate.execution.engine.fetch.ReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.compress.CompressorFactory;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class RawCollectorExpression extends LuceneCollectorExpression<String> {

    private SourceLookup sourceLookup;
    private ReaderContext context;

    @Override
    public void startCollect(CollectorContext context) {
        this.sourceLookup = context.sourceLookup();
    }

    @Override
    public void setNextDocId(int doc, boolean ordered) {
        sourceLookup.setSegmentAndDocument(context, doc);
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        this.context = context;
    }

    @Override
    public String value() {
        try {
            return CompressorFactory.uncompressIfNeeded(sourceLookup.rawSource()).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to uncompress source", e);
        }
    }
}
