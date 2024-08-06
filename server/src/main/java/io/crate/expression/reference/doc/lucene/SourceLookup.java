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

package io.crate.expression.reference.doc.lucene;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.elasticsearch.common.compress.CompressorFactory;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.metadata.Reference;

public final class SourceLookup {

    private final SourceFieldVisitor fieldsVisitor = new SourceFieldVisitor();
    private final SourceParser sourceParser;
    private int doc;
    private ReaderContext readerContext;
    private boolean docVisited = false;
    private Map<String, Object> parsedSource = null;

    private final Source source = new Source() {
        @Override
        public Map<String, Object> sourceAsMap() {
            if (parsedSource == null) {
                ensureDocVisited();
                parsedSource = sourceParser.parse(fieldsVisitor.source());
            }
            return parsedSource;
        }

        @Override
        public String rawSource() {
            ensureDocVisited();
            try {
                return CompressorFactory.uncompressIfNeeded(fieldsVisitor.source()).utf8ToString();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to decompress source", e);
            }
        }
    };

    SourceLookup(Set<Reference> droppedColumns, UnaryOperator<String> lookupNameBySourceKey) {
        sourceParser = new SourceParser(droppedColumns, lookupNameBySourceKey);
    }

    public Source getSource(ReaderContext context, int doc) {
        if (this.doc == doc
                && this.readerContext != null
                && this.readerContext.reader() == context.reader()) {
            // Don't invalidate source
            return source;
        }
        fieldsVisitor.reset();
        this.docVisited = false;
        this.doc = doc;
        this.readerContext = context;
        this.parsedSource = null;
        return this.source;
    }

    private void ensureDocVisited() {
        if (docVisited) {
            return;
        }
        try {
            readerContext.visitDocument(doc, fieldsVisitor);
            docVisited = true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SourceLookup registerRef(Reference ref) {
        sourceParser.register(ref.column(), ref.valueType());
        return this;
    }
}
