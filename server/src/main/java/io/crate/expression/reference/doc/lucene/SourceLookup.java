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

package io.crate.expression.reference.doc.lucene;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.DataType;

public final class SourceLookup {

    private final SourceFieldVisitor fieldsVisitor = new SourceFieldVisitor();
    private final HashMap<ColumnIdent, DataType<?>> typesByColumn = new HashMap<>();
    private LeafReader reader;
    private int doc;
    private Map<ColumnIdent, Object> source;
    private boolean docVisited = false;

    SourceLookup() {
    }

    public void setSegmentAndDocument(LeafReaderContext context, int doc) {
        if (this.doc == doc && this.reader == context.reader()) {
            // Don't invalidate source
            return;
        }
        fieldsVisitor.reset();
        this.docVisited = false;
        this.source = null;
        this.reader = context.reader();
        this.doc = doc;
    }

    public Object get(ColumnIdent column) {
        ensureSourceParsed();
        return source.get(column.shiftRight());
    }

    public Map<String, Object> sourceAsMap() {
        ensureDocVisited();
        // TODO: If there are multiple `_doc` occurances or `_doc` + column we parse the source multiple times.
        // This could be avoided by registering the `_doc` and then depending on the registration use different code paths
        return XContentHelper.toMap(fieldsVisitor.source(), XContentType.JSON);
    }

    public BytesReference rawSource() {
        ensureDocVisited();
        return fieldsVisitor.source();
    }

    private void ensureSourceParsed() {
        if (source == null) {
            ensureDocVisited();
            source = SourceParser.parse(fieldsVisitor.source(), typesByColumn);
        }
    }

    private void ensureDocVisited() {
        if (docVisited) {
            return;
        }
        try {
            reader.document(doc, fieldsVisitor);
            docVisited = true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SourceLookup registerRef(Reference ref) {
        typesByColumn.put(ref.column().shiftRight(), ref.valueType());
        return this;
    }
}
