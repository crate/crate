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


import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

public final class SourceLookup {

    private final SourceFieldVisitor fieldsVisitor = new SourceFieldVisitor();
    private LeafReader reader;
    private int doc;
    private Map<String, Object> source;
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

    public Object get(List<String> path) {
        ensureSourceParsed();
        return extractValue(source, path, 0);
    }

    public Map<String, Object> sourceAsMap() {
        ensureSourceParsed();
        return source;
    }

    public BytesReference rawSource() {
        ensureDocVisited();
        return fieldsVisitor.source();
    }

    private Map<String, Object> loadSource() {
        ensureDocVisited();
        return XContentHelper.convertToMap(fieldsVisitor.source(), false, XContentType.JSON).v2();
    }

    private void ensureSourceParsed() {
        if (source == null) {
            source = loadSource();
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

    @SuppressWarnings("unchecked")
    static Object extractValue(final Map map, List<String> path, int pathStartIndex) {
        assert path instanceof RandomAccess : "path should support RandomAccess for fast index optimized loop";
        Map m = map;
        Object tmp = null;
        for (int i = pathStartIndex; i < path.size(); i++) {
            tmp = m.get(path.get(i));
            if (tmp instanceof Map) {
                m = (Map) tmp;
            } else if (tmp instanceof List) {
                List list = (List) tmp;
                if (i + 1 == path.size()) {
                    return list;
                }
                List newList = new ArrayList(list.size());
                for (Object o : list) {
                    if (o instanceof Map) {
                        newList.add(extractValue((Map) o, path, i + 1));
                    } else {
                        newList.add(o);
                    }
                }
                return newList;
            } else {
                break;
            }
        }
        return tmp;
    }
}
