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

package io.crate.execution.engine.fetch;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;

import java.io.IOException;

public class ReaderContext {

    private final LeafReaderContext leafReaderContext;
    private final CheckedBiConsumer<Integer,StoredFieldVisitor, IOException> storedFieldsReader;

    public ReaderContext(LeafReaderContext leafReaderContext,
                         CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> storedFieldsReader) {
        this.leafReaderContext = leafReaderContext;
        this.storedFieldsReader = storedFieldsReader;
    }

    public ReaderContext(LeafReaderContext leafReaderContext) {
        this(leafReaderContext, leafReaderContext.reader()::document);
    }

    public void visitDocument(int docId, StoredFieldVisitor visitor) throws IOException {
        storedFieldsReader.accept(docId, visitor);
    }

    public LeafReader reader() {
        return leafReaderContext.reader();
    }

    public int docBase() {
        return leafReaderContext.docBase;
    }

}
