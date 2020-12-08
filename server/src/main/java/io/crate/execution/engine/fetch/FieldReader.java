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

import io.crate.exceptions.Exceptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

public class FieldReader {

    private FieldReader() {}

    public static void visitReader(LeafReaderContext context, Integer docId, StoredFieldVisitor visitor) throws IOException {
        context.reader().document(docId, visitor);
    }

    public static void visitSequentialReader(LeafReaderContext context, Integer docId, StoredFieldVisitor visitor) throws IOException {
        ((SequentialStoredFieldsLeafReader) context.reader()).getSequentialStoredFieldsReader().visitDocument(docId, visitor);
    }

    public static CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> getSequentialFieldReader(LeafReaderContext context) {
        try {
            if (context.reader() instanceof SequentialStoredFieldsLeafReader) {
                SequentialStoredFieldsLeafReader reader = (SequentialStoredFieldsLeafReader) context.reader();
                return reader.getSequentialStoredFieldsReader()::visitDocument;
            }
        } catch (IOException e) {
            throw Exceptions.toRuntimeException(e);
        }
        throw new RuntimeException("Query is not ordered");
    }

    public static CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> getFieldReader(LeafReaderContext context) {
        return context.reader()::document;
    }

}
