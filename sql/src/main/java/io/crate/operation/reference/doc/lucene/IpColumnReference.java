/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.doc.lucene;


import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class IpColumnReference extends LuceneCollectorExpression<BytesRef> {

    private BytesRef value;
    private SortedDocValues values;

    public IpColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public BytesRef value() {
        return value;
    }

    @Override
    public void setNextDocId(int docId) {
        if (values == null) {
            value = null;
        } else {
            BytesRef bytesRef = values.get(docId);
            String format = DocValueFormat.IP.format(bytesRef);
            value = BytesRefs.toBytesRef(format);
        }
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        // TODO: is it safe to unwrap in case it's an array of ips?
        values = DocValues.unwrapSingleton(context.reader().getSortedSetDocValues(columnName));
    }
}
