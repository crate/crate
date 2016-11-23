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

import io.crate.metadata.doc.DocSysColumns;
import org.apache.lucene.index.LeafReaderContext;

public class FetchIdCollectorExpression extends LuceneCollectorExpression<Long> {

    public static final String COLUMN_NAME = DocSysColumns.FETCHID.name();

    private long readerId;
    private long docId;
    private int docBase;
    private byte relationId;

    @Override
    public void startCollect(CollectorContext context) {
        super.startCollect(context);
        this.readerId = (long) context.readerId();
        this.relationId = context.relationId();
    }

    @Override
    public void setNextDocId(int doc) {
        super.setNextDocId(doc);
        docId = FetchIds.packFetchId(relationId, readerId, docBase + doc);
    }

    @Override
    public Long value() {
        return docId;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        super.setNextReader(context);
        docBase = context.docBase;
    }
}
