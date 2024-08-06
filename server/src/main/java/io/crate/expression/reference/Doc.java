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

package io.crate.expression.reference;

import java.util.Map;

import io.crate.expression.reference.doc.lucene.StoredRow;

public final class Doc {

    private final StoredRow storedRow;
    private final int docId;
    private final String index;
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;

    public Doc(int docId,
               String index,
               String id,
               long version,
               long seqNo,
               long primaryTerm,
               StoredRow storedRow) {
        this.docId = docId;
        this.index = index;
        this.id = id;
        this.version = version;
        this.storedRow = storedRow;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public int docId() {
        return docId;
    }

    public long getVersion() {
        return version;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public String getId() {
        return id;
    }

    public String getRaw() {
        return storedRow.asString();
    }

    public Map<String, Object> getSource() {
        return storedRow.asMap();
    }

    public String getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return storedRow != null ? storedRow.toString() : null;
    }
}
