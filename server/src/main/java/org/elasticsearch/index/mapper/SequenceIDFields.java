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

package org.elasticsearch.index.mapper;

import java.util.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.elasticsearch.index.seqno.SequenceNumbers;

import io.crate.metadata.doc.SysColumns;

/**
 * A sequence ID, which is made up of a sequence number (both the searchable
 * and doc_value version of the field) and the primary term.
 */
public class SequenceIDFields {

    public final Field seqNo;
    public final Field seqNoDocValue;
    public final Field primaryTerm;
    public final Field tombstoneField;

    public SequenceIDFields(Field seqNo, Field seqNoDocValue, Field primaryTerm, Field tombstoneField) {
        Objects.requireNonNull(seqNo, "sequence number field cannot be null");
        Objects.requireNonNull(seqNoDocValue, "sequence number dv field cannot be null");
        Objects.requireNonNull(primaryTerm, "primary term field cannot be null");
        this.seqNo = seqNo;
        this.seqNoDocValue = seqNoDocValue;
        this.primaryTerm = primaryTerm;
        this.tombstoneField = tombstoneField;
    }

    public static SequenceIDFields emptySeqID() {
        return new SequenceIDFields(
            new LongPoint(SysColumns.Names.SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO),
            new NumericDocValuesField(SysColumns.Names.SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO),
            new NumericDocValuesField(SysColumns.Names.PRIMARY_TERM, 0),
            new NumericDocValuesField(SysColumns.Names.TOMBSTONE, 0));
    }
}
