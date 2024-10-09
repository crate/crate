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

package org.elasticsearch.index.engine;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;

import io.crate.metadata.doc.SysColumns;

final class CombinedDocValues {
    private final NumericDocValues versionDV;
    private final NumericDocValues seqNoDV;
    private final NumericDocValues primaryTermDV;
    private final NumericDocValues tombstoneDV;
    private final NumericDocValues recoverySource;

    CombinedDocValues(LeafReader leafReader) throws IOException {
        this.versionDV = Objects.requireNonNull(leafReader.getNumericDocValues(SysColumns.VERSION.name()), "VersionDV is missing");
        this.seqNoDV = Objects.requireNonNull(leafReader.getNumericDocValues(SysColumns.Names.SEQ_NO), "SeqNoDV is missing");
        this.primaryTermDV = Objects.requireNonNull(
            leafReader.getNumericDocValues(SysColumns.Names.PRIMARY_TERM), "PrimaryTermDV is missing");
        this.tombstoneDV = leafReader.getNumericDocValues(SysColumns.Names.TOMBSTONE);
        this.recoverySource = leafReader.getNumericDocValues(SysColumns.Source.RECOVERY_NAME);
    }

    long docVersion(int segmentDocId) throws IOException {
        assert versionDV.docID() < segmentDocId;
        if (versionDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + SysColumns.VERSION.name() + "] is not found";
            throw new IllegalStateException("DocValues for field [" + SysColumns.VERSION.name() + "] is not found");
        }
        return versionDV.longValue();
    }

    long docSeqNo(int segmentDocId) throws IOException {
        assert seqNoDV.docID() < segmentDocId;
        if (seqNoDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + SysColumns.Names.SEQ_NO + "] is not found";
            throw new IllegalStateException("DocValues for field [" + SysColumns.Names.SEQ_NO + "] is not found");
        }
        return seqNoDV.longValue();
    }

    long docPrimaryTerm(int segmentDocId) throws IOException {
        // We exclude non-root nested documents when querying changes, every returned document must have primary term.
        assert primaryTermDV.docID() < segmentDocId;
        if (primaryTermDV.advanceExact(segmentDocId) == false) {
            assert false : "DocValues for field [" + SysColumns.Names.PRIMARY_TERM + "] is not found";
            throw new IllegalStateException("DocValues for field [" + SysColumns.Names.PRIMARY_TERM + "] is not found");
        }
        return primaryTermDV.longValue();
    }

    boolean isTombstone(int segmentDocId) throws IOException {
        if (tombstoneDV == null) {
            return false;
        }
        assert tombstoneDV.docID() < segmentDocId;
        return tombstoneDV.advanceExact(segmentDocId) && tombstoneDV.longValue() > 0;
    }

    boolean hasRecoverySource(int segmentDocId) throws IOException {
        if (recoverySource == null) {
            return false;
        }
        assert recoverySource.docID() < segmentDocId;
        return recoverySource.advanceExact(segmentDocId);
    }
}
