/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 * Specialization of {@link AtomicNumericFieldData} for integers.
 */
abstract class AtomicLongFieldData implements AtomicNumericFieldData {

    private final long ramBytesUsed;
    /**
     * Type of this field. Used to expose appropriate types in {@link #getScriptValues()}.
     */
    private final NumericType numericType;

    AtomicLongFieldData(long ramBytesUsed, NumericType numericType) {
        this.ramBytesUsed = ramBytesUsed;
        this.numericType = numericType;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public final ScriptDocValues<?> getLegacyFieldValues() {
        switch (numericType) {
            case DATE:
                final ScriptDocValues.Dates realDV = new ScriptDocValues.Dates(getLongValues());
                return new ScriptDocValues<DateTime>() {

                    @Override
                    public int size() {
                        return realDV.size();
                    }

                    @Override
                    public DateTime get(int index) {
                        JodaCompatibleZonedDateTime dt = realDV.get(index);
                        return new DateTime(dt.toInstant().toEpochMilli(), DateTimeZone.UTC);
                    }

                    @Override
                    public void setNextDocId(int docId) throws IOException {
                        realDV.setNextDocId(docId);
                    }
                };
            default:
                return getScriptValues();
        }
    }

    @Override
    public final ScriptDocValues<?> getScriptValues() {
        switch (numericType) {
        case DATE:
            return new ScriptDocValues.Dates(getLongValues());
        case BOOLEAN:
            return new ScriptDocValues.Booleans(getLongValues());
        default:
            return new ScriptDocValues.Longs(getLongValues());
        }
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getLongValues());
    }

    @Override
    public final SortedNumericDoubleValues getDoubleValues() {
        return FieldData.castToDouble(getLongValues());
    }

    @Override
    public void close() {}
}
