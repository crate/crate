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

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.util.Collection;
import java.util.Collections;


/**
 * Specialization of {@link AtomicNumericFieldData} for floating-point numerics.
 */
abstract class AtomicDoubleFieldData implements AtomicNumericFieldData {

    private final long ramBytesUsed;

    AtomicDoubleFieldData(long ramBytesUsed) {
        this.ramBytesUsed = ramBytesUsed;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public final ScriptDocValues getScriptValues() {
        return new ScriptDocValues.Doubles(getDoubleValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getDoubleValues());
    }

    @Override
    public final SortedNumericDocValues getLongValues() {
        return FieldData.castToLong(getDoubleValues());
    }

    public static AtomicNumericFieldData empty(final int maxDoc) {
        return new AtomicDoubleFieldData(0) {

            @Override
            public SortedNumericDoubleValues getDoubleValues() {
                return FieldData.emptySortedNumericDoubles();
            }
            
            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

        };
    }

    @Override
    public void close() {
    }

}
