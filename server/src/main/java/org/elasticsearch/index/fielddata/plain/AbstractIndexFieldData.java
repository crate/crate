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

import java.io.IOException;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.RamAccountingTermsEnum;

public abstract class AbstractIndexFieldData<FD extends AtomicFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final String fieldName;

    public AbstractIndexFieldData(IndexSettings indexSettings, String fieldName) {
        super(indexSettings);
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public void clear() {
    }

    /**
     * @param maxDoc of the current reader
     * @return an empty field data instances for field data lookups of empty segments (returning no values)
     */
    protected abstract FD empty(int maxDoc);

    /**
     * A {@code PerValueEstimator} is a sub-class that can be used to estimate
     * the memory overhead for loading the data. Each field data
     * implementation should implement its own {@code PerValueEstimator} if it
     * intends to take advantage of the MemoryCircuitBreaker.
     * <p>
     * Note that the .beforeLoad(...) and .afterLoad(...) methods must be
     * manually called.
     */
    public interface PerValueEstimator {

        /**
         * @return the number of bytes for the given term
         */
        long bytesPerValue(BytesRef term);

        /**
         * Execute any pre-loading estimations for the terms. May also
         * optionally wrap a {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum}
         * which will estimate the memory on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A TermsEnum for the given terms
         */
        TermsEnum beforeLoad(Terms terms) throws IOException;

        /**
         * Possibly adjust a circuit breaker after field data has been loaded,
         * now that the actual amount of memory used by the field data is known
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        void afterLoad(TermsEnum termsEnum, long actualUsed);
    }
}
