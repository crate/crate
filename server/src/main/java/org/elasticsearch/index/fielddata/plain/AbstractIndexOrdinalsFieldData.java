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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

public abstract class AbstractIndexOrdinalsFieldData extends AbstractIndexFieldData<AtomicOrdinalsFieldData> implements IndexOrdinalsFieldData {

    private final double minFrequency;
    private final double maxFrequency;
    private final int minSegmentSize;
    protected final CircuitBreakerService breakerService;

    protected AbstractIndexOrdinalsFieldData(IndexSettings indexSettings,
                                             String fieldName,
                                             CircuitBreakerService breakerService,
                                             double minFrequency,
                                             double maxFrequency,
                                             int minSegmentSize) {
        super(indexSettings, fieldName);
        this.breakerService = breakerService;
        this.minFrequency = minFrequency;
        this.maxFrequency = maxFrequency;
        this.minSegmentSize = minSegmentSize;
    }

    @Override
    protected AtomicOrdinalsFieldData empty(int maxDoc) {
        return AbstractAtomicOrdinalsFieldData.empty();
    }

    protected TermsEnum filter(Terms terms, TermsEnum iterator, LeafReader reader) throws IOException {
        if (iterator == null) {
            return null;
        }
        int docCount = terms.getDocCount();
        if (docCount == -1) {
            docCount = reader.maxDoc();
        }
        if (docCount >= minSegmentSize) {
            final int minFreq = minFrequency > 1.0
                    ? (int) minFrequency
                    : (int)(docCount * minFrequency);
            final int maxFreq = maxFrequency > 1.0
                    ? (int) maxFrequency
                    : (int)(docCount * maxFrequency);
            if (minFreq > 1 || maxFreq < docCount) {
                iterator = new FrequencyFilter(iterator, minFreq, maxFreq);
            }
        }
        return iterator;
    }

    private static final class FrequencyFilter extends FilteredTermsEnum {

        private final int minFreq;
        private final int maxFreq;

        FrequencyFilter(TermsEnum delegate, int minFreq, int maxFreq) {
            super(delegate, false);
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
        }

        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            int docFreq = docFreq();
            if (docFreq >= minFreq && docFreq <= maxFreq) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }

}
