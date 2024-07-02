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

package org.elasticsearch.index.codec;

import java.io.IOException;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.lucene.Lucene;

import io.crate.lucene.codec.CustomLucene90DocValuesFormat;
import io.crate.types.FloatVectorType;


/**
 * {@link CrateCodec This codec} is the default {@link Codec} for Crate.
 * It disables compression on docvalues terms dictionaries, and increases
 * the max supported vector dimension to {@link FloatVectorType#MAX_DIMENSIONS}
 */
// LUCENE UPGRADE: make sure to move to a new codec depending on the lucene version
public class CrateCodec extends Lucene99Codec {

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(CrateCodec.class) : "CrateCodec must subclass the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    public CrateCodec(Mode compressionMode) {
        super(compressionMode);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return new CustomLucene90DocValuesFormat(CustomLucene90DocValuesFormat.Mode.BEST_SPEED);
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        var format = super.getKnnVectorsFormatForField(field);
        return new KnnVectorsFormat(format.getName()) {

            @Override
            public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                return format.fieldsWriter(state);
            }

            @Override
            public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                return format.fieldsReader(state);
            }

            @Override
            public int getMaxDimensions(String fieldName) {
                return FloatVectorType.MAX_DIMENSIONS;
            }
        };
    }
}
