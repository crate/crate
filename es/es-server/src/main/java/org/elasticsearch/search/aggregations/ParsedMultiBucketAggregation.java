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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public abstract class ParsedMultiBucketAggregation<B extends ParsedMultiBucketAggregation.Bucket>
        extends ParsedAggregation implements MultiBucketsAggregation {

    protected final List<B> buckets = new ArrayList<>();
    protected boolean keyed = false;

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (B bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    protected static void declareMultiBucketAggregationFields(final ObjectParser<? extends ParsedMultiBucketAggregation, Void> objectParser,
                                                  final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser,
                                                  final CheckedFunction<XContentParser, ParsedBucket, IOException> keyedBucketParser) {
        declareAggregationFields(objectParser);
        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.keyed = true;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    aggregation.buckets.add(keyedBucketParser.apply(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.keyed = false;
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    aggregation.buckets.add(bucketParser.apply(parser));
                }
            }
        }, CommonFields.BUCKETS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    public abstract static class ParsedBucket implements MultiBucketsAggregation.Bucket {

        private Aggregations aggregations;
        private String keyAsString;
        private long docCount;
        private boolean keyed;

        protected void setKeyAsString(String keyAsString) {
            this.keyAsString = keyAsString;
        }

        @Override
        public String getKeyAsString() {
            return keyAsString;
        }

        protected void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        public void setKeyed(boolean keyed) {
            this.keyed = keyed;
        }

        protected boolean isKeyed() {
            return keyed;
        }

        protected void setAggregations(Aggregations aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                // Subclasses can override the getKeyAsString method to handle specific cases like
                // keyed bucket with RAW doc value format where the key_as_string field is not printed
                // out but we still need to have a string version of the key to use as the bucket's name.
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            if (keyAsString != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        protected static <B extends ParsedBucket> B parseXContent(final XContentParser parser,
                                                                  final boolean keyed,
                                                                  final Supplier<B> bucketSupplier,
                                                                  final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer)
                                                                        throws IOException {
            final B bucket = bucketSupplier.get();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else {
                        XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class,
                            aggregations::add);
                    }
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }
}
