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
package org.elasticsearch.search.aggregations.metrics.min;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MinAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;

    final String pointField;
    final Function<byte[], Number> pointConverter;

    DoubleArray mins;

    MinAggregator(String name,
                    ValuesSourceConfig<ValuesSource.Numeric> config,
                    ValuesSource.Numeric valuesSource,
                    SearchContext context,
                    Aggregator parent,
                    List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            mins = context.bigArrays().newDoubleArray(1, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        }
        this.format = config.format();
        this.pointConverter = getPointReaderOrNull(context, parent, config);
        if (pointConverter != null) {
            pointField = config.fieldContext().field();
        } else {
            pointField = null;
        }
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            if (parent == null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                // we have no parent and the values source is empty so we can skip collecting hits.
                throw new CollectionTerminatedException();
            }
        }
        if (pointConverter != null) {
            Number segMin = findLeafMinValue(ctx.reader(), pointField, pointConverter);
            if (segMin != null) {
                /**
                 * There is no parent aggregator (see {@link MinAggregator#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                double min = mins.get(0);
                min = Math.min(min, segMin.doubleValue());
                mins.set(0, min);
                // the minimum value has been extracted, we don't need to collect hits on this segment.
                throw new CollectionTerminatedException();
            }
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues allValues = valuesSource.doubleValues(ctx);
        final NumericDoubleValues values = MultiValueMode.MIN.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= mins.size()) {
                    long from = mins.size();
                    mins = bigArrays.grow(mins, bucket + 1);
                    mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
                }
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double min = mins.get(bucket);
                    min = Math.min(min, value);
                    mins.set(bucket, min);
                }
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= mins.size()) {
            return Double.POSITIVE_INFINITY;
        }
        return mins.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= mins.size()) {
            return buildEmptyAggregation();
        }
        return new InternalMin(name, mins.get(bucket), format, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMin(name, Double.POSITIVE_INFINITY, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(mins);
    }


    /**
     * Returns a converter for point values if early termination is applicable to
     * the context or <code>null</code> otherwise.
     *
     * @param context The {@link SearchContext} of the aggregation.
     * @param parent The parent aggregator.
     * @param config The config for the values source metric.
     */
    public static Function<byte[], Number> getPointReaderOrNull(SearchContext context, Aggregator parent,
                                                                ValuesSourceConfig<ValuesSource.Numeric> config) {
        if (context.query() != null &&
                context.query().getClass() != MatchAllDocsQuery.class) {
            return null;
        }
        if (parent != null) {
            return null;
        }
        if (config.fieldContext() != null && config.script() == null) {
            MappedFieldType fieldType = config.fieldContext().fieldType();
            if (fieldType == null || fieldType.indexOptions() == IndexOptions.NONE) {
                return null;
            }
            Function<byte[], Number> converter = null;
            if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
                converter = ((NumberFieldMapper.NumberFieldType) fieldType)::parsePoint;
            } else if (fieldType.getClass() == DateFieldMapper.DateFieldType.class) {
                converter = (in) -> LongPoint.decodeDimension(in, 0);
            }
            return converter;
        }
        return null;
    }

    /**
     * Returns the minimum value indexed in the <code>fieldName</code> field or <code>null</code>
     * if the value cannot be inferred from the indexed {@link PointValues}.
     */
    public static Number findLeafMinValue(LeafReader reader, String fieldName, Function<byte[], Number> converter) throws IOException {
        final PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues == null) {
            return null;
        }
        final Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return converter.apply(pointValues.getMinPackedValue());
        }
        final Number[] result = new Number[1];
        try {
            pointValues.intersect(new PointValues.IntersectVisitor() {
                @Override
                public void visit(int docID) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (liveDocs.get(docID)) {
                        result[0] = converter.apply(packedValue);
                        // this is the first leaf with a live doc so the value is the minimum for this segment.
                        throw new CollectionTerminatedException();
                    }
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            });
        } catch (CollectionTerminatedException e) {}
        return result[0];
    }
}
