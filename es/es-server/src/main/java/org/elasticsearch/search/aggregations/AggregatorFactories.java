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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.AggregationPath.PathElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AggregatorFactories {
    public static final Pattern VALID_AGG_NAME = Pattern.compile("[^\\[\\]>]+");

    /**
     * Parses the aggregation request recursively generating aggregator
     * factories in turn.
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser) throws IOException {
        return parseAggregators(parser, 0);
    }

    private static AggregatorFactories.Builder parseAggregators(XContentParser parser, int level) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation.");
            }
            final String aggregationName = parser.currentName();
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid aggregation name [" + aggregationName
                        + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'");
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Aggregation definition for [" + aggregationName + " starts with a ["
                        + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }

            BaseAggregationBuilder aggBuilder = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                            parser.getTokenLocation(), "Expected [" + XContentParser.Token.FIELD_NAME + "] under a ["
                            + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + aggregationName + "]",
                            parser.getTokenLocation());
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                    case "meta":
                        metaData = parser.map();
                        break;
                    case "aggregations":
                    case "aggs":
                        if (subFactories != null) {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "Found two sub aggregation definitions under [" + aggregationName + "]");
                        }
                        subFactories = parseAggregators(parser, level + 1);
                        break;
                    default:
                        if (aggBuilder != null) {
                            throw new ParsingException(parser.getTokenLocation(), "Found two aggregation type definitions in ["
                                    + aggregationName + "]: [" + aggBuilder.getType() + "] and [" + fieldName + "]");
                        }

                        aggBuilder = parser.namedObject(BaseAggregationBuilder.class, fieldName,
                                new AggParseContext(aggregationName));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] under ["
                            + fieldName + "], but got a [" + token + "] in [" + aggregationName + "]");
                }
            }

            if (aggBuilder == null) {
                throw new ParsingException(parser.getTokenLocation(), "Missing definition for aggregation [" + aggregationName + "]",
                        parser.getTokenLocation());
            } else {
                if (metaData != null) {
                    aggBuilder.setMetaData(metaData);
                }

                if (subFactories != null) {
                    aggBuilder.subAggregations(subFactories);
                }

                if (aggBuilder instanceof AggregationBuilder) {
                    factories.addAggregator((AggregationBuilder) aggBuilder);
                } else {
                    factories.addPipelineAggregator((PipelineAggregationBuilder) aggBuilder);
                }
            }
        }

        return factories;
    }

    /**
     * Context to parse and aggregation. This should eventually be removed and replaced with a String.
     */
    public static final class AggParseContext {
        public final String name;

        public AggParseContext(String name) {
            this.name = name;
        }
    }

    public static final AggregatorFactories EMPTY = new AggregatorFactories(null, new AggregatorFactory<?>[0],
            new ArrayList<PipelineAggregationBuilder>());

    private AggregatorFactory<?> parent;
    private AggregatorFactory<?>[] factories;
    private List<PipelineAggregationBuilder> pipelineAggregatorFactories;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory<?> parent, AggregatorFactory<?>[] factories,
            List<PipelineAggregationBuilder> pipelineAggregators) {
        this.parent = parent;
        this.factories = factories;
        this.pipelineAggregatorFactories = pipelineAggregators;
    }

    public List<PipelineAggregator> createPipelineAggregators() throws IOException {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>(this.pipelineAggregatorFactories.size());
        for (PipelineAggregationBuilder factory : this.pipelineAggregatorFactories) {
            pipelineAggregators.add(factory.create());
        }
        return pipelineAggregators;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple
     * buckets.
     */
    public Aggregator[] createSubAggregators(Aggregator parent) throws IOException {
        Aggregator[] aggregators = new Aggregator[countAggregators()];
        for (int i = 0; i < factories.length; ++i) {
            // TODO: sometimes even sub aggregations always get called with bucket 0, eg. if
            // you have a terms agg under a top-level filter agg. We should have a way to
            // propagate the fact that only bucket 0 will be collected with single-bucket
            // aggs
            final boolean collectsFromSingleBucket = false;
            Aggregator factory = factories[i].create(parent, collectsFromSingleBucket);
            Profilers profilers = factory.context().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators() throws IOException {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            // top-level aggs only get called with bucket 0
            final boolean collectsFromSingleBucket = true;
            Aggregator factory = factories[i].create(null, collectsFromSingleBucket);
            Profilers profilers = factory.context().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    /**
     * @return the number of sub-aggregator factories not including pipeline
     *         aggregator factories
     */
    public int countAggregators() {
        return factories.length;
    }

    /**
     * @return the number of pipeline aggregator factories
     */
    public int countPipelineAggregators() {
        return pipelineAggregatorFactories.size();
    }

    public static class Builder implements Writeable, ToXContentObject {
        private final Set<String> names = new HashSet<>();

        // Using LinkedHashSets to preserve the order of insertion, that makes the results
        // ordered nicely, although technically order does not matter
        private final Collection<AggregationBuilder> aggregationBuilders = new LinkedHashSet<>();
        private final Collection<PipelineAggregationBuilder> pipelineAggregatorBuilders = new LinkedHashSet<>();
        private boolean skipResolveOrder;

        /**
         * Create an empty builder.
         */
        public Builder() {
        }

        /**
         * Read from a stream.
         */
        public Builder(StreamInput in) throws IOException {
            int factoriesSize = in.readVInt();
            for (int i = 0; i < factoriesSize; i++) {
                addAggregator(in.readNamedWriteable(AggregationBuilder.class));
            }
            int pipelineFactoriesSize = in.readVInt();
            for (int i = 0; i < pipelineFactoriesSize; i++) {
                addPipelineAggregator(in.readNamedWriteable(PipelineAggregationBuilder.class));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.aggregationBuilders.size());
            for (AggregationBuilder factory : aggregationBuilders) {
                out.writeNamedWriteable(factory);
            }
            out.writeVInt(this.pipelineAggregatorBuilders.size());
            for (PipelineAggregationBuilder factory : pipelineAggregatorBuilders) {
                out.writeNamedWriteable(factory);
            }
        }

        public boolean mustVisitAllDocs() {
            for (AggregationBuilder builder : aggregationBuilders) {
                if (builder instanceof GlobalAggregationBuilder) {
                    return true;
                } else if (builder instanceof TermsAggregationBuilder) {
                    if (((TermsAggregationBuilder) builder).minDocCount() == 0) {
                        return true;
                    }
                }

            }
            return false;
        }



        public Builder addAggregator(AggregationBuilder factory) {
            if (!names.add(factory.name)) {
                throw new IllegalArgumentException("Two sibling aggregations cannot have the same name: [" + factory.name + "]");
            }
            aggregationBuilders.add(factory);
            return this;
        }

        public Builder addPipelineAggregator(PipelineAggregationBuilder pipelineAggregatorFactory) {
            this.pipelineAggregatorBuilders.add(pipelineAggregatorFactory);
            return this;
        }

        /**
         * FOR TESTING ONLY
         */
        Builder skipResolveOrder() {
            this.skipResolveOrder = true;
            return this;
        }

        public AggregatorFactories build(SearchContext context, AggregatorFactory<?> parent) throws IOException {
            if (aggregationBuilders.isEmpty() && pipelineAggregatorBuilders.isEmpty()) {
                return EMPTY;
            }
            List<PipelineAggregationBuilder> orderedpipelineAggregators = null;
            if (skipResolveOrder) {
                orderedpipelineAggregators = new ArrayList<>(pipelineAggregatorBuilders);
            } else {
                orderedpipelineAggregators = resolvePipelineAggregatorOrder(this.pipelineAggregatorBuilders, this.aggregationBuilders,
                        parent);
            }
            AggregatorFactory<?>[] aggFactories = new AggregatorFactory<?>[aggregationBuilders.size()];

            int i = 0;
            for (AggregationBuilder agg : aggregationBuilders) {
                aggFactories[i] = agg.build(context, parent);
                ++i;
            }
            return new AggregatorFactories(parent, aggFactories, orderedpipelineAggregators);
        }

        private List<PipelineAggregationBuilder> resolvePipelineAggregatorOrder(
                Collection<PipelineAggregationBuilder> pipelineAggregatorBuilders, Collection<AggregationBuilder> aggregationBuilders,
                AggregatorFactory<?> parent) {
            Map<String, PipelineAggregationBuilder> pipelineAggregatorBuildersMap = new HashMap<>();
            for (PipelineAggregationBuilder builder : pipelineAggregatorBuilders) {
                pipelineAggregatorBuildersMap.put(builder.getName(), builder);
            }
            Map<String, AggregationBuilder> aggBuildersMap = new HashMap<>();
            for (AggregationBuilder aggBuilder : aggregationBuilders) {
                aggBuildersMap.put(aggBuilder.name, aggBuilder);
            }
            List<PipelineAggregationBuilder> orderedPipelineAggregatorrs = new LinkedList<>();
            List<PipelineAggregationBuilder> unmarkedBuilders = new ArrayList<>(pipelineAggregatorBuilders);
            Collection<PipelineAggregationBuilder> temporarilyMarked = new HashSet<>();
            while (!unmarkedBuilders.isEmpty()) {
                PipelineAggregationBuilder builder = unmarkedBuilders.get(0);
                builder.validate(parent, aggregationBuilders, pipelineAggregatorBuilders);
                resolvePipelineAggregatorOrder(aggBuildersMap, pipelineAggregatorBuildersMap, orderedPipelineAggregatorrs, unmarkedBuilders,
                        temporarilyMarked, builder);
            }
            return orderedPipelineAggregatorrs;
        }

        private void resolvePipelineAggregatorOrder(Map<String, AggregationBuilder> aggBuildersMap,
                Map<String, PipelineAggregationBuilder> pipelineAggregatorBuildersMap,
                List<PipelineAggregationBuilder> orderedPipelineAggregators, List<PipelineAggregationBuilder> unmarkedBuilders,
                Collection<PipelineAggregationBuilder> temporarilyMarked, PipelineAggregationBuilder builder) {
            if (temporarilyMarked.contains(builder)) {
                throw new IllegalArgumentException("Cyclical dependency found with pipeline aggregator [" + builder.getName() + "]");
            } else if (unmarkedBuilders.contains(builder)) {
                temporarilyMarked.add(builder);
                String[] bucketsPaths = builder.getBucketsPaths();
                for (String bucketsPath : bucketsPaths) {
                    List<AggregationPath.PathElement> bucketsPathElements = AggregationPath.parse(bucketsPath).getPathElements();
                    String firstAggName = bucketsPathElements.get(0).name;
                    if (bucketsPath.equals("_count") || bucketsPath.equals("_key")) {
                        continue;
                    } else if (aggBuildersMap.containsKey(firstAggName)) {
                        AggregationBuilder aggBuilder = aggBuildersMap.get(firstAggName);
                        for (int i = 1; i < bucketsPathElements.size(); i++) {
                            PathElement pathElement = bucketsPathElements.get(i);
                            String aggName = pathElement.name;
                            if ((i == bucketsPathElements.size() - 1) && (aggName.equalsIgnoreCase("_key") || aggName.equals("_count"))) {
                                break;
                            } else {
                                // Check the non-pipeline sub-aggregator
                                // factories
                                Collection<AggregationBuilder> subBuilders = aggBuilder.factoriesBuilder.aggregationBuilders;
                                boolean foundSubBuilder = false;
                                for (AggregationBuilder subBuilder : subBuilders) {
                                    if (aggName.equals(subBuilder.name)) {
                                        aggBuilder = subBuilder;
                                        foundSubBuilder = true;
                                        break;
                                    }
                                }
                                // Check the pipeline sub-aggregator factories
                                if (!foundSubBuilder && (i == bucketsPathElements.size() - 1)) {
                                    Collection<PipelineAggregationBuilder> subPipelineBuilders = aggBuilder.factoriesBuilder.pipelineAggregatorBuilders;
                                    for (PipelineAggregationBuilder subFactory : subPipelineBuilders) {
                                        if (aggName.equals(subFactory.getName())) {
                                            foundSubBuilder = true;
                                            break;
                                        }
                                    }
                                }
                                if (!foundSubBuilder) {
                                    throw new IllegalArgumentException("No aggregation [" + aggName + "] found for path [" + bucketsPath
                                            + "]");
                                }
                            }
                        }
                        continue;
                    } else {
                        PipelineAggregationBuilder matchingBuilder = pipelineAggregatorBuildersMap.get(firstAggName);
                        if (matchingBuilder != null) {
                            resolvePipelineAggregatorOrder(aggBuildersMap, pipelineAggregatorBuildersMap, orderedPipelineAggregators,
                                    unmarkedBuilders, temporarilyMarked, matchingBuilder);
                        } else {
                            throw new IllegalArgumentException("No aggregation found for path [" + bucketsPath + "]");
                        }
                    }
                }
                unmarkedBuilders.remove(builder);
                temporarilyMarked.remove(builder);
                orderedPipelineAggregators.add(builder);
            }
        }

        public Collection<AggregationBuilder> getAggregatorFactories() {
            return Collections.unmodifiableCollection(aggregationBuilders);
        }

        public Collection<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
            return Collections.unmodifiableCollection(pipelineAggregatorBuilders);
        }

        public int count() {
            return aggregationBuilders.size() + pipelineAggregatorBuilders.size();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (aggregationBuilders != null) {
                for (AggregationBuilder subAgg : aggregationBuilders) {
                    subAgg.toXContent(builder, params);
                }
            }
            if (pipelineAggregatorBuilders != null) {
                for (PipelineAggregationBuilder subAgg : pipelineAggregatorBuilders) {
                    subAgg.toXContent(builder, params);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregationBuilders, pipelineAggregatorBuilders);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Builder other = (Builder) obj;

            if (!Objects.equals(aggregationBuilders, other.aggregationBuilders))
                return false;
            if (!Objects.equals(pipelineAggregatorBuilders, other.pipelineAggregatorBuilders))
                return false;
            return true;
        }

        /**
         * Rewrites the underlying aggregation builders into their primitive
         * form. If the builder did not change the identity reference must be
         * returned otherwise the builder will be rewritten infinitely.
         */
        public Builder rewrite(QueryRewriteContext context) throws IOException {
            boolean changed = false;
            Builder newBuilder = new Builder();

            for (AggregationBuilder builder : aggregationBuilders) {
                AggregationBuilder result = AggregationBuilder.rewriteAggregation(builder, context);
                if (result != builder) {
                    changed = true;
                }
                newBuilder.addAggregator(result);
            }

            if (changed) {
                for (PipelineAggregationBuilder builder : pipelineAggregatorBuilders) {
                    newBuilder.addPipelineAggregator(builder);
                }
                return newBuilder;
            } else {
                return this;
            }
        }
    }
}
