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

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory.MultiBucketAggregatorWrapper;
import org.elasticsearch.search.profile.AbstractInternalProfileTree;

public class InternalAggregationProfileTree extends AbstractInternalProfileTree<AggregationProfileBreakdown, Aggregator> {

    @Override
    protected AggregationProfileBreakdown createProfileBreakdown() {
        return new AggregationProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(Aggregator element) {

        // Anonymous classes (such as NonCollectingAggregator in TermsAgg) won't have a name,
        // we need to get the super class
        if (element.getClass().getSimpleName().isEmpty() == true) {
            return element.getClass().getSuperclass().getSimpleName();
        }
        if (element instanceof MultiBucketAggregatorWrapper) {
            return ((MultiBucketAggregatorWrapper) element).getWrappedClass().getSimpleName();
        }
        return element.getClass().getSimpleName();
    }

    @Override
    protected String getDescriptionFromElement(Aggregator element) {
        return element.name();
    }

}
