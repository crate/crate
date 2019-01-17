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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code geohash_grid} aggregation. Defines multiple buckets, each representing a cell in a geo-grid of a specific
 * precision.
 */
public interface GeoHashGrid extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a {@code geohash_grid} cell. The key of the bucket is the {@code geohash} of the cell
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {
    }

    /**
     * @return  The buckets of this aggregation (each bucket representing a geohash grid cell)
     */
    @Override
    List<? extends Bucket> getBuckets();
}
