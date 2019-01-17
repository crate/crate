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

package org.elasticsearch.index.query;

import org.elasticsearch.common.ParseField;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder {

    public static final ParseField LTE_FIELD = new ParseField("lte");
    public static final ParseField GTE_FIELD = new ParseField("gte");
    public static final ParseField GT_FIELD = new ParseField("gt");
    public static final ParseField LT_FIELD = new ParseField("lt");
}
