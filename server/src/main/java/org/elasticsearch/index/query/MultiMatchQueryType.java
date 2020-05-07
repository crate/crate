/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.index.search.MatchQuery;

public enum MultiMatchQueryType {

    /**
     * Uses the best matching boolean field as main score and uses
     * a tie-breaker to adjust the score based on remaining field matches
     */
    BEST_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("best_fields", "boolean")),

    /**
     * Uses the sum of the matching boolean fields to score the query
     */
    MOST_FIELDS(MatchQuery.Type.BOOLEAN, 1.0f, new ParseField("most_fields")),

    /**
     * Uses a blended DocumentFrequency to dynamically combine the queried
     * fields into a single field given the configured analysis is identical.
     * This type uses a tie-breaker to adjust the score based on remaining
     * matches per analyzed terms
     */
    CROSS_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("cross_fields")),

    /**
     * Uses the best matching phrase field as main score and uses
     * a tie-breaker to adjust the score based on remaining field matches
     */
    PHRASE(MatchQuery.Type.PHRASE, 0.0f, new ParseField("phrase")),

    /**
     * Uses the best matching phrase-prefix field as main score and uses
     * a tie-breaker to adjust the score based on remaining field matches
     */
    PHRASE_PREFIX(MatchQuery.Type.PHRASE_PREFIX, 0.0f, new ParseField("phrase_prefix"));

    private MatchQuery.Type matchQueryType;
    private final float tieBreaker;
    private final ParseField parseField;

    MultiMatchQueryType(MatchQuery.Type matchQueryType, float tieBreaker, ParseField parseField) {
        this.matchQueryType = matchQueryType;
        this.tieBreaker = tieBreaker;
        this.parseField = parseField;
    }

    public float tieBreaker() {
        return this.tieBreaker;
    }

    public MatchQuery.Type matchQueryType() {
        return matchQueryType;
    }

    public ParseField parseField() {
        return parseField;
    }

    public static MultiMatchQueryType parse(String value, DeprecationHandler deprecationHandler) {
        MultiMatchQueryType[] values = MultiMatchQueryType.values();
        MultiMatchQueryType type = null;
        for (MultiMatchQueryType t : values) {
            if (t.parseField().match(value, deprecationHandler)) {
                type = t;
                break;
            }
        }
        if (type == null) {
            throw new ElasticsearchParseException("failed to parse [multi_match] query type [{}]. unknown type.", value);
        }
        return type;
    }
}
