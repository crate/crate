/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class MatchOptionsAnalysis {

    static final Predicate<Object> POSITIVE_NUMBER = new Predicate<Object>() {
        @Override
        public boolean apply(@Nullable Object input) {
            return input != null && input instanceof Number && ((Number) input).doubleValue() > 0;
        }
    };
    static final Predicate<Object> IS_STRING = Predicates.instanceOf(String.class);
    static final Predicate<Object> IS_NUMBER = Predicates.instanceOf(Number.class);
    static final Predicate<Object> NUMBER_OR_STRING = Predicates.or(IS_STRING, IS_NUMBER);

    static final Map<String, Predicate<Object>> ALLOWED_SETTINGS = ImmutableMap.<String, Predicate<Object>>builder()
            .put("analyzer", IS_STRING)
            .put("boost", POSITIVE_NUMBER)
            .put("cutoff_frequency", POSITIVE_NUMBER)
            .put("fuzziness", NUMBER_OR_STRING) // validated at ES QueryParser
            .put("fuzzy_rewrite", IS_STRING)
            .put("max_expansions", POSITIVE_NUMBER)
            .put("minimum_should_match", NUMBER_OR_STRING)
            .put("operator", Predicates.in(Arrays.<Object>asList("or", "and", "OR", "AND")))
            .put("prefix_length", POSITIVE_NUMBER)
            .put("rewrite", IS_STRING)
            .put("slop", POSITIVE_NUMBER)
            .put("tie_breaker", IS_NUMBER)
            .put("zero_terms_query", IS_STRING)
            .build();

    public static Map<String, Object> process(GenericProperties properties, Object[] parameters) {
        Map<String, Object> processed = new HashMap<>(properties.properties().size());
        for (Map.Entry<String, Expression> option : properties.properties().entrySet()) {
            Predicate<Object> validator = ALLOWED_SETTINGS.get(option.getKey());
            if (validator == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "unknown match option '%s'", option.getKey()));
            }
            Object value = ExpressionToObjectVisitor.convert(option.getValue(), parameters);
            if (!validator.apply(value)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "invalid value for option '%s': %s", option.getKey(), value));
            }
            processed.put(option.getKey(), value);
        }
        return processed;
    }
}
