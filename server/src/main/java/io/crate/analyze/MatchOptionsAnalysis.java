/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static java.util.Map.entry;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class MatchOptionsAnalysis {

    private static final Predicate<Object> POSITIVE_NUMBER = x -> x instanceof Number number && number.doubleValue() > 0;
    private static final Predicate<Object> IS_STRING = x -> x instanceof String;
    private static final Predicate<Object> IS_NUMBER = x -> x instanceof Number;
    private static final Predicate<Object> NUMBER_OR_STRING = IS_NUMBER.or(IS_STRING);
    private static final Predicate<Object> IS_OPERATOR = Set.of("or", "and", "OR", "AND")::contains;

    private static final Map<String, Predicate<Object>> ALLOWED_SETTINGS = Map.ofEntries(
        entry("analyzer", IS_STRING),
        entry("boost", POSITIVE_NUMBER),
        entry("cutoff_frequency", POSITIVE_NUMBER),
        entry("fuzziness", NUMBER_OR_STRING), // validated at ES QueryParser
        entry("fuzzy_rewrite", IS_STRING),
        entry("max_expansions", POSITIVE_NUMBER),
        entry("minimum_should_match", NUMBER_OR_STRING),
        entry("operator", IS_OPERATOR),
        entry("prefix_length", POSITIVE_NUMBER),
        entry("rewrite", IS_STRING),
        entry("slop", POSITIVE_NUMBER),
        entry("tie_breaker", IS_NUMBER),
        entry("zero_terms_query", IS_STRING)
    );

    public static void validate(Map<String, Object> options) {
        for (Map.Entry<String, Object> e : options.entrySet()) {
            String optionName = e.getKey();
            Predicate<Object> validator = ALLOWED_SETTINGS.get(optionName);
            if (validator == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "unknown match option '%s'", optionName));
            }
            Object value = e.getValue();
            if (!validator.test(value)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid value for option '%s': %s", optionName, value));
            }
        }
    }
}
