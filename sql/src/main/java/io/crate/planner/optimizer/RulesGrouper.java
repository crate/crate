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

package io.crate.planner.optimizer;

import com.google.common.reflect.TypeToken;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.matcher.TypeOfPattern;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

class RulesGrouper {

    private final Map<Class<?>, List<Rule<?>>> rulesByRootType;

    private RulesGrouper(Map<Class<?>, List<Rule<?>>> rulesByRootType) {
        this.rulesByRootType = Map.copyOf(rulesByRootType);
    }

    Stream<Rule<?>> getCandidates(Object object) {
        return supertypes(object.getClass())
            .map(rulesByRootType::get)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream);
    }

    private static Stream<Class<?>> supertypes(Class<?> type) {
        return TypeToken.of(type).getTypes().stream()
            .map(TypeToken::getRawType);
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private final Map<Class<?>, List<Rule<?>>> rulesByRootType = new HashMap<>();

        Builder registerAll(List<Rule<?>> rules) {
            for (Rule<?> rule : rules) {
                register(rule);
            }
            return this;
        }

        Builder register(Rule<?> rule) {
            Pattern pattern = getFirstPattern(rule.pattern());
            if (pattern instanceof TypeOfPattern<?>) {
                Class<?> patternType = ((TypeOfPattern<?>) pattern).expectedClass();
                rulesByRootType
                    .computeIfAbsent(patternType, k -> new ArrayList<>())
                    .add(rule);
            } else {
                throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
            }
            return this;
        }

        private Pattern<?> getFirstPattern(Pattern<?> pattern) {
            while (pattern.previous() != null) {
                pattern = pattern.previous();
            }
            return pattern;
        }

        RulesGrouper build() {
            return new RulesGrouper(rulesByRootType);
        }
    }
}
