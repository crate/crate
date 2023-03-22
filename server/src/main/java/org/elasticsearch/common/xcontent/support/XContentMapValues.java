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

package org.elasticsearch.common.xcontent.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.regex.Regex;

import io.crate.common.Booleans;

public class XContentMapValues {


    /**
     * Only keep properties in {@code map} that match the {@code includes} but
     * not the {@code excludes}. An empty list of includes is interpreted as a
     * wildcard while an empty list of excludes does not match anything.
     *
     * If a property matches both an include and an exclude, then the exclude
     * wins.
     *
     * If an object matches, then any of its sub properties are automatically
     * considered as matching as well, both for includes and excludes.
     *
     * Dots in field names are treated as sub objects. So for instance if a
     * document contains {@code a.b} as a property and {@code a} is an include,
     * then {@code a.b} will be kept in the filtered map.
     */
    public static Map<String, Object> filter(Map<String, ?> map, String[] includes, String[] excludes) {
        return filter(includes, excludes).apply(map);
    }

    /**
     * Returns a function that filters a document map based on the given include and exclude rules.
     * @see #filter(Map, String[], String[]) for details
     */
    public static Function<Map<String, ?>, Map<String, Object>> filter(String[] includes, String[] excludes) {
        CharacterRunAutomaton matchAllAutomaton = new CharacterRunAutomaton(Automata.makeAnyString());

        CharacterRunAutomaton include;
        if (includes == null || includes.length == 0) {
            include = matchAllAutomaton;
        } else {
            Automaton includeA = Regex.simpleMatchToAutomaton(includes);
            includeA = makeMatchDotsInFieldNames(includeA);
            include = new CharacterRunAutomaton(includeA);
        }

        Automaton excludeA;
        if (excludes == null || excludes.length == 0) {
            excludeA = Automata.makeEmpty();
        } else {
            excludeA = Regex.simpleMatchToAutomaton(excludes);
            excludeA = makeMatchDotsInFieldNames(excludeA);
        }
        CharacterRunAutomaton exclude = new CharacterRunAutomaton(excludeA);

        // NOTE: We cannot use Operations.minus because of the special case that
        // we want all sub properties to match as soon as an object matches

        return (map) -> filter(map,
            include, 0,
            exclude, 0,
            matchAllAutomaton);
    }

    /** Make matches on objects also match dots in field names.
     *  For instance, if the original simple regex is `foo`, this will translate
     *  it into `foo` OR `foo.*`. */
    private static Automaton makeMatchDotsInFieldNames(Automaton automaton) {
        return Operations.union(
                automaton,
                Operations.concatenate(Arrays.asList(automaton, Automata.makeChar('.'), Automata.makeAnyString())));
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

    private static Map<String, Object> filter(Map<String, ?> map,
            CharacterRunAutomaton includeAutomaton, int initialIncludeState,
            CharacterRunAutomaton excludeAutomaton, int initialExcludeState,
            CharacterRunAutomaton matchAllAutomaton) {
        Map<String, Object> filtered = (map instanceof LinkedHashMap) ? new LinkedHashMap<>() : new HashMap<>();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String key = entry.getKey();

            int includeState = step(includeAutomaton, key, initialIncludeState);
            if (includeState == -1) {
                continue;
            }

            int excludeState = step(excludeAutomaton, key, initialExcludeState);
            if (excludeState != -1 && excludeAutomaton.isAccept(excludeState)) {
                continue;
            }

            Object value = entry.getValue();

            CharacterRunAutomaton subIncludeAutomaton = includeAutomaton;
            int subIncludeState = includeState;
            if (includeAutomaton.isAccept(includeState)) {
                if (excludeState == -1 || excludeAutomaton.step(excludeState, '.') == -1) {
                    // the exclude has no chances to match inner properties
                    filtered.put(key, value);
                    continue;
                } else {
                    // the object matched, so consider that the include matches every inner property
                    // we only care about excludes now
                    subIncludeAutomaton = matchAllAutomaton;
                    subIncludeState = 0;
                }
            }

            if (value instanceof Map) {

                subIncludeState = subIncludeAutomaton.step(subIncludeState, '.');
                if (subIncludeState == -1) {
                    continue;
                }
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }

                Map<String, Object> valueAsMap = (Map<String, Object>) value;
                Map<String, Object> filteredValue = filter(valueAsMap,
                        subIncludeAutomaton, subIncludeState, excludeAutomaton, excludeState, matchAllAutomaton);
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else if (value instanceof Iterable) {

                List<Object> filteredValue = filter((Iterable<?>) value,
                        subIncludeAutomaton, subIncludeState, excludeAutomaton, excludeState, matchAllAutomaton);
                if (filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else {

                // leaf property
                if (includeAutomaton.isAccept(includeState)
                        && (excludeState == -1 || excludeAutomaton.isAccept(excludeState) == false)) {
                    filtered.put(key, value);
                }

            }

        }
        return filtered;
    }

    private static List<Object> filter(Iterable<?> iterable,
            CharacterRunAutomaton includeAutomaton, int initialIncludeState,
            CharacterRunAutomaton excludeAutomaton, int initialExcludeState,
            CharacterRunAutomaton matchAllAutomaton) {
        List<Object> filtered = new ArrayList<>();
        boolean isInclude = includeAutomaton.isAccept(initialIncludeState);
        for (Object value : iterable) {
            if (value instanceof Map) {
                int includeState = includeAutomaton.step(initialIncludeState, '.');
                int excludeState = initialExcludeState;
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }
                Map<String, Object> filteredValue = filter((Map<String, ?>)value,
                        includeAutomaton, includeState, excludeAutomaton, excludeState, matchAllAutomaton);
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (value instanceof Iterable) {
                List<Object> filteredValue = filter((Iterable<?>) value,
                        includeAutomaton, initialIncludeState, excludeAutomaton, initialExcludeState, matchAllAutomaton);
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (isInclude) {
                // #22557: only accept this array value if the key we are on is accepted:
                filtered.add(value);
            }
        }
        return filtered;
    }

    public static String nodeStringValue(Object node, String defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return node.toString();
    }

    public static int nodeIntegerValue(Object node) {
        if (node instanceof Number) {
            return Numbers.toIntExact((Number) node);
        }
        return Integer.parseInt(node.toString());
    }

    public static int nodeIntegerValue(Object node, int defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeIntegerValue(node);
    }

    public static boolean nodeBooleanValue(Object node, String name) {
        try {
            return nodeBooleanValue(node);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Could not convert [" + name + "] to boolean", ex);
        }
    }

    public static boolean nodeBooleanValue(Object node) {
        return Booleans.parseBoolean(node.toString());
    }
}
