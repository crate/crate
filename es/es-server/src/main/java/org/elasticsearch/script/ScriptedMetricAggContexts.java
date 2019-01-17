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

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptedMetricAggContexts {

    public abstract static class InitScript {
        private static final Map<String, String> DEPRECATIONS;
        static {
            Map<String, String> deprecations = new HashMap<>();
            deprecations.put(
                "_agg",
                "Accessing variable [_agg] via [params._agg] from within a scripted metric agg init script " +
                    "is deprecated in favor of using [state]."
            );
            DEPRECATIONS = Collections.unmodifiableMap(deprecations);
        }

        private final Map<String, Object> params;
        private final Object state;

        public InitScript(Map<String, Object> params, Object state) {
            this.params = new ParameterMap(params, DEPRECATIONS);
            this.state = state;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public Object getState() {
            return state;
        }

        public abstract void execute();

        public interface Factory {
            InitScript newInstance(Map<String, Object> params, Object state);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_init", Factory.class);
    }

    public abstract static class MapScript {
        private static final Map<String, String> DEPRECATIONS;

        static {
            Map<String, String> deprecations = new HashMap<>();
            deprecations.put(
                "doc",
                "Accessing variable [doc] via [params.doc] from within a scripted metric agg map script " +
                    "is deprecated in favor of directly accessing [doc]."
            );
            deprecations.put(
                "_doc",
                "Accessing variable [doc] via [params._doc] from within a scripted metric agg map script " +
                    "is deprecated in favor of directly accessing [doc]."
            );
            deprecations.put(
                "_agg",
                "Accessing variable [_agg] via [params._agg] from within a scripted metric agg map script " +
                    "is deprecated in favor of using [state]."
            );
            DEPRECATIONS = Collections.unmodifiableMap(deprecations);
        }

        private final Map<String, Object> params;
        private final Object state;
        private final LeafSearchLookup leafLookup;
        private Scorer scorer;

        public MapScript(Map<String, Object> params, Object state, SearchLookup lookup, LeafReaderContext leafContext) {
            this.state = state;
            this.leafLookup = leafContext == null ? null : lookup.getLeafSearchLookup(leafContext);
            if (leafLookup != null) {
                params = new HashMap<>(params); // copy params so we aren't modifying input
                params.putAll(leafLookup.asMap()); // add lookup vars
                params = new ParameterMap(params, DEPRECATIONS); // wrap with deprecations
            }
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public Object getState() {
            return state;
        }

        // Return the doc as a map (instead of LeafDocLookup) in order to abide by type whitelisting rules for
        // Painless scripts.
        public Map<String, ScriptDocValues<?>> getDoc() {
            return leafLookup == null ? null : leafLookup.doc();
        }

        public void setDocument(int docId) {
            if (leafLookup != null) {
                leafLookup.setDocument(docId);
            }
        }

        public void setScorer(Scorer scorer) {
            this.scorer = scorer;
        }

        // get_score() is named this way so that it's picked up by Painless as '_score'
        public double get_score() {
            if (scorer == null) {
                return 0.0;
            }

            try {
                return scorer.score();
            } catch (IOException e) {
                throw new ElasticsearchException("Couldn't look up score", e);
            }
        }

        public abstract void execute();

        public interface LeafFactory {
            MapScript newInstance(LeafReaderContext ctx);
        }

        public interface Factory {
            LeafFactory newFactory(Map<String, Object> params, Object state, SearchLookup lookup);
        }

        public static String[] PARAMETERS = new String[] {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_map", Factory.class);
    }

    public abstract static class CombineScript {
        private static final Map<String, String> DEPRECATIONS;
        static {
            Map<String, String> deprecations = new HashMap<>();
            deprecations.put(
                "_agg",
                "Accessing variable [_agg] via [params._agg] from within a scripted metric agg combine script " +
                    "is deprecated in favor of using [state]."
            );
            DEPRECATIONS = Collections.unmodifiableMap(deprecations);
        }

        private final Map<String, Object> params;
        private final Object state;

        public CombineScript(Map<String, Object> params, Object state) {
            this.params = new ParameterMap(params, DEPRECATIONS);
            this.state = state;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public Object getState() {
            return state;
        }

        public abstract Object execute();

        public interface Factory {
            CombineScript newInstance(Map<String, Object> params, Object state);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_combine", Factory.class);
    }

    public abstract static class ReduceScript {
        private static final Map<String, String> DEPRECATIONS;
        static {
            Map<String, String> deprecations = new HashMap<>();
            deprecations.put(
                "_aggs",
                "Accessing variable [_aggs] via [params._aggs] from within a scripted metric agg reduce script " +
                    "is deprecated in favor of using [state]."
            );
            DEPRECATIONS = Collections.unmodifiableMap(deprecations);
        }

        private final Map<String, Object> params;
        private final List<Object> states;

        public ReduceScript(Map<String, Object> params, List<Object> states) {
            this.params = new ParameterMap(params, DEPRECATIONS);
            this.states = states;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public List<Object> getStates() {
            return states;
        }

        public abstract Object execute();

        public interface Factory {
            ReduceScript newInstance(Map<String, Object> params, List<Object> states);
        }

        public static String[] PARAMETERS = {};
        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs_reduce", Factory.class);
    }
}
