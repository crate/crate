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

package org.elasticsearch.search.profile.query;

import java.util.List;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.ProfileResult;

/**
 * This class acts as a thread-local storage for profiling a query.  It also
 * builds a representation of the query tree which is built constructed
 * "online" as the weights are wrapped by ContextIndexSearcher.  This allows us
 * to know the relationship between nodes in tree without explicitly
 * walking the tree or pre-wrapping everything
 *
 * A Profiler is associated with every Search, not per Search-Request. E.g. a
 * request may execute two searches (query + global agg).  A Profiler just
 * represents one of those
 */
public final class QueryProfiler {

    private final QueryProfileTree profileTree;

    public QueryProfiler() {
        this.profileTree = new QueryProfileTree();
    }

    /**
     * Get the {@link AbstractProfileBreakdown} for the given element in the
     * tree, potentially creating it if it did not exist.
     */
    public QueryProfileBreakdown getQueryBreakdown(Query query) {
        return profileTree.getProfileBreakdown(query);
    }

    /**
     * Removes the last (e.g. most recent) element on the stack.
     */
    public void pollLastElement() {
        profileTree.pollLast();
    }

    /**
     * @return a hierarchical representation of the profiled tree
     */
    public List<ProfileResult> getTree() {
        return profileTree.getTree();
    }
}
