///*
// * Licensed to Crate.io GmbH ("Crate") under one or more contributor
// * license agreements.  See the NOTICE file distributed with this work for
// * additional information regarding copyright ownership.  Crate licenses
// * this file to you under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.  You may
// * obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * However, if you have executed another commercial license agreement
// * with Crate these terms will supersede the license and you may use the
// * software solely pursuant to the terms of the relevant commercial agreement.
// */
//
//package io.crate.planner.optimizer.joinorder;
//
//import java.util.HashMap;
//import java.util.Map;
//
//
///*
// *  Implementation of the join-ordering algorithm DPhyp from
// *  `Dynamic Programming Strikes Back` G. Moerkotte and T. Neumann.
// *  https://15721.courses.cs.cmu.edu/spring2020/papers/20-optimizer2/p539-moerkotte.pdf
// *
// */
//public class DPhyp {
//
//    private final int nodes;
//    private final Graph graph;
//    private final Map<Long, Boolean> dpTable;
//
//    public DPhyp(Graph graph, int nodes) {
//        this.nodes = nodes;
//        this.graph = graph;
//        this.dpTable = new HashMap<>(nodes);
//    }
//
//    public boolean solve() {
//        // Fill in the dpTable with single relation plans
//        for (int i = 1; i <= nodes; i++) {
//            var s = LongBitmap.newBitmap(i);
//            dpTable.put(s, true);
//        }
//        // Process and expand singleton sets
//        for (int i = nodes; i > 0; i--) {
//            var s = LongBitmap.newBitmap(i);
//            emitCsg(s);
//
//            var bv = LongBitmap.newBitmap();
//            for (int j = i; j > 0; j--) {
//                bv = LongBitmap.set(bv, j);
//            }
//            enumerateCsgRec(s, bv);
//        }
//        // Return the entry of the dpTable that corresponds to the full
//        // set of nodes in the graph
//        long v = LongBitmap.newBitmap();
//        for (int i = 1; i <= nodes; i++) {
//            LongBitmap.set(v, i);
//        }
//        return dpTable.get(v);
//    }
//
//    void emitCsg(long s) {
//        long bMin = computeBmin(s);
//        long x = LongBitmap.or(bMin, s);
//        long nS = neighbors(s, x);
//        for (int i = nodes; i > 0; i--) {
//            if (LongBitmap.get(nS, i)) {
//                long s2 = LongBitmap.newBitmap(i);
//                if (containsEdge(s, s2)) {
//                    emitCsgCmp(s, s2);
//                }
//                enumerateCmpRec(s, s2, x);
//            }
//        }
//    }
//
//    void enumerateCsgRec(long s, long x) {
//        long nS = neighbors(s, x);
//        long prev;
//        long next;
//
//        while ((next = nextBitSet(prev, nS)) != nS) {
//            var subset = LongBitmap.newBitmap();
//            subset.or(s);
//            if (dpTable.containsKey(s | next)) {
//                emitCsg(s | next);
//            }
//        }
//
//        prev.clear();
//        while ((next = nextBitSet(prev, nS)) != nS) {
//            enumerateCsgRec(s | next, x | nS);
//        }
//    }
//
//    void emitCsgCmp(long s1, long s2) {
//        // Here we build the pair (s1,s2)and compare the
//        // new plan to what's in the dpTable, if it there
//
//        // In this implementation we don't have a cost function, so we
//        // just fill in the dpTable
//        long joined = 0;
//        joined.or(s1);
//        joined.or(s2);
//
//        // calculate cost here
//        // cost = cost(joined);
//        if (dpTable.get(joined)) {
//            throw new IllegalStateException("Duplicate found");
//        }
//        // check comulative operations
//
//        dpTable.put(joined, true);
//    }
//
//    void enumerateCmpRec(long s1, long s2, long x) {
//        long nS = neighbors(s2, x);
//
//        long prev;
//        long next;
//        while ((next = nextBitSet(prev, nS)) != nS) {
//            if (dpTable.get(s2 | next)) {
//                if (containsEdge(s1, s2 | next)) {
//                    emitCsgCmp(s1, s2 | next);
//                }
//            }
//        }
//        long x2 = x | nS;
//        nS = neighbors(s2, x2);
//        prev.clear();
//        while ((next = nextBitSet(prev, nS)) != nS) {
//            enumerateCmpRec(s1, s2 | next, x2);
//        }
//    }
//
//    long neighbors(long a, long b) {
//        long res;
//
//        // First we need to take care of simple edges, we just add them to the neighborhood
//        for (int i = 1; i <= graph.edges().size(); i++) {
//            if (S[i]) {
//                for (Graph.HyperEdge e : graph.edges().values())) {
//                    if (!X[ * e.to.begin()
//                }])
//                res.set( * e.to.begin());
//            }
//        }
//
//        // Now we fetch edges that result from real hyper-edges
//        std::long < N > F = S | res | X;
//
//        // We iterate over all edges of the graph
//        // TODO: make this more efficient by building and using an index
//        for (const HyperEdge & e :graph.edges){
//            // if the 'from' hypernode is a subset of S and doesn't intersect with forbidden nodes,
//            // add its mininum element to the result
//            if (isSubset(e.from, S) && emptyIntersection(e.to, F)) {
//                int min_el = *e.from.begin();
//                res.set(min_el);
//                for (int el : e.from) {
//                    F.set(el);
//                }
//            }
//            // if the 'to' hypernode is a subset of S and doesn't intersect with forbidden nodes,
//            // add its minimum element to the result
//            if (isSubset(e.to, S) && emptyIntersection(e.from, F)) {
//                int min_el = *e.to.begin();
//                res.set(min_el);
//                for (int el : e.to) {
//                    F.set(el);
//                }
//            }
//        }
//
//        return res;
//    }
//
//    boolean isSubset(long a, long b) {
//        return false;
//    }
//
//    boolean emptyIntersection(long a, long b) {
//        return false;
//    }
//
//    long computeBmin(long s) {
//        long result = LongBitmap.newBitmap();
//        for (int i = 1; i <= nodes; i++) {
//            if (LongBitmap.get(s, i)) {
//                for (int j = 1; j < i; j++) {
//                    result = LongBitmap.set(result, i);
//                }
//                break;
//            }
//        }
//        return result;
//    }
//
//    long nextBitSet(long a, long b) {
//        return null;
//    }
//
//    boolean containsEdge((long a, (long b) {
//        return false;
//    }
//}
