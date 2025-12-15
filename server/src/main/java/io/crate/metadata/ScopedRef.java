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

package io.crate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.SymbolType;

public interface ScopedRef extends UnscopedRef {

    RelationName relation();

    ScopedRef withRelation(RelationName relation);

    static void toStream(StreamOutput out, ScopedRef ref) throws IOException {
        out.writeVInt(ref.symbolType().ordinal());
        ref.writeTo(out);
    }

    @SuppressWarnings("unchecked")
    static <T extends ScopedRef> T fromStream(StreamInput in) throws IOException {
        return (T) SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }

    /**
     * Builds a hierarchy for an object column(s) out of the flat structure.
     *
     * @param references must contain all path members of each leaf node
     * to make sure that leaf references are reachable from the root.
     *
     * @return tree represented by sort of "adjacency list".
     * We identify references by FQN and store tree as a map(ident -> list(reference)).
     * NULL node is a root which is an entry point for any traversing method utilizing the tree.
     */
    static HashMap<ColumnIdent, List<ScopedRef>> buildTree(Iterable<ScopedRef> references) {
        HashMap<ColumnIdent, List<ScopedRef>> tree = new LinkedHashMap<>();
        for (ScopedRef treeNode: references) {
            // To build an "adjacency list" we add each edge only once, thus we add only direct neighbor node (parent).
            // I.e if a leaf node C has path A-B we add only (B,C) edge when handling node C.
            // Edge (A,B) will be added later when processing node B:
            // we have a requirement to contain all path nodes in the flat list, so it's guaranteed that we will process B at some point and add (B,A).
            List<ScopedRef> siblings = tree.computeIfAbsent(treeNode.column().getParent(), k -> new ArrayList<>()); // When parent is null we are adding a root.
            siblings.add(treeNode); // Every node is added only once, no duplicates in the list.
        }
        return tree;
    }
}
