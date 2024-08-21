/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.crate.metadata.doc.DocSysColumns;

public class ReferenceTree {

    private sealed interface Node permits Composite, Leaf {
        Reference ref();
    }

    private record Composite(String name, Map<String, Node> children, Reference ref) implements Node {}

    private record Leaf(Reference ref) implements Node {}

    public static ReferenceTree of(Collection<Reference> references) {
        Composite root = new Composite("_doc", new HashMap<>(), null);
        references.stream().sorted(Comparator.comparing(Reference::column)).forEach(r -> addReference(root, r, r.column()));
        return new ReferenceTree(root);
    }

    private static void addReference(Composite root, Reference ref, ColumnIdent column) {
        if (column.isRoot()) {
            if (root.children.get(column.name()) == null) {
                root.children.put(column.name(), new Leaf(ref));
            }
        } else {
            var childColumn = column.shiftRight();
            assert childColumn != null;
            switch (root.children.get(column.name())) {
                case null -> {
                    var child = new Composite(column.name(), new HashMap<>(), ref);
                    root.children.put(column.name(), child);
                    addReference(child, ref, childColumn);
                }
                case Leaf l -> {
                    var child = new Composite(column.name(), new HashMap<>(), l.ref);
                    root.children.put(column.name(), child);
                    addReference(child, ref, childColumn);
                }
                case Composite c -> addReference(c, ref, childColumn);
            }
        }
    }

    private final Node root;

    private ReferenceTree(Node root) {
        this.root = root;
    }

    public List<Reference> getChildren(Reference parent) {
        Node ref = findRef(parent);
        return switch (ref) {
            case null -> List.of();
            case Leaf _ -> List.of();
            case Composite c -> c.children.values().stream().map(Node::ref).toList();
        };
    }

    public List<Reference> findDescendants(Reference parent) {
        Node ref = findRef(parent);
        if (ref == null) {
            return List.of();
        }
        List<Reference> children = new ArrayList<>();
        visitLeaves(ref, children::add);
        return children;
    }

    public Reference findFirstParentMatching(Reference child, Predicate<Reference> test) {
        Node node = root;
        var col = child.column();
        if (col.name().equals(DocSysColumns.Names.DOC) == false) {
            child = DocReferences.toDocLookup(child);
            col = child.column();
        }
        do {
            switch (node) {
                case Leaf l -> {
                    return test.test(l.ref) ? l.ref : null;
                }
                case Composite c -> {
                    if (Objects.equals(c.name, col.name())) {
                        if (c.ref != null && test.test(c.ref)) {
                            return c.ref;
                        }
                        col = col.shiftRight();
                        if (col == null) {
                            return null;
                        }
                        node = c.children.get(col.name());
                    } else {
                        return null;
                    }
                }
            }
        } while (node != null);
        return null;
    }

    private static void visitLeaves(Node node, Consumer<Reference> visitor) {
        switch (node) {
            case Composite c -> {
                c.children.forEach((_, v) -> visitLeaves(v, visitor));
            }
            case Leaf l -> visitor.accept(l.ref);
        }
    }

    private Node findRef(Reference ref) {
        if (Objects.equals(DocSysColumns.Names.DOC, ref.column().name())) {
            if (ref.column().isRoot()) {
                return root;
            }
            return findRef(root, ref.column().shiftRight());
        }
        return findRef(root, ref.column());
    }

    private static Node findRef(Node root, ColumnIdent columnIdent) {
        switch (root) {
            case Composite c -> {
                if (columnIdent.isRoot() && Objects.equals(c.name, columnIdent.name())) {
                    return c;
                }
                if (c.children.containsKey(columnIdent.name())) {
                    return columnIdent.isRoot()
                        ? c.children.get(columnIdent.name())
                        : findRef(c.children.get(columnIdent.name()), columnIdent.shiftRight());
                }
                return null;
            }
            case Leaf l -> {
                return l;
            }
        }
    }

}
