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

package io.crate.planner.optimizer.iterative;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanIdAllocator;
import io.crate.planner.operators.LogicalPlanVisitor;

/**
 * Stores a plan in a form that's efficient to mutate locally (i.e. without
 * having to do full ancestor tree rewrites due to plan nodes being immutable).
 * <p>
 * Each node in a plan is placed in a group, and it's children are replaced with
 * symbolic references to the corresponding groups.
 * <p>
 * For example, a plan like:
 * <pre>
 *    A -> B -> C -> D
 *           \> E -> F
 * </pre>
 * would be stored as:
 * <pre>
 * root: G0
 *
 * G0 : { A -> G1 }
 * G1 : { B -> [G2, G3] }
 * G2 : { C -> G4 }
 * G3 : { E -> G5 }
 * G4 : { D }
 * G5 : { F }
 * </pre>
 * Groups are reference-counted, and groups that become unreachable from the root
 * due to mutations in a subtree get garbage-collected.
 */
public class Memo {
    private static final int ROOT_GROUP_REF = 0;

    private final LogicalPlanIdAllocator idAllocator;
    private final int rootGroup;

    private final Map<Integer, Group> groups = new HashMap<>();

    private int nextGroupId = ROOT_GROUP_REF + 1;

    public Memo(LogicalPlanIdAllocator idAllocator, LogicalPlan plan) {
        this.idAllocator = idAllocator;
        rootGroup = insertRecursive(plan);
        groups.get(rootGroup).incomingReferences.add(ROOT_GROUP_REF);
    }

    public int getRootGroup() {
        return rootGroup;
    }

    public LogicalPlan getNode(int group) {
        return getGroup(group).membership;
    }

    public LogicalPlan resolve(GroupReference groupReference) {
        return getNode(groupReference.groupId());
    }

    public LogicalPlan extract() {
        return extract(getNode(rootGroup));
    }

    private LogicalPlan extract(LogicalPlan node) {
        return resolveGroupReferences(node, Lookup.from(planNode -> Stream.of(this.resolve(planNode))));
    }

    private Group getGroup(int group) {
        if (!groups.containsKey(group)) {
            throw new IllegalStateException();
        }
        return groups.get(group);
    }


    public LogicalPlan replace(int groupId, LogicalPlan node, String reason) {
        Group group = getGroup(groupId);
        LogicalPlan old = group.membership;

        if (node instanceof GroupReference) {
            node = getNode(((GroupReference) node).groupId());
        } else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node, groupId);
        group.membership = node;
        decrementReferenceCounts(old, groupId);
        return node;
    }


    private void incrementReferenceCounts(LogicalPlan fromNode, int fromGroup) {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            groups.get(group).incomingReferences.add(fromGroup);
        }
    }

    private void decrementReferenceCounts(LogicalPlan fromNode, Integer fromGroup) {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            Group childGroup = groups.get(group);
            if (!childGroup.incomingReferences.remove(fromGroup)) {
                throw new IllegalStateException("Reference to remove not found");
            }
            ;
            if (childGroup.incomingReferences.isEmpty()) {
                deleteGroup(group);
            }
        }
    }

    private Set<Integer> getAllReferences(LogicalPlan node) {
        return node.sources().stream()
            .map(GroupReference.class::cast)
            .map(GroupReference::groupId)
            .collect(Collectors.toSet());
    }

    private void deleteGroup(int group) {
        LogicalPlan deletedNode = groups.remove(group).membership;
        decrementReferenceCounts(deletedNode, group);
    }

    private LogicalPlan insertChildrenAndRewrite(LogicalPlan node) {
        return node.replaceSources(
            node.sources().stream()
                .map(child -> new GroupReference(
                    idAllocator.nextId(),
                    insertRecursive(child),
                    child.outputs()))
                .collect(Collectors.toList()));
    }

    private int insertRecursive(LogicalPlan node) {
        if (node instanceof GroupReference) {
            return ((GroupReference) node).groupId();
        }

        int group = nextGroupId();
        LogicalPlan rewritten = insertChildrenAndRewrite(node);

        groups.put(group, Group.withMember(rewritten));
        incrementReferenceCounts(rewritten, group);

        return group;
    }

    private int nextGroupId() {
        return nextGroupId++;
    }

    public int getGroupCount() {
        return groups.size();
    }

    private static final class Group {
        static Group withMember(LogicalPlan member) {
            return new Group(member);
        }

        private LogicalPlan membership;
        private final List<Integer> incomingReferences = new ArrayList<>();

        private Group(LogicalPlan member) {
            this.membership = requireNonNull(member, "member is null");
        }
    }

    public static LogicalPlan resolveGroupReferences(LogicalPlan node, Lookup lookup) {
        requireNonNull(node, "node is null");
        return node.accept(new ResolvingVisitor(lookup), null);
    }

    private static class ResolvingVisitor extends LogicalPlanVisitor<Void, LogicalPlan> {
        private final Lookup lookup;

        public ResolvingVisitor(Lookup lookup) {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        public LogicalPlan visitPlan(LogicalPlan node, Void context) {
            List<LogicalPlan> children = node.sources().stream()
                .map(child -> child.accept(this, context))
                .collect(Collectors.toList());

            return node.replaceSources(children);
        }

        @Override
        public LogicalPlan visitGroupReference(GroupReference node, Void context) {
            return lookup.resolveGroup(node).findFirst().get().accept(this, context);
        }
    }
}

