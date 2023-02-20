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
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

import com.carrotsearch.hppc.IntObjectHashMap;
import io.crate.planner.operators.LogicalPlan;

/**
 * Memo is used as part of an iterative Optimizer as an in-place
 * Data structure to mutate tree's without rewriting the whole tree.
 *
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

    private final int rootGroup;

    private final IntObjectHashMap<Group> groups = new IntObjectHashMap<>();

    private int nextGroupId = ROOT_GROUP_REF + 1;

    public Memo(LogicalPlan plan) {
        rootGroup = insertRecursive(plan);
        groups.get(rootGroup).incomingReferences.add(ROOT_GROUP_REF);
    }

    public int getRootGroup() {
        return rootGroup;
    }

    /**
     * Returns the {@LogicalPlan} referenced by the given group id.
     *
     * @param group group id
     * @return {@LogicalPlan} for the group id, throws an {@IllegalStateException}
     * if the group does not exist
     */
    public LogicalPlan resolve(int group) {
        return group(group).membership;
    }

    /**
     * Returns the  {@LogicalPlan} referenced by the given GroupReference.
     *
     * @param groupReference
     * @return {@LogicalPlan} for the {@GroupReference}, throws an {@IllegalStateException}
     * ff no {@LogicalPlan} exists
     */
    public LogicalPlan resolve(GroupReference groupReference) {
        return resolve(groupReference.groupId());
    }

    /**
     * Returns the full operator tree of {@LogicalPlan}s all {@GroupReference}s resolved.
     *
     * @return {@LogicalPlan}
     */
    public LogicalPlan extract() {
        return extract(resolve(rootGroup));
    }

    private LogicalPlan extract(LogicalPlan node) {
        return resolveGroupReferences(node, this::resolve);
    }

    private LogicalPlan resolve(LogicalPlan node) {
        if (node instanceof GroupReference groupRef) {
            return resolve(groupRef);
        }
        throw new IllegalStateException("Node is not a GroupReference");
    }

    private Group group(int group) {
        if (!groups.containsKey(group)) {
            throw new IllegalStateException("Group not found");
        }
        return groups.get(group);
    }

    /**
     * Replaces the previous {@LogicalPlan} for a given group id with the new {@LogicalPlan}
     *
     * @param groupId exisiting group id
     * @param node A {@LogicalPlan} which will be added to the group
     * @return the {@LogicalPlan} where to group is updated to
     */
    public LogicalPlan replace(int groupId, LogicalPlan node) {
        Group group = group(groupId);
        final LogicalPlan old = group.membership;

        if (node instanceof GroupReference groupRef) {
            node = resolve(groupRef.groupId());
        } else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node, groupId);
        group.membership = node;
        decrementReferenceCounts(old, groupId);
        return node;
    }

    private void incrementReferenceCounts(LogicalPlan fromNode, int fromGroup) {
        Set<Integer> references = allReferences(fromNode);
        for (int group : references) {
            groups.get(group).incomingReferences.add(fromGroup);
        }
    }

    private void decrementReferenceCounts(LogicalPlan fromNode, Integer fromGroup) {
        Set<Integer> references = allReferences(fromNode);

        for (int group : references) {
            Group childGroup = groups.get(group);
            if (!childGroup.incomingReferences.remove(fromGroup)) {
                throw new IllegalStateException("Reference to remove not found");
            }

            if (childGroup.incomingReferences.isEmpty()) {
                deleteGroup(group);
            }
        }
    }

    private Set<Integer> allReferences(LogicalPlan node) {
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

        groups.put(group, new Group(rewritten));
        incrementReferenceCounts(rewritten, group);

        return group;
    }

    private int nextGroupId() {
        return nextGroupId++;
    }

    public int groupCount() {
        return groups.size();
    }

    private static final class Group {

        private LogicalPlan membership;
        private final List<Integer> incomingReferences = new ArrayList<>();

        private Group(LogicalPlan member) {
            this.membership = requireNonNull(member, "member is null");
        }
    }

    private LogicalPlan resolveGroupReferences(LogicalPlan node, Function<LogicalPlan, LogicalPlan> resolvePlan) {
        requireNonNull(node, "node is null");
        return node.accept(new ResolvingVisitor(resolvePlan), null);
    }
}

