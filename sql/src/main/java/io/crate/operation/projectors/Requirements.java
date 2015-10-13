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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class Requirements {

    public final static Set<Requirement> NO_REQUIREMENTS = ImmutableSet.of();

    public static Set<Requirement> add(Set<Requirement> requirements, Requirement additionalRequirement) {
        EnumSet<Requirement> newRequirements = Sets.newEnumSet(requirements, Requirement.class);
        newRequirements.add(additionalRequirement);
        return Collections.unmodifiableSet(newRequirements);
    }

    public static Set<Requirement> remove(Set<Requirement> requirements, Requirement requirementToRemove) {
        EnumSet<Requirement> newRequirements = Sets.newEnumSet(requirements, Requirement.class);
        newRequirements.remove(requirementToRemove);
        return Collections.unmodifiableSet(newRequirements);
    }
}
