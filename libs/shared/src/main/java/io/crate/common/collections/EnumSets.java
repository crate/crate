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

package io.crate.common.collections;

import java.util.EnumSet;
import java.util.Set;

public class EnumSets {

    /**
     * Pack an {@link EnumSet} into one integer
     */
    public static <E extends Enum<E>> int packToInt(Set<E> set) {
        assert set.size() < 32 : "EnumSet must be smaller than 32";
        int i = 0;
        for (E element : set) {
            i |= (1 << element.ordinal());
        }
        return i;
    }

    /**
     * Unpack an {@link EnumSet} from a given integer and element type.
     */
    public static <E extends Enum<E>> EnumSet<E> unpackFromInt(int i, Class<E> enumType) {
        EnumSet<E> result = EnumSet.noneOf(enumType);
        for (E element : enumType.getEnumConstants()) {
            if ((i & (1 << element.ordinal())) != 0) {
                result.add(element);
            }
        }
        return result;
    }
}
