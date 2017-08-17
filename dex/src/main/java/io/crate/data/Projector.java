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

package io.crate.data;

import java.util.function.UnaryOperator;


/**
 * Projects a given upstream {@link BatchIterator} object and returns a batch iterator which follows the semantics of
 * the underlying projection.
 */
public interface Projector extends UnaryOperator<BatchIterator<Row>> {

    /**
     * Indicates that the projector supports scrolling independent of the source iterator.
     * <p>
     * If the projector does not support independent scrolling, the source iterator must provide the scrolling
     * capability on its own
     */
    default boolean providesIndependentScroll() {
        return false;
    }

}
