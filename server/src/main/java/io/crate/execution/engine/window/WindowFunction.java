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

package io.crate.execution.engine.window;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.FunctionImplementation;

public interface WindowFunction extends FunctionImplementation {

    /**
     * Computes the window function for the row identified by the provided {@param rowIdx}.
     * This method should be called sequentially for all the rows in a window, with each's row corresponding window
     * frame state {@link WindowFrameState}.
     *  @param rowIdx       the 0-indexed id of the current partition
     * @param currentFrame the frame the row identified by {@param rowIdx} is part of.
     * @param ignoreNulls
     */
    Object execute(int rowIdx,
                   WindowFrameState currentFrame,
                   List<? extends CollectExpression<Row, ?>> expressions,
                   @Nullable Boolean ignoreNulls,
                   Input<?> ... args);
}
