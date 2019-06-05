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

package io.crate.execution.engine.window;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static org.hamcrest.core.Is.is;

public class UnboundedPrecedingFrameBoundTest extends CrateUnitTest {

    @Test
    public void testStartForFirstFrame() {
        int end = UNBOUNDED_PRECEDING.getStart(0, 3, 0, 1, true, (pos1, pos2) -> true);
        assertThat("the start boundary should always be the start of the partition for the UNBOUNDED PRECEDING frames",
                   end,
                   is(0));
    }

    @Test
    public void testStartForSecondFrame() {
        int end = UNBOUNDED_PRECEDING.getStart(0, 3, 0, 2, true, (pos1, pos2) -> false);
        assertThat("the start boundary should always be the start of the partition for the UNBOUNDED PRECEDING frames",
                   end,
                   is(0));
    }

    @Test
    public void testUnboundePrecedingCannotBeTheEndOfTheFrame() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("UNBOUNDED PRECEDING cannot be the start of a frame");
        UNBOUNDED_PRECEDING.getEnd(0, 3, 0, true, (pos1, pos2) -> 1);
    }

}
