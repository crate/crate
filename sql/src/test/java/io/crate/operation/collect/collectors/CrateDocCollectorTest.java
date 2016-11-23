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

package io.crate.operation.collect.collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.operation.Input;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.junit.Test;
import org.mockito.Answers;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CrateDocCollectorTest {

    @Test
    public void testCollectorKill() throws Exception {
        CrateSearchContext sc = mock(CrateSearchContext.class, Answers.RETURNS_MOCKS.get());
        RowReceiver rowReceiver = mock(RowReceiver.class, Answers.RETURNS_MOCKS.get());
        CrateDocCollector c = new CrateDocCollector(sc, MoreExecutors.directExecutor(), false, null,
            rowReceiver, ImmutableList.<Input<?>>of(), ImmutableList.<LuceneCollectorExpression<?>>of(), (byte) 0);

        c.kill(null);

        verify(rowReceiver, only()).kill(any(Throwable.class));
    }
}
