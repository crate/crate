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

package io.crate.operation.reference.sys.snapshot;

import com.google.common.collect.ImmutableMap;
import io.crate.operation.reference.sys.repositories.SysRepositoriesService;
import io.crate.operation.reference.sys.repositories.SysRepository;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysSnapshotsTest extends CrateUnitTest {

    @Test
    public void testErrorsOnRetrievingSnapshotsAreIgnored() throws Exception {
        SysRepositoriesService sysRepos = mock(SysRepositoriesService.class);

        final Iterable<?> objects =
            Collections.singletonList((Object) new SysRepository("foo", "url", ImmutableMap.<String, Object>of()));
        when(sysRepos.get()).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return objects;
            }
        });

        SnapshotsService snapshotService = mock(SnapshotsService.class);
        when(snapshotService.snapshots(anyString(), anyList(), anyBoolean())).thenThrow(new IllegalStateException("dummy"));
        SysSnapshots sysSnapshots = new SysSnapshots(sysRepos, snapshotService, mock(RepositoriesService.class));
        assertThat(sysSnapshots.get().iterator().hasNext(), is(false));
    }
}
