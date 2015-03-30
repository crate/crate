/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.distributed;

import com.google.common.base.Optional;
import io.crate.Streamer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributedResultRequestTest {

    @Test
    public void testStreaming() throws Exception {

        DistributedRequestContextManager cm = mock(DistributedRequestContextManager.class);

        Streamer<?>[] streamers = new Streamer[]{DataTypes.STRING.streamer()};
        when(cm.getStreamer((UUID) anyObject())).thenReturn(Optional.of(streamers));

        Object[][] rows = new Object[][]{
                {new BytesRef("ab")},{null},{new BytesRef("cd")}
        };
        UUID uuid = UUID.randomUUID();

        DistributedResultRequest r1 = new DistributedResultRequest(uuid, streamers);
        r1.rows(rows);

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        DistributedResultRequest r2 = new DistributedResultRequest(cm);
        r2.readFrom(in);

        assertEquals(r1.rows().length, r2.rows().length);

        assertThat(r1.rows(), is(r2.rows()));
    }

}
