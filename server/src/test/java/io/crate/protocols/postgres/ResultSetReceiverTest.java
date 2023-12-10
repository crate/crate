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

package io.crate.protocols.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.auth.AccessControl;
import io.crate.data.Row1;
import io.crate.protocols.postgres.DelayableWriteChannel.DelayedWrites;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.DataTypes;
import io.netty.channel.Channel;

public class ResultSetReceiverTest {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    @Test
    public void testChannelIsPeriodicallyFlushedToAvoidConsumingTooMuchMemory() {
        Channel channel = mock(Channel.class, Answers.RETURNS_DEEP_STUBS);
        DelayableWriteChannel delayableWriteChannel = new DelayableWriteChannel(channel);
        DelayedWrites delayWrites = delayableWriteChannel.delayWrites();
        ResultSetReceiver resultSetReceiver = new ResultSetReceiver(
            "select * from t",
            delayableWriteChannel,
            delayWrites,
            TransactionState.IDLE,
            AccessControl.DISABLED,
            Collections.singletonList(PGTypes.get(DataTypes.INTEGER)),
            null
        );
        Row1 row1 = new Row1(1);
        for (int i = 0; i < 1500; i++) {
            resultSetReceiver.setNextRow(row1);
        }
        verify(channel, times(1)).flush();
    }
}
