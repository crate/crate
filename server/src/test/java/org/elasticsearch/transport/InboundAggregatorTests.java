/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Predicate;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class InboundAggregatorTests extends ESTestCase {

    private final String unBreakableAction = "non_breakable_action";
    private final String unknownAction = "unknown_action";
    private InboundAggregator aggregator;
    private TestCircuitBreaker circuitBreaker;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Predicate<String> requestCanTripBreaker = action -> {
            if (unknownAction.equals(action)) {
                throw new ActionNotFoundTransportException(action);
            } else {
                return unBreakableAction.equals(action) == false;
            }
        };
        circuitBreaker = new TestCircuitBreaker();
        aggregator = new InboundAggregator(() -> circuitBreaker, requestCanTripBreaker);
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.bwcNeedsToReadVariableHeader = false;
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes.slice(0, 3));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes.slice(3, 3));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
            final ReleasableBytesReference content3 = ReleasableBytesReference.wrap(bytes.slice(6, 4));
            references.add(content3);
            aggregator.aggregate(content3);
            content3.close();
        }

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated).isNotNull();
        assertThat(aggregated.isPing()).isFalse();
        assertThat(aggregated.getHeader().isRequest()).isTrue();
        assertThat(aggregated.getHeader().getRequestId()).isEqualTo(requestId);
        assertThat(aggregated.getHeader().getVersion()).isEqualTo(Version.CURRENT);
        for (ReleasableBytesReference reference : references) {
            assertThat(reference.refCount()).isEqualTo(1);
        }
        aggregated.close();
        for (ReleasableBytesReference reference : references) {
            assertThat(reference.refCount()).isEqualTo(0);
        }
    }

    public void testInboundUnknownAction() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.bwcNeedsToReadVariableHeader = false;
        header.actionName = unknownAction;
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content);
        content.close();
        assertThat(content.refCount()).isEqualTo(0);

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated).isNotNull();
        assertThat(aggregated.isShortCircuit()).isTrue();
        assertThat(aggregated.getException()).isExactlyInstanceOf(ActionNotFoundTransportException.class);
        assertThat(aggregated.takeBreakerReleaseControl()).isNotNull();
    }

    public void testCircuitBreak() throws IOException {
        circuitBreaker.startBreaking();
        // Actions are breakable
        Header breakableHeader = new Header(randomInt(), randomNonNegativeLong(), TransportStatus.setRequest((byte) 0), Version.CURRENT);
        breakableHeader.bwcNeedsToReadVariableHeader = false;
        breakableHeader.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(breakableHeader);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content1);
        content1.close();

        // Signal EOS
        InboundMessage aggregated1 = aggregator.finishAggregation();

        assertThat(content1.refCount()).isEqualTo(0);
        assertThat(aggregated1).isNotNull();
        assertThat(aggregated1.isShortCircuit()).isTrue();
        assertThat(aggregated1.getException()).isExactlyInstanceOf(CircuitBreakingException.class);

        // Actions marked as unbreakable are not broken
        Header unbreakableHeader = new Header(randomInt(), randomNonNegativeLong(), TransportStatus.setRequest((byte) 0), Version.CURRENT);
        unbreakableHeader.bwcNeedsToReadVariableHeader = false;
        unbreakableHeader.actionName = unBreakableAction;
        // Initiate Message
        aggregator.headerReceived(unbreakableHeader);

        final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content2);
        content2.close();

        // Signal EOS
        InboundMessage aggregated2 = aggregator.finishAggregation();

        assertThat(content2.refCount()).isEqualTo(1);
        assertThat(aggregated2).isNotNull();
        assertThat(aggregated2.isShortCircuit()).isFalse();

        // Handshakes are not broken
        final byte handshakeStatus = TransportStatus.setHandshake(TransportStatus.setRequest((byte) 0));
        Header handshakeHeader = new Header(randomInt(), randomNonNegativeLong(), handshakeStatus, Version.CURRENT);
        handshakeHeader.bwcNeedsToReadVariableHeader = false;
        handshakeHeader.actionName = "handshake";
        // Initiate Message
        aggregator.headerReceived(handshakeHeader);

        final ReleasableBytesReference content3 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content3);
        content3.close();

        // Signal EOS
        InboundMessage aggregated3 = aggregator.finishAggregation();

        assertThat(content3.refCount()).isEqualTo(1);
        assertThat(aggregated3).isNotNull();
        assertThat(aggregated3.isShortCircuit()).isFalse();
    }

    public void testCloseWillCloseContent() {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.bwcNeedsToReadVariableHeader = false;
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes.slice(0, 5));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes.slice(5, 5));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
        }

        aggregator.close();

        for (ReleasableBytesReference reference : references) {
            assertThat(reference.refCount()).isEqualTo(0);
        }
    }

    public void testFinishAggregationWillFinishHeader() throws IOException {
        long requestId = randomNonNegativeLong();
        final String actionName;
        final boolean unknownAction = randomBoolean();
        if (unknownAction) {
            actionName = this.unknownAction;
        } else {
            actionName = "action_name";
        }
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        // Initiate Message
        aggregator.headerReceived(header);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            ThreadContext.bwcWriteHeaders(streamOutput);
            streamOutput.writeString(actionName);
            streamOutput.write(randomByteArrayOfLength(10));

            final ReleasableBytesReference content = ReleasableBytesReference.wrap(streamOutput.bytes());
            aggregator.aggregate(content);
            content.close();

            // Signal EOS
            InboundMessage aggregated = aggregator.finishAggregation();

            assertThat(aggregated).isNotNull();
            assertThat(header.needsToReadVariableHeader()).isFalse();
            assertThat(header.getActionName()).isEqualTo(actionName);
            if (unknownAction) {
                assertThat(content.refCount()).isEqualTo(0);
                assertThat(aggregated.isShortCircuit()).isTrue();
            } else {
                assertThat(content.refCount()).isEqualTo(1);
                assertThat(aggregated.isShortCircuit()).isFalse();
            }
        }
    }

}
