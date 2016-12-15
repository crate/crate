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
package io.crate.bp;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 */
public class Netty3Utils {

    /**
     * Turns the given BytesReference into a ChannelBuffer. Note: the returned ChannelBuffer will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ChannelBuffer goes out of scope.
     */
    @SuppressForbidden(reason="This will make upgrading to ES 5.0 easier")
    public static ChannelBuffer toChannelBuffer(BytesReference reference) {
        return reference.toChannelBuffer();
    }
}
