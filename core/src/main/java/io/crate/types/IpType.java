/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.LegacyIpFieldMapper;

import java.util.Locale;

public class IpType extends StringType {

    public static final int ID = 5;
    public static final IpType INSTANCE = new IpType();

    IpType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public BytesRef value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            BytesRef ip = (BytesRef) value;
            validate(ip.utf8ToString());
            return ip;
        }
        if (value instanceof String) {
            validate((String) value);
            return new BytesRef((String) value);
        } else {
            Long longIp = ((Number) value).longValue();
            if (longIp < 0) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Failed to convert long value: %s to ipv4 address)",
                    longIp));
            }
            String strIp = LegacyIpFieldMapper.longToIp(longIp);
            return new BytesRef(strIp);
        }
    }

    private void validate(String ip) {
        if (!InetAddresses.isInetAddress(ip)) {
            throw new IllegalArgumentException(
                "Failed to validate ip [" + ip + "], not a valid ipv4 address");
        }
    }

    @Override
    public String getName() {
        return "ip";
    }

}
