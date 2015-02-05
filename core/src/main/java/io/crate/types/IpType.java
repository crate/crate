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

import com.google.common.net.InetAddresses;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;

public class IpType extends StringType {

    public final static int ID = 5;
    public final static IpType INSTANCE = new IpType();
    protected IpType() {}

    @Override
    public int id() {
        return ID;
    }

    @Override
    public BytesRef value(Object value) {
        String strIp;
        if (value instanceof BytesRef) {
            BytesRef ip = (BytesRef) value;
            if(InetAddresses.isInetAddress(ip.utf8ToString())){
                return ip;
            } else {
                throw new IllegalArgumentException("failed to parse ip [" + ip.utf8ToString() + "], not a valid ip address");
            }
        }
        if (value instanceof String) {
            strIp = (String) value;
        } else {
            Long longIp = ((Number)value).longValue();
            strIp = IpFieldMapper.longToIp(longIp);
        }
        return new BytesRef(strIp);
    }

    @Override
    public String getName() {
        return "ip";
    }

    @Override
    public DataType<?> create() {
        return IpType.INSTANCE;
    }
}
