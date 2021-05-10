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

package io.crate.monitor;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Locale;

public class ExtendedNetworkInfo {

    public static final Interface NA_INTERFACE = new Interface("", "");

    private final Interface primary;

    public ExtendedNetworkInfo(Interface primary) {
        this.primary = primary;
    }

    public Interface primaryInterface() {
        return primary;
    }

    static Interface iface() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                byte[] addr = iface.getHardwareAddress();
                if (iface.isUp() && addr != null) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < addr.length; i++) {
                        sb.append(String.format(Locale.ROOT, "%02x", addr[i]));
                        if (i < addr.length - 1) {
                            sb.append("-");
                        }
                    }
                    return new Interface(iface.getName(), sb.toString().toLowerCase(Locale.ROOT));
                }
            }
        } catch (SocketException e) {
            // pass
        }
        return NA_INTERFACE;
    }

    public static class Interface {

        private final String name;
        private final String macAddress;

        Interface(String name, String macAddress) {
            this.name = name;
            this.macAddress = macAddress;
        }

        public String name() {
            return name;
        }

        public String macAddress() {
            return macAddress;
        }
    }
}
