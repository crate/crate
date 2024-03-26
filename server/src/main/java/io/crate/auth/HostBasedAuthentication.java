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

package io.crate.auth;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.role.Roles;
import io.crate.protocols.postgres.ConnectionProperties;
import org.apache.http.conn.DnsResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.Cidrs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;

import org.jetbrains.annotations.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;


public class HostBasedAuthentication implements Authentication {

    private static final Logger LOGGER = LogManager.getLogger(HostBasedAuthentication.class);

    private static final String DEFAULT_AUTH_METHOD = "trust";
    private static final String KEY_USER = "user";
    private static final String KEY_ADDRESS = "address";
    private static final String KEY_METHOD = "method";
    private static final String KEY_PROTOCOL = "protocol";

    enum SSL {
        REQUIRED("on"),
        NEVER("off"),
        OPTIONAL("optional");

        static final String KEY = "ssl";
        final String VALUE;

        SSL(String value) {
            this.VALUE = value;
        }

        static SSL parseValue(String value) {
            return switch (value.toLowerCase(Locale.ENGLISH)) {
                // allow true/false as well because YAML `on` is interpreted as true
                case "on" -> REQUIRED;
                case "true" -> REQUIRED;
                case "off" -> NEVER;
                case "false" -> NEVER;
                case "optional" -> OPTIONAL;
                default -> throw new IllegalArgumentException(value + " is not a valid HBA SSL setting");
            };
        }
    }

    /*
     * The cluster state contains the hbaConf from the setting in this format:
     {
       "<ident>": {
         "address": "<cidr>",
         "method": "auth",
         "user": "<username>"
         "protocol": "pg"
       },
       ...
     }
     */
    private SortedMap<String, Map<String, String>> hbaConf;
    private final Roles roles;
    private final DnsResolver dnsResolver;

    private final Supplier<String> clusterId;

    public HostBasedAuthentication(Settings settings,
                                   Roles roles,
                                   DnsResolver dnsResolver,
                                   Supplier<String> clusterId) {
        hbaConf = convertHbaSettingsToHbaConf(settings);
        this.roles = roles;
        this.dnsResolver = dnsResolver;
        this.clusterId = clusterId;
    }

    @VisibleForTesting
    SortedMap<String, Map<String, String>> convertHbaSettingsToHbaConf(Settings settings) {
        Settings hbaSettings = AuthSettings.AUTH_HOST_BASED_CONFIG_SETTING.get(settings);
        SortedMap<String, Map<String, String>> hostBasedConf = new TreeMap<>();
        for (Map.Entry<String, Settings> entry : hbaSettings.getAsGroups().entrySet()) {
            Settings hbaEntry = entry.getValue();
            HashMap<String, String> map = new HashMap<>(hbaEntry.size());
            for (String name : hbaEntry.keySet()) {
                map.put(name, hbaEntry.get(name));
            }
            if (JWTAuthenticationMethod.NAME.equals(map.get("method"))) {
                if (Protocol.HTTP.toString().equals(map.get("protocol")) == false) {
                    throw new IllegalArgumentException("protocol must be set to http when using jwt auth method");
                }
            }
            hostBasedConf.put(entry.getKey(), map);
        }
        return Collections.unmodifiableSortedMap(hostBasedConf);
    }

    @Nullable
    private AuthenticationMethod methodForName(String method) {
        return switch (method) {
            case (TrustAuthenticationMethod.NAME) -> new TrustAuthenticationMethod(roles);
            case (ClientCertAuth.NAME) -> new ClientCertAuth(roles);
            case (PasswordAuthenticationMethod.NAME) -> new PasswordAuthenticationMethod(roles);
            case (JWTAuthenticationMethod.NAME) ->
                new JWTAuthenticationMethod(
                    roles,
                    JWTAuthenticationMethod::jwkProvider,
                    clusterId
                );
            default -> null;
        };
    }

    @Override
    @Nullable
    public AuthenticationMethod resolveAuthenticationType(@Nullable String user, ConnectionProperties connProperties) {
        assert hbaConf != null : "hba configuration is missing";
        Optional<Map.Entry<String, Map<String, String>>> entry = getEntry(user, connProperties);
        if (entry.isPresent()) {
            String methodName = entry.get()
                .getValue()
                .getOrDefault(KEY_METHOD, DEFAULT_AUTH_METHOD);
            return methodForName(methodName);
        }
        return null;
    }

    @VisibleForTesting
    Map<String, Map<String, String>> hbaConf() {
        return hbaConf;
    }

    @VisibleForTesting
    Optional<Map.Entry<String, Map<String, String>>> getEntry(@Nullable String user, ConnectionProperties connectionProperties) {
        if (user == null || connectionProperties == null) {
            return Optional.empty();
        }
        return hbaConf.entrySet().stream()
            .filter(e -> Matchers.isValidUser(e, user))
            .filter(e -> Matchers.isValidAddress(e.getValue().get(KEY_ADDRESS), connectionProperties.address(), dnsResolver))
            .filter(e -> Matchers.isValidProtocol(e.getValue().get(KEY_PROTOCOL), connectionProperties.protocol()))
            .filter(e -> Matchers.isValidConnection(e.getValue().get(SSL.KEY), connectionProperties))
            .findFirst();
    }

    static class Matchers {

        // IPv4 127.0.0.1 -> 2130706433
        private static final long IPV4_LOCALHOST = inetAddressToInt(InetAddresses.forString("127.0.0.1"));
        // IPv6 ::1 -> 1
        private static final long IPV6_LOCALHOST = inetAddressToInt(InetAddresses.forString("::1"));

        static boolean isValidUser(Map.Entry<String, Map<String, String>> entry, String user) {
            String hbaUser = entry.getValue().get(KEY_USER);
            return hbaUser == null || user.equals(hbaUser);
        }

        static boolean isValidAddress(@Nullable String hbaAddressOrHostname, long address, Supplier<String> getHostname, DnsResolver resolver) {
            if (hbaAddressOrHostname == null) {
                // no IP/CIDR --> 0.0.0.0/0 --> match all
                return true;
            }
            if (hbaAddressOrHostname.equals("_local_")) {
                // special case "_local_" which matches both IPv4 and IPv6 localhost addresses
                return address == IPV4_LOCALHOST || address == IPV6_LOCALHOST;
            }
            int p = hbaAddressOrHostname.indexOf('/');
            if (p < 0) {
                try {
                    if (hbaAddressOrHostname.startsWith(".")) {
                        // not an ip address, subdomain
                        var clientHostName = getHostname.get();
                        return clientHostName != null ? clientHostName.endsWith(hbaAddressOrHostname) : false;
                    } else {
                        // SystemDefaultDnsResolver is injected here and internally it uses InetAddress.getAllByName
                        // which tries to treat argument as an ip address and then as a hostname.
                        for (var resolvedAddress : resolver.resolve(hbaAddressOrHostname)) {
                            if (inetAddressToInt(resolvedAddress) == address) {
                                return true;
                            }
                        }
                        return false;
                    }
                } catch (UnknownHostException e) {
                    LOGGER.warn("Cannot resolve hostname {} specified in the HBA configuration.", hbaAddressOrHostname);
                    return false;
                }
            }
            long[] minAndMax = Cidrs.cidrMaskToMinMax(hbaAddressOrHostname);
            return minAndMax[0] <= address && address < minAndMax[1];
        }

        static boolean isValidAddress(@Nullable String hbaAddressOrHostname, InetAddress address, DnsResolver resolver) {
            return isValidAddress(hbaAddressOrHostname, inetAddressToInt(address), address::getCanonicalHostName, resolver);
        }

        static boolean isValidProtocol(String hbaProtocol, Protocol protocol) {
            return hbaProtocol == null || hbaProtocol.equals(protocol.toString());
        }

        static boolean isValidConnection(String hbaConnectionMode, ConnectionProperties connectionProperties) {
            if (hbaConnectionMode == null || hbaConnectionMode.isEmpty()) {
                return true;
            }
            SSL sslMode = SSL.parseValue(hbaConnectionMode);
            return switch (sslMode) {
                case OPTIONAL -> true;
                case NEVER -> !connectionProperties.hasSSL();
                case REQUIRED -> connectionProperties.hasSSL();
            };
        }

        static long inetAddressToInt(InetAddress address) {
            long net = 0;
            for (byte a : address.getAddress()) {
                net <<= 8;
                net |= a & 0xFF;
            }
            return net;
        }
    }
}
