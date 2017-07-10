/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.crate.operation.user.UserLookup;
import io.crate.protocols.postgres.ConnectionProperties;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.Cidrs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


public class HostBasedAuthentication implements Authentication {

    public static final String DEFAULT_AUTH_METHOD = "trust";
    public static final String KEY_USER = "user";
    public static final String KEY_ADDRESS = "address";
    public static final String KEY_METHOD = "method";
    public static final String KEY_PROTOCOL = "protocol";

    enum SSL_OPTIONS {
        REQUIRED("on"),
        NEVER("off"),
        OPTIONAL("optional");

        static final String KEY = "ssl";
        final String VALUE;

        SSL_OPTIONS(String value) {
            this.VALUE = value;
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
    private Map<String, Map<String, String>> hbaConf;
    private boolean enabled;

    private final Map<String, Supplier<AuthenticationMethod>> authMethodRegistry = new HashMap<>();

    @Inject
    public HostBasedAuthentication(Settings settings, UserLookup userLookup) {
        enabled = AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.setting().get(settings);
        hbaConf = convertHbaSettingsToHbaConf(AuthSettings.AUTH_HOST_BASED_CONFIG_SETTING.setting().get(settings));
        authMethodRegistry.put(TrustAuthentication.NAME, () -> new TrustAuthentication(userLookup));
        authMethodRegistry.put(ClientCertAuth.NAME, () -> new ClientCertAuth(userLookup));
    }

    void updateHbaConfig(Map<String, Map<String, String>> hbaMap) {
        hbaConf = hbaMap;
    }

    private Map<String, Map<String, String>> convertHbaSettingsToHbaConf(Settings hbaSetting) {
        if (hbaSetting.isEmpty()) {
            return Collections.emptyMap();
        }

        ImmutableMap.Builder<String, Map<String, String>> hostBasedConf = ImmutableMap.builder();
        for (Map.Entry<String, Settings> entry : hbaSetting.getAsGroups().entrySet()) {
            hostBasedConf.put(entry.getKey(), entry.getValue().getAsMap());
        }
        return hostBasedConf.build();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    @Nullable
    public AuthenticationMethod resolveAuthenticationType(String user, ConnectionProperties connProperties) {
        assert hbaConf != null : "hba configuration is missing";
        Optional<Map.Entry<String, Map<String, String>>> entry = getEntry(user, connProperties);
        if (entry.isPresent()) {
            String methodName = entry.get()
                .getValue()
                .getOrDefault(KEY_METHOD, DEFAULT_AUTH_METHOD);
            Supplier<AuthenticationMethod> supplier = authMethodRegistry.get(methodName);
            if (supplier != null) {
                return supplier.get();
            }
        }
        return null;
    }

    @VisibleForTesting
    Map<String, Map<String, String>> hbaConf() {
        return hbaConf;
    }

    @VisibleForTesting
    Optional<Map.Entry<String, Map<String, String>>> getEntry(String user, ConnectionProperties connectionProperties) {
        if (user == null || connectionProperties == null) {
            return Optional.empty();
        }
        return hbaConf.entrySet().stream()
            .filter(e -> Matchers.isValidUser(e, user))
            .filter(e -> Matchers.isValidAddress(e.getValue().get(KEY_ADDRESS), connectionProperties.address()))
            .filter(e -> Matchers.isValidProtocol(e.getValue().get(KEY_PROTOCOL), connectionProperties.protocol()))
            .filter(e -> Matchers.isValidConnection(e.getValue().get(SSL_OPTIONS.KEY), connectionProperties))
            .findFirst();
    }

    static class Matchers {

        static boolean isValidUser(Map.Entry<String, Map<String, String>> entry, String user) {
            String hbaUser = entry.getValue().get(KEY_USER);
            return hbaUser == null || user.equals(hbaUser);
        }

        static boolean isValidAddress(@Nullable String hbaAddress, InetAddress address) {
            if (hbaAddress == null) {
                // no IP/CIDR --> 0.0.0.0/0 --> match all
                return true;
            }
            int p = hbaAddress.indexOf('/');
            if (p < 0) {
                return InetAddresses.forString(hbaAddress).equals(address);
            }
            long[] minAndMax = Cidrs.cidrMaskToMinMax(hbaAddress);
            int addressAsInt = ipv4AddressToInt(address.getAddress());
            return minAndMax[0] <= addressAsInt && addressAsInt < minAndMax[1];
        }

        static boolean isValidProtocol(String hbaProtocol, Protocol protocol) {
            return hbaProtocol == null || hbaProtocol.equals(protocol.toString());
        }

        static boolean isValidConnection(String hbaConnectionMode, ConnectionProperties connectionProperties) {
            return hbaConnectionMode == null ||
                   hbaConnectionMode.isEmpty() ||
                   hbaConnectionMode.equals(SSL_OPTIONS.OPTIONAL.VALUE) ||
                   (hbaConnectionMode.equals(SSL_OPTIONS.NEVER.VALUE) && !connectionProperties.hasSSL()) ||
                   (hbaConnectionMode.equals(SSL_OPTIONS.REQUIRED.VALUE) && connectionProperties.hasSSL());
        }
    }

    private static int ipv4AddressToInt(byte[] address) {
        int net = 0;
        for (byte a : address) {
            net <<= 8;
            net |= a & 0xFF;
        }
        return net;
    }
}
