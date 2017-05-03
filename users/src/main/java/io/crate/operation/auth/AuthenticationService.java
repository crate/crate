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
import io.crate.settings.SharedSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.handler.ipfilter.CIDR4;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


public class AuthenticationService implements Authentication {

    private static final String DEFAULT_AUTH_METHOD = "trust";
    private static final String KEY_USER = "user";
    private static final String KEY_ADDRESS = "address";
    private static final String KEY_METHOD = "method";

    /*
     * The cluster state contains the hbaConf from the setting in this format:
     {
       "<ident>": {
         "address": "<cidr>",
         "method": "auth",
         "user": "<username>"
       },
       ...
     }
     */
    private Map<String, Map<String, String>> hbaConf;
    private boolean enabled;

    private final Map<String, Supplier<AuthenticationMethod>> authMethodRegistry = new HashMap<>();

    @Inject
    AuthenticationService(ClusterService clusterService, Settings settings) {
        enabled = SharedSettings.AUTH_HOST_BASED_ENABLED_SETTING.setting().get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SharedSettings.AUTH_HOST_BASED_ENABLED_SETTING.setting(), (s) -> { enabled = s; });

        updateHbaConfig(SharedSettings.AUTH_HOST_BASED_CONFIG_SETTING.setting().get(settings));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SharedSettings.AUTH_HOST_BASED_CONFIG_SETTING.setting(), this::updateHbaConfig);

        registerAuthMethod(TrustAuthentication.NAME, TrustAuthentication::new);
    }

    private void updateHbaConfig(Settings hbaSetting) {
        updateHbaConfig(convertHbaSettingsToHbaConf(hbaSetting));
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

    void registerAuthMethod(String name, Supplier<AuthenticationMethod> supplier) {
        authMethodRegistry.put(name, supplier);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    @Nullable
    public AuthenticationMethod resolveAuthenticationType(String user, InetAddress address) {
        assert hbaConf != null : "hba configuration is missing";
        Optional<Map.Entry<String, Map<String, String>>> entry = getEntry(user, address);
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
    Optional<Map.Entry<String, Map<String, String>>> getEntry(String user, InetAddress address) {
        if (user == null || address == null) {
            return Optional.empty();
        }
        return hbaConf.entrySet().stream()
            .filter(e -> Matchers.isValidUser(e, user))
            .filter(e -> Matchers.isValidAddress(e, address))
            .findFirst();
    }

    static class Matchers {

        static boolean isValidUser(Map.Entry<String, Map<String, String>> entry, String user) {
            String hbaUser = entry.getValue().get(KEY_USER);
            return hbaUser == null || user.equals(hbaUser);
        }

        static boolean isValidAddress(Map.Entry<String, Map<String, String>> entry, InetAddress address) {
            String hbaAddress = entry.getValue().get(KEY_ADDRESS);
            if (hbaAddress == null) {
                // no IP/CIDR --> 0.0.0.0/0 --> match all
                return true;
            } else if (!hbaAddress.contains("/")) {
                // if IP format --> add 32 mask bits
                hbaAddress += "/32";
            }
            try {
                return CIDR4.newCIDR(hbaAddress).contains(address);
            } catch (UnknownHostException e) {
                // this should not happen because we add the required network mask upfront
            }
            return false;
        }

    }
}
