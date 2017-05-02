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
import com.google.common.net.InetAddresses;
import io.crate.settings.SharedSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.handler.ipfilter.CIDR4;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class AuthenticationService implements Authentication {

    private static final String DEFAULT_AUTH_METHOD = "trust";
    private static final String KEY_USER = "user";
    private static final String KEY_ADDRESS = "address";
    private static final String KEY_METHOD = "method";

    private final Map<String, AuthenticationMethod> authMethodRegistry = new HashMap<>();
    private final ClusterService clusterService;


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

    @VisibleForTesting
    protected Map<String, Map<String, String>> hbaConf() {
        return hbaConf;
    }

    @Inject
    AuthenticationService(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        Settings hbaSettings = SharedSettings.AUTH_HOST_BASED_SETTING.setting().get(settings);
        updateHbaConfig(convertHbaSettingsToHbaConf(hbaSettings));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SharedSettings.AUTH_HOST_BASED_SETTING
                    .setting(), this::updateHbaConfig);

    }

    public void updateHbaConfig(Settings hbaSetting) {
        this.hbaConf = convertHbaSettingsToHbaConf(hbaSetting);
    }

    public void updateHbaConfig(Map<String, Map<String, String>> hbaConf) {
        this.hbaConf = hbaConf;
    }

    Map<String, Map<String, String>> convertHbaSettingsToHbaConf(Settings hbaSetting) {
        if (hbaSetting == null || hbaSetting.isEmpty()) {
            return null;
        }

        Map<String, Settings> childSettings = clusterService.getSettings().getGroups(SharedSettings.AUTH_HOST_BASED_SETTING.getKey());
        ImmutableMap.Builder<String, Map<String, String>> hostBasedConf = ImmutableMap.builder();

        for (Map.Entry<String, Settings> entry : childSettings.entrySet()) {
            hostBasedConf.put(entry.getKey(), entry.getValue().getAsMap());
        }
        return hostBasedConf.build();
    }

    void registerAuthMethod(AuthenticationMethod method) {
        authMethodRegistry.put(method.name(), method);
    }

    @Override
    public boolean enabled() {
        return hbaConf != null;
    }

    @Override
    @Nullable
    public AuthenticationMethod resolveAuthenticationType(String user, String address) {
        assert hbaConf != null : "hba configuration is missing";
        Optional<Map.Entry<String, Map<String, String>>> entry = getEntry(user, address);
        if (entry.isPresent()) {
            String methodName = entry.get()
                .getValue()
                .getOrDefault(KEY_METHOD, DEFAULT_AUTH_METHOD);
            return authMethodRegistry.get(methodName);
        }
        return null;
    }

    @VisibleForTesting
    Optional<Map.Entry<String, Map<String, String>>> getEntry(String user, String address) {
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

        static boolean isValidAddress(Map.Entry<String, Map<String, String>> entry, String address) {
            String hbaAddress = entry.getValue().get(KEY_ADDRESS);
            InetAddress inetAddr = InetAddresses.forString(address);
            if (hbaAddress == null) {
                // no IP/CIDR --> 0.0.0.0/0 --> match all
                return true;
            } else if (!hbaAddress.contains("/")) {
                // if IP format --> add 32 mask bits
                hbaAddress += "/32";
            }
            try {
                return CIDR4.newCIDR(hbaAddress).contains(inetAddr);
            } catch (UnknownHostException e) {
                // this should not happen because we add the required network mask upfront
            }
            return false;
        }

    }
}
