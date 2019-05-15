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

package org.elasticsearch.common.settings;

import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.Set;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.util.ArrayUtils;

/**
 * A secure setting.
 *
 * This class allows access to settings from the Elasticsearch keystore.
 */
public abstract class SecureSetting<T> extends Setting<T> {

    /** Determines whether legacy settings with sensitive values should be allowed. */
    private static final boolean ALLOW_INSECURE_SETTINGS = Booleans.parseBoolean(System.getProperty("es.allow_insecure_settings", "true"));

    private static final Set<Property> ALLOWED_PROPERTIES = EnumSet.of(Property.Deprecated);

    private static final Property[] FIXED_PROPERTIES = {
        Property.NodeScope
    };

    private SecureSetting(String key, Property... properties) {
        super(key, (String)null, null, ArrayUtils.concat(properties, FIXED_PROPERTIES, Property.class));
        assert assertAllowedProperties(properties);
    }

    private boolean assertAllowedProperties(Setting.Property... properties) {
        for (Setting.Property property : properties) {
            if (ALLOWED_PROPERTIES.contains(property) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getDefaultRaw(Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    public T getDefault(Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    String innerGetRaw(final Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    public boolean exists(Settings settings) {
        final SecureSettings secureSettings = settings.getSecureSettings();
        return secureSettings != null && secureSettings.getSettingNames().contains(getKey());
    }

    @Override
    public T get(Settings settings) {
        checkDeprecation(settings);
        final SecureSettings secureSettings = settings.getSecureSettings();
        if (secureSettings == null || secureSettings.getSettingNames().contains(getKey()) == false) {
            if (super.exists(settings)) {
                throw new IllegalArgumentException("Setting [" + getKey() + "] is a secure setting" +
                    " and must be stored inside the CrateDB keystore, but was found inside crate.yml");
            }
            return getFallback(settings);
        }
        try {
            return getSecret(secureSettings);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("failed to read secure setting " + getKey(), e);
        }
    }

    /** Returns the secret setting from the keyStoreReader store. */
    abstract T getSecret(SecureSettings secureSettings) throws GeneralSecurityException;

    /** Returns the value from a fallback setting. Returns null if no fallback exists. */
    abstract T getFallback(Settings settings);

    // TODO: override toXContent

    /**
     * Overrides the diff operation to make this a no-op for secure settings as they shouldn't be returned in a diff
     */
    @Override
    public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
    }

    /**
     * A setting which contains a sensitive string, but which for legacy reasons must be found outside secure settings.
     */
    public static Setting<SecureString> insecureString(String name) {
        return new InsecureStringSetting(name);
    }

    private static class InsecureStringSetting extends Setting<SecureString> {
        private final String name;

        private InsecureStringSetting(String name) {
            super(name, "", SecureString::new, Property.Filtered, Property.NodeScope);
            this.name = name;
        }

        @Override
        public SecureString get(Settings settings) {
            if (ALLOW_INSECURE_SETTINGS == false && exists(settings)) {
                throw new IllegalArgumentException("Setting [" + name + "] is insecure, " +
                    "but property [allow_insecure_settings] is not set");
            }
            return super.get(settings);
        }
    }
}
