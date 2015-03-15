/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package io.crate.security.plugin;


import io.crate.security.netty.SSLNettyTransport;
import io.crate.security.netty.UnexpectedSecurityException;
import io.crate.security.util.ConfigConstants;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

public class SecurityPlugin extends AbstractPlugin {

	private static final String TRANSPORT_TYPE = "transport.type";
    private final Settings settings;

    public SecurityPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "security";
    }

    public String description() {
        return "plugin that adds SECURITY support to crate";
    }
    
    @Override
    public Settings additionalSettings() {
    	ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
    	if(settings.getAsBoolean(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_ENABLED, false)) 
    	{
    		String keystoreFilePath = settings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH, System.getProperty("javax.net.ssl.keyStore", null));
    		String truststoreFilePath = settings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH,  System.getProperty("javax.net.ssl.trustStore", null));

    		if(StringUtils.isBlank(keystoreFilePath) || StringUtils.isBlank(truststoreFilePath)) {
    			throw new UnexpectedSecurityException(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH+" and "+ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH+" must be set if transport ssl is reqested.");
    		}

    		settingsBuilder.put(TRANSPORT_TYPE, SSLNettyTransport.class);
    	}
    	return settingsBuilder.build();
    }
}