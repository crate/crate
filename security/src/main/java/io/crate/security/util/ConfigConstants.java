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

package io.crate.security.util;

public final class ConfigConstants {

    public static final String SECURITY_SSL_TRANSPORT_NODE_ENABLED = "security.ssl.transport.node.enabled";
    public static final String SECURITY_SSL_TRANSPORT_NODE_ENCFORCE_HOSTNAME_VERIFICATION = "security.ssl.transport.node.hostname_verification.enabled";
    public static final String SECURITY_SSL_TRANSPORT_NODE_ENCFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME = "security.ssl.transport.node.hostname_verification.resolve_host_name";
    public static final String SECURITY_SSL_TRANSPORT_NODE_NEED_CLIENTAUTH = "security.ssl.transport.node.need_clientauth";
    public static final String SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH = "security.ssl.transport.node.keystore.path";
    public static final String SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_PASSWORD = "security.ssl.transport.node.keystore.password";
    public static final String SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_TYPE = "security.ssl.transport.node.keystore.type";
    public static final String SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH = "security.ssl.transport.node.truststore.path";
    public static final String SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_PASSWORD = "security.ssl.transport.node.truststore.password";
    public static final String SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE = "security.ssl.transport.node.truststore.type";
    public static final String SECURITY_SSL_TRANSPORT_NODE_SESSION_CACHE_SIZE = "security.ssl.transport.node.session.cache_size";
    public static final String SECURITY_SSL_TRANSPORT_NODE_SESSION_TIMEOUT = "security.ssl.transport.node.session.timeout";

    private ConfigConstants() {

    }

}
