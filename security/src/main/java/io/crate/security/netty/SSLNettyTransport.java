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

package io.crate.security.netty;

import io.crate.security.util.ConfigConstants;
import io.crate.security.util.SecurityUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.ssl.SslHandler;


public class SSLNettyTransport extends SecureNettyTransport {
	
	private static final int SECURITY_SSL_TRANSPORT_NODE_SESSION_CACHE_SIZE_DEFAULT = 1000; //number of sessions
	private static final int SECURITY_SSL_TRANSPORT_NODE_SESSION_TIMEOUT_DEFAULT = 24 * 60 * 60; //24h in seconds
	
	private Map<String, SSLContext> contextCache = ConcurrentCollections.newConcurrentMap();
	
    @Inject
    public SSLNettyTransport(final Settings settings, final ThreadPool threadPool, final NetworkService networkService,
            final BigArrays bigArrays, final Version version) {
        super(settings, threadPool, networkService, bigArrays, version);
    }

    @Override
    public ChannelPipelineFactory configureClientChannelPipelineFactory() {
        logger.info("Node client configured for SSL");
        return new SSLClientChannelPipelineFactory(this, this.settings);
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory(final String name,
            final Settings settings) {
        logger.info("Node server configured for SSL");
        return new SSLServerChannelPipelineFactory(this, name, settings, this.settings);
    }

    protected class SSLServerChannelPipelineFactory extends SecureServerChannelPipelineFactory {

        private final String keystoreType;
        private final String keystoreFilePath;
        private final String keystorePassword;
        private final boolean needClientAuth;

        private final String truststoreType;
        private final String truststoreFilePath;
        private final String truststorePassword;
        
        private final int sslSessionCacheSize;
        private final int sslSessionTimeout;

        public SSLServerChannelPipelineFactory(final NettyTransport nettyTransport, final String name, final Settings sslsettings,
                final Settings essettings) {
            super(nettyTransport, name, sslsettings);

            keystoreType = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_TYPE, System.getProperty("javax.net.ssl.keyStoreType", "JKS"));
            keystoreFilePath = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH, System.getProperty("javax.net.ssl.keyStore", null));
            keystorePassword = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_PASSWORD, System.getProperty("javax.net.ssl.keyStorePassword", "changeit"));
              
            truststoreType = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, System.getProperty("javax.net.ssl.trustStoreType", "JKS"));
            truststoreFilePath = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH,  System.getProperty("javax.net.ssl.trustStore", null));
            truststorePassword = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_PASSWORD, System.getProperty("javax.net.ssl.trustStorePassword", "changeit"));
            
            sslSessionCacheSize = essettings.getAsInt(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, SECURITY_SSL_TRANSPORT_NODE_SESSION_CACHE_SIZE_DEFAULT);
            sslSessionTimeout = essettings.getAsInt(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, SECURITY_SSL_TRANSPORT_NODE_SESSION_TIMEOUT_DEFAULT);
     
            needClientAuth = essettings.getAsBoolean(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_NEED_CLIENTAUTH, true);
            
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = super.getPipeline();

            //Setup serverside SSL
            final SSLContext serverContext = SSLNettyTransport.this.createSSLContext(keystoreType, keystoreFilePath, keystorePassword, truststoreType, truststoreFilePath, truststorePassword, sslSessionCacheSize, sslSessionTimeout);
            
            final SSLEngine engine = serverContext.createSSLEngine();
            final SSLParameters sslParams = new SSLParameters();
            sslParams.setCipherSuites(SecurityUtil.ENABLED_SSL_CIPHERS);
            sslParams.setProtocols(SecurityUtil.ENABLED_SSL_PROTOCOLS);
            sslParams.setNeedClientAuth(needClientAuth);
            engine.setSSLParameters(sslParams);
            engine.setUseClientMode(false);

            final SslHandler sslHandler = new SslHandler(engine);
            //sslHandler.setEnableRenegotiation(false);
            pipeline.addFirst("ssl_server", sslHandler);

            return pipeline;
        }

    }

    protected static class ClientSslHandler extends SimpleChannelHandler {
        private final SSLContext serverContext;
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResovleHostName;

        private ClientSslHandler(final SSLContext serverContext, final boolean hostnameVerificationEnabled,
                final boolean hostnameVerificationResovleHostName) {
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.hostnameVerificationResovleHostName = hostnameVerificationResovleHostName;
            this.serverContext = serverContext;
        }

        @Override
        public void connectRequested(final ChannelHandlerContext ctx, final ChannelStateEvent event) {
        	
        	//Setup clientside SSL
        	
            SSLEngine engine = null;
            final SSLParameters sslParams = new SSLParameters();
            sslParams.setCipherSuites(SecurityUtil.ENABLED_SSL_CIPHERS);
            sslParams.setProtocols(SecurityUtil.ENABLED_SSL_PROTOCOLS);

            if (hostnameVerificationEnabled) {
                final InetSocketAddress inetSocketAddress = (InetSocketAddress) event.getValue();

                String hostname = null;
                if (hostnameVerificationResovleHostName) {
                    hostname = inetSocketAddress.getHostName();
                } else {
                    hostname = inetSocketAddress.getHostString();
                }

                engine = serverContext.createSSLEngine(hostname, inetSocketAddress.getPort());
                sslParams.setEndpointIdentificationAlgorithm("HTTPS");
            } else {
                engine = serverContext.createSSLEngine();
            }

            engine.setSSLParameters(sslParams);
            engine.setUseClientMode(true);

            final SslHandler sslHandler = new SslHandler(engine);
            sslHandler.setEnableRenegotiation(false);
            ctx.getPipeline().replace(this, "ssl_client", sslHandler);

            ctx.sendDownstream(event);
        }
    }

    protected class SSLClientChannelPipelineFactory extends SecureClientChannelPipelineFactory {

        private final String keystoreType;
        private final String keystoreFilePath;
        private final String keystorePassword;
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResovleHostName;
        private final String truststoreType;
        private final String truststoreFilePath;
        private final String truststorePassword;
        private final int sslSessionCacheSize;
        private final int sslSessionTimeout;

        public SSLClientChannelPipelineFactory(final NettyTransport nettyTransport, final Settings essettings) {
            super(nettyTransport);

            keystoreType = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_TYPE, System.getProperty("javax.net.ssl.keyStoreType", "JKS"));
            keystoreFilePath = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH, System.getProperty("javax.net.ssl.keyStore", null));
            keystorePassword = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_PASSWORD, System.getProperty("javax.net.ssl.keyStorePassword", "changeit"));
              
            truststoreType = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, System.getProperty("javax.net.ssl.trustStoreType", "JKS"));
            truststoreFilePath = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH,  System.getProperty("javax.net.ssl.trustStore", null));
            truststorePassword = essettings.get(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_PASSWORD, System.getProperty("javax.net.ssl.trustStorePassword", "changeit"));
        
            sslSessionCacheSize = essettings.getAsInt(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, SECURITY_SSL_TRANSPORT_NODE_SESSION_CACHE_SIZE_DEFAULT);
            sslSessionTimeout = essettings.getAsInt(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_TYPE, SECURITY_SSL_TRANSPORT_NODE_SESSION_TIMEOUT_DEFAULT);
     
            
            hostnameVerificationEnabled = essettings.getAsBoolean(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_ENCFORCE_HOSTNAME_VERIFICATION,
                    true);
            hostnameVerificationResovleHostName = essettings.getAsBoolean(
                    ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_ENCFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, true);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = super.getPipeline();

            final SSLContext clientContext = SSLNettyTransport.this.createSSLContext(keystoreType, keystoreFilePath, keystorePassword, truststoreType, truststoreFilePath, truststorePassword, sslSessionCacheSize, sslSessionTimeout);
            
            pipeline.addFirst("client_ssl_handler", new ClientSslHandler(clientContext, hostnameVerificationEnabled,
                    hostnameVerificationResovleHostName));

            return pipeline;
        }

    }
    
    private SSLContext createSSLContext(String keystoreType, String keystoreFilePath, String keystorePassword, String truststoreType, String truststoreFilePath, String truststorePassword, int sslSessionCacheSize, int sslSessionTimeout) throws KeyManagementException, KeyStoreException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException {

    	if(StringUtils.isBlank(keystoreFilePath) || StringUtils.isBlank(truststoreFilePath)) {
    		logger.error(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH+" and "+ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH+" must be set if transport ssl is reqested.");
    		throw new IOException(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH+" and "+ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH+" must be set if transport ssl is reqested.");
    	}

    	String contextKey = keystoreFilePath + truststoreFilePath;

    	if(contextCache.containsKey(contextKey)) {
    		return contextCache.get(contextKey);
    	}

    	//## Keystore ##
    	final KeyStore ks = KeyStore.getInstance(keystoreType);
    	ks.load(new FileInputStream(new File(keystoreFilePath)), keystorePassword.toCharArray());
    	KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    	kmf.init(ks, keystorePassword.toCharArray());

    	//## Truststore ##
    	final KeyStore ts = KeyStore.getInstance(truststoreType);
    	ts.load(new FileInputStream(new File(truststoreFilePath)), truststorePassword.toCharArray());
    	TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    	tmf.init(ts);

    	//## SSLContext ##
    	final SSLContext sslContext = SSLContext.getInstance("TLS");
    	sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    	sslContext.getServerSessionContext().setSessionCacheSize(sslSessionCacheSize);
    	sslContext.getServerSessionContext().setSessionTimeout(sslSessionTimeout);

    	contextCache.put(contextKey, sslContext);
    	return sslContext;
    }
}
