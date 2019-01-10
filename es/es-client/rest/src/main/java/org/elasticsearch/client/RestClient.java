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
package org.elasticsearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.DeadHostState.TimeSupplier;

import javax.net.ssl.SSLHandshakeException;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;

/**
 * Client that connects to an Elasticsearch cluster through HTTP.
 * <p>
 * Must be created using {@link RestClientBuilder}, which allows to set all the different options or just rely on defaults.
 * The hosts that are part of the cluster need to be provided at creation time, but can also be replaced later
 * by calling {@link #setNodes(Collection)}.
 * <p>
 * The method {@link #performRequest(String, String, Map, HttpEntity, Header...)} allows to send a request to the cluster. When
 * sending a request, a host gets selected out of the provided ones in a round-robin fashion. Failing hosts are marked dead and
 * retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously
 * failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead nodes that
 * deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
 * <p>
 * Requests can be either synchronous or asynchronous. The asynchronous variants all end with {@code Async}.
 * <p>
 * Requests can be traced by enabling trace logging for "tracer". The trace logger outputs requests and responses in curl format.
 */
public class RestClient implements Closeable {

    private static final Log logger = LogFactory.getLog(RestClient.class);

    private final CloseableHttpAsyncClient client;
    // We don't rely on default headers supported by HttpAsyncClient as those cannot be replaced.
    // These are package private for tests.
    final List<Header> defaultHeaders;
    private final long maxRetryTimeoutMillis;
    private final String pathPrefix;
    private final AtomicInteger lastNodeIndex = new AtomicInteger(0);
    private final ConcurrentMap<HttpHost, DeadHostState> blacklist = new ConcurrentHashMap<>();
    private final FailureListener failureListener;
    private final NodeSelector nodeSelector;
    private volatile NodeTuple<List<Node>> nodeTuple;
    private final boolean strictDeprecationMode;

    RestClient(CloseableHttpAsyncClient client, long maxRetryTimeoutMillis, Header[] defaultHeaders, List<Node> nodes, String pathPrefix,
            FailureListener failureListener, NodeSelector nodeSelector, boolean strictDeprecationMode) {
        this.client = client;
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        this.defaultHeaders = Collections.unmodifiableList(Arrays.asList(defaultHeaders));
        this.failureListener = failureListener;
        this.pathPrefix = pathPrefix;
        this.nodeSelector = nodeSelector;
        this.strictDeprecationMode = strictDeprecationMode;
        setNodes(nodes);
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the hosts that the client will send requests to.
     * <p>
     * Prefer this to {@link #builder(HttpHost...)} if you have metadata up front about the nodes.
     * If you don't either one is fine.
     */
    public static RestClientBuilder builder(Node... nodes) {
        return new RestClientBuilder(nodes == null ? null : Arrays.asList(nodes));
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     * <p>
     * You can use this if you do not have metadata up front about the nodes. If you do, prefer
     * {@link #builder(Node...)}.
     * @see Node#Node(HttpHost)
     */
    public static RestClientBuilder builder(HttpHost... hosts) {
        return new RestClientBuilder(hostsToNodes(hosts));
    }

    /**
     * Replaces the hosts with which the client communicates.
     *
     * @deprecated prefer {@link #setNodes(Collection)} because it allows you
     * to set metadata for use with {@link NodeSelector}s
     */
    @Deprecated
    public void setHosts(HttpHost... hosts) {
        setNodes(hostsToNodes(hosts));
    }

    /**
     * Replaces the nodes with which the client communicates.
     */
    public synchronized void setNodes(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        AuthCache authCache = new BasicAuthCache();

        Map<HttpHost, Node> nodesByHost = new LinkedHashMap<>();
        for (Node node : nodes) {
            Objects.requireNonNull(node, "node cannot be null");
            // TODO should we throw an IAE if we have two nodes with the same host?
            nodesByHost.put(node.getHost(), node);
            authCache.put(node.getHost(), new BasicScheme());
        }
        this.nodeTuple = new NodeTuple<>(
                Collections.unmodifiableList(new ArrayList<>(nodesByHost.values())), authCache);
        this.blacklist.clear();
    }

    private static List<Node> hostsToNodes(HttpHost[] hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null nor empty");
        }
        List<Node> nodes = new ArrayList<>(hosts.length);
        for (HttpHost host : hosts) {
            nodes.add(new Node(host));
        }
        return nodes;
    }

    /**
     * Get the list of nodes that the client knows about. The list is
     * unmodifiable.
     */
    public List<Node> getNodes() {
        return nodeTuple.nodes;
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to.
     * Blocks until the request is completed and returns its response or fails
     * by throwing an exception. Selects a host out of the provided ones in a
     * round-robin fashion. Failing hosts are marked dead and retried after a
     * certain amount of time (minimum 1 minute, maximum 30 minutes), depending
     * on how many times they previously failed (the more failures, the later
     * they will be retried). In case of failures all of the alive nodes (or
     * dead nodes that deserve a retry) are retried until one responds or none
     * of them does, in which case an {@link IOException} will be thrown.
     *
     * This method works by performing an asynchronous call and waiting
     * for the result. If the asynchronous call throws an exception we wrap
     * it and rethrow it so that the stack trace attached to the exception
     * contains the call site. While we attempt to preserve the original
     * exception this isn't always possible and likely haven't covered all of
     * the cases. You can get the original exception from
     * {@link Exception#getCause()}.
     *
     * @param request the request to perform
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     */
    public Response performRequest(Request request) throws IOException {
        SyncResponseListener listener = new SyncResponseListener(maxRetryTimeoutMillis);
        performRequestAsyncNoCatch(request, listener);
        return listener.get();
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to.
     * The request is executed asynchronously and the provided
     * {@link ResponseListener} gets notified upon request completion or
     * failure. Selects a host out of the provided ones in a round-robin
     * fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how
     * many times they previously failed (the more failures, the later they
     * will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of
     * them does, in which case an {@link IOException} will be thrown.
     *
     * @param request the request to perform
     * @param responseListener the {@link ResponseListener} to notify when the
     *      request is completed or fails
     */
    public void performRequestAsync(Request request, ResponseListener responseListener) {
        try {
            performRequestAsyncNoCatch(request, responseListener);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without parameters
     * and request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     * @deprecated prefer {@link #performRequest(Request)}
     */
    @Deprecated
    public Response performRequest(String method, String endpoint, Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        addHeaders(request, headers);
        return performRequest(request);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     * @deprecated prefer {@link #performRequest(Request)}
     */
    @Deprecated
    public Response performRequest(String method, String endpoint, Map<String, String> params, Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        addParameters(request, params);
        addHeaders(request, headers);
        return performRequest(request);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to and waits for the corresponding response
     * to be returned. Shortcut to {@link #performRequest(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, Header...)}
     * which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     * @deprecated prefer {@link #performRequest(Request)}
     */
    @Deprecated
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        addParameters(request, params);
        request.setEntity(entity);
        addHeaders(request, headers);
        return performRequest(request);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Blocks until the request is completed and returns
     * its response or fails by throwing an exception. Selects a host out of the provided ones in a round-robin fashion. Failing hosts
     * are marked dead and retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times
     * they previously failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * This method works by performing an asynchronous call and waiting
     * for the result. If the asynchronous call throws an exception we wrap
     * it and rethrow it so that the stack trace attached to the exception
     * contains the call site. While we attempt to preserve the original
     * exception this isn't always possible and likely haven't covered all of
     * the cases. You can get the original exception from
     * {@link Exception#getCause()}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param headers the optional request headers
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status code that indicated an error
     * @deprecated prefer {@link #performRequest(Request)}
     */
    @Deprecated
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                   HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                   Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        addParameters(request, params);
        request.setEntity(entity);
        setOptions(request, httpAsyncResponseConsumerFactory, headers);
        return performRequest(request);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without parameters and  request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     * @deprecated prefer {@link #performRequestAsync(Request, ResponseListener)}
     */
    @Deprecated
    public void performRequestAsync(String method, String endpoint, ResponseListener responseListener, Header... headers) {
        Request request;
        try {
            request = new Request(method, endpoint);
            addHeaders(request, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
            return;
        }
        performRequestAsync(request, responseListener);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure. Shortcut to
     * {@link #performRequestAsync(String, String, Map, HttpEntity, ResponseListener, Header...)} but without request body.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     * @deprecated prefer {@link #performRequestAsync(Request, ResponseListener)}
     */
    @Deprecated
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    ResponseListener responseListener, Header... headers) {
        Request request;
        try {
            request = new Request(method, endpoint);
            addParameters(request, params);
            addHeaders(request, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
            return;
        }
        performRequestAsync(request, responseListener);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. Doesn't wait for the response, instead
     * the provided {@link ResponseListener} will be notified upon completion or failure.
     * Shortcut to {@link #performRequestAsync(String, String, Map, HttpEntity, HttpAsyncResponseConsumerFactory, ResponseListener,
     * Header...)} which doesn't require specifying an {@link HttpAsyncResponseConsumerFactory} instance,
     * {@link HttpAsyncResponseConsumerFactory} will be used to create the needed instances of {@link HttpAsyncResponseConsumer}.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     * @deprecated prefer {@link #performRequestAsync(Request, ResponseListener)}
     */
    @Deprecated
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, ResponseListener responseListener, Header... headers) {
        Request request;
        try {
            request = new Request(method, endpoint);
            addParameters(request, params);
            request.setEntity(entity);
            addHeaders(request, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
            return;
        }
        performRequestAsync(request, responseListener);
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to. The request is executed asynchronously
     * and the provided {@link ResponseListener} gets notified upon request completion or failure.
     * Selects a host out of the provided ones in a round-robin fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously failed (the more failures,
     * the later they will be retried). In case of failures all of the alive nodes (or dead nodes that deserve a retry) are retried
     * until one responds or none of them does, in which case an {@link IOException} will be thrown.
     *
     * @param method the http method
     * @param endpoint the path of the request (without host and port)
     * @param params the query_string parameters
     * @param entity the body of the request, null if not applicable
     * @param httpAsyncResponseConsumerFactory the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the response body gets streamed from a non-blocking HTTP
     * connection on the client side.
     * @param responseListener the {@link ResponseListener} to notify when the request is completed or fails
     * @param headers the optional request headers
     * @deprecated prefer {@link #performRequestAsync(Request, ResponseListener)}
     */
    @Deprecated
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                    HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                    ResponseListener responseListener, Header... headers) {
        Request request;
        try {
            request = new Request(method, endpoint);
            addParameters(request, params);
            request.setEntity(entity);
            setOptions(request, httpAsyncResponseConsumerFactory, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
            return;
        }
        performRequestAsync(request, responseListener);
    }

    void performRequestAsyncNoCatch(Request request, ResponseListener listener) throws IOException {
        Map<String, String> requestParams = new HashMap<>(request.getParameters());
        //ignore is a special parameter supported by the clients, shouldn't be sent to es
        String ignoreString = requestParams.remove("ignore");
        Set<Integer> ignoreErrorCodes;
        if (ignoreString == null) {
            if (HttpHead.METHOD_NAME.equals(request.getMethod())) {
                //404 never causes error if returned for a HEAD request
                ignoreErrorCodes = Collections.singleton(404);
            } else {
                ignoreErrorCodes = Collections.emptySet();
            }
        } else {
            String[] ignoresArray = ignoreString.split(",");
            ignoreErrorCodes = new HashSet<>();
            if (HttpHead.METHOD_NAME.equals(request.getMethod())) {
                //404 never causes error if returned for a HEAD request
                ignoreErrorCodes.add(404);
            }
            for (String ignoreCode : ignoresArray) {
                try {
                    ignoreErrorCodes.add(Integer.valueOf(ignoreCode));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead", e);
                }
            }
        }
        URI uri = buildUri(pathPrefix, request.getEndpoint(), requestParams);
        HttpRequestBase httpRequest = createHttpRequest(request.getMethod(), uri, request.getEntity());
        setHeaders(httpRequest, request.getOptions().getHeaders());
        FailureTrackingResponseListener failureTrackingResponseListener = new FailureTrackingResponseListener(listener);
        long startTime = System.nanoTime();
        performRequestAsync(startTime, nextNode(), httpRequest, ignoreErrorCodes,
                request.getOptions().getHttpAsyncResponseConsumerFactory(), failureTrackingResponseListener);
    }

    private void performRequestAsync(final long startTime, final NodeTuple<Iterator<Node>> nodeTuple, final HttpRequestBase request,
                                     final Set<Integer> ignoreErrorCodes,
                                     final HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                     final FailureTrackingResponseListener listener) {
        final Node node = nodeTuple.nodes.next();
        //we stream the request body if the entity allows for it
        final HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(node.getHost(), request);
        final HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer =
            httpAsyncResponseConsumerFactory.createHttpAsyncResponseConsumer();
        final HttpClientContext context = HttpClientContext.create();
        context.setAuthCache(nodeTuple.authCache);
        client.execute(requestProducer, asyncResponseConsumer, context, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    RequestLogger.logResponse(logger, request, node.getHost(), httpResponse);
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    Response response = new Response(request.getRequestLine(), node.getHost(), httpResponse);
                    if (isSuccessfulResponse(statusCode) || ignoreErrorCodes.contains(response.getStatusLine().getStatusCode())) {
                        onResponse(node);
                        if (strictDeprecationMode && response.hasWarnings()) {
                            listener.onDefinitiveFailure(new ResponseException(response));
                        } else {
                            listener.onSuccess(response);
                        }
                    } else {
                        ResponseException responseException = new ResponseException(response);
                        if (isRetryStatus(statusCode)) {
                            //mark host dead and retry against next one
                            onFailure(node);
                            retryIfPossible(responseException);
                        } else {
                            //mark host alive and don't retry, as the error should be a request problem
                            onResponse(node);
                            listener.onDefinitiveFailure(responseException);
                        }
                    }
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            @Override
            public void failed(Exception failure) {
                try {
                    RequestLogger.logFailedRequest(logger, request, node, failure);
                    onFailure(node);
                    retryIfPossible(failure);
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            private void retryIfPossible(Exception exception) {
                if (nodeTuple.nodes.hasNext()) {
                    //in case we are retrying, check whether maxRetryTimeout has been reached
                    long timeElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    long timeout = maxRetryTimeoutMillis - timeElapsedMillis;
                    if (timeout <= 0) {
                        IOException retryTimeoutException = new IOException(
                                "request retries exceeded max retry timeout [" + maxRetryTimeoutMillis + "]");
                        listener.onDefinitiveFailure(retryTimeoutException);
                    } else {
                        listener.trackFailure(exception);
                        request.reset();
                        performRequestAsync(startTime, nodeTuple, request, ignoreErrorCodes, httpAsyncResponseConsumerFactory, listener);
                    }
                } else {
                    listener.onDefinitiveFailure(exception);
                }
            }

            @Override
            public void cancelled() {
                listener.onDefinitiveFailure(new ExecutionException("request was cancelled", null));
            }
        });
    }

    private void setHeaders(HttpRequest httpRequest, Collection<Header> requestHeaders) {
        // request headers override default headers, so we don't add default headers if they exist as request headers
        final Set<String> requestNames = new HashSet<>(requestHeaders.size());
        for (Header requestHeader : requestHeaders) {
            httpRequest.addHeader(requestHeader);
            requestNames.add(requestHeader.getName());
        }
        for (Header defaultHeader : defaultHeaders) {
            if (requestNames.contains(defaultHeader.getName()) == false) {
                httpRequest.addHeader(defaultHeader);
            }
        }
    }

    /**
     * Returns a non-empty {@link Iterator} of nodes to be used for a request
     * that match the {@link NodeSelector}.
     * <p>
     * If there are no living nodes that match the {@link NodeSelector}
     * this will return the dead node that matches the {@link NodeSelector}
     * that is closest to being revived.
     * @throws IOException if no nodes are available
     */
    private NodeTuple<Iterator<Node>> nextNode() throws IOException {
        NodeTuple<List<Node>> nodeTuple = this.nodeTuple;
        Iterable<Node> hosts = selectNodes(nodeTuple, blacklist, lastNodeIndex, nodeSelector);
        return new NodeTuple<>(hosts.iterator(), nodeTuple.authCache);
    }

    /**
     * Select nodes to try and sorts them so that the first one will be tried initially, then the following ones
     * if the previous attempt failed and so on. Package private for testing.
     */
    static Iterable<Node> selectNodes(NodeTuple<List<Node>> nodeTuple, Map<HttpHost, DeadHostState> blacklist,
                                      AtomicInteger lastNodeIndex, NodeSelector nodeSelector) throws IOException {
        /*
         * Sort the nodes into living and dead lists.
         */
        List<Node> livingNodes = new ArrayList<>(nodeTuple.nodes.size() - blacklist.size());
        List<DeadNode> deadNodes = new ArrayList<>(blacklist.size());
        for (Node node : nodeTuple.nodes) {
            DeadHostState deadness = blacklist.get(node.getHost());
            if (deadness == null) {
                livingNodes.add(node);
                continue;
            }
            if (deadness.shallBeRetried()) {
                livingNodes.add(node);
                continue;
            }
            deadNodes.add(new DeadNode(node, deadness));
        }

        if (false == livingNodes.isEmpty()) {
            /*
             * Normal state: there is at least one living node. If the
             * selector is ok with any over the living nodes then use them
             * for the request.
             */
            List<Node> selectedLivingNodes = new ArrayList<>(livingNodes);
            nodeSelector.select(selectedLivingNodes);
            if (false == selectedLivingNodes.isEmpty()) {
                /*
                 * Rotate the list using a global counter as the distance so subsequent
                 * requests will try the nodes in a different order.
                 */
                Collections.rotate(selectedLivingNodes, lastNodeIndex.getAndIncrement());
                return selectedLivingNodes;
            }
        }

        /*
         * Last resort: there are no good nodes to use, either because
         * the selector rejected all the living nodes or because there aren't
         * any living ones. Either way, we want to revive a single dead node
         * that the NodeSelectors are OK with. We do this by passing the dead
         * nodes through the NodeSelector so it can have its say in which nodes
         * are ok. If the selector is ok with any of the nodes then we will take
         * the one in the list that has the lowest revival time and try it.
         */
        if (false == deadNodes.isEmpty()) {
            final List<DeadNode> selectedDeadNodes = new ArrayList<>(deadNodes);
            /*
             * We'd like NodeSelectors to remove items directly from deadNodes
             * so we can find the minimum after it is filtered without having
             * to compare many things. This saves us a sort on the unfiltered
             * list.
             */
            nodeSelector.select(new Iterable<Node>() {
                @Override
                public Iterator<Node> iterator() {
                    return new DeadNodeIteratorAdapter(selectedDeadNodes.iterator());
                }
            });
            if (false == selectedDeadNodes.isEmpty()) {
                return singletonList(Collections.min(selectedDeadNodes).node);
            }
        }
        throw new IOException("NodeSelector [" + nodeSelector + "] rejected all nodes, "
                + "living " + livingNodes + " and dead " + deadNodes);
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the host that was used for the successful request.
     */
    private void onResponse(Node node) {
        DeadHostState removedHost = this.blacklist.remove(node.getHost());
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed [" + node + "] from blacklist");
        }
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the host that was used for the failed attempt.
     */
    private void onFailure(Node node) {
        while(true) {
            DeadHostState previousDeadHostState =
                blacklist.putIfAbsent(node.getHost(), new DeadHostState(TimeSupplier.DEFAULT));
            if (previousDeadHostState == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("added [" + node + "] to blacklist");
                }
                break;
            }
            if (blacklist.replace(node.getHost(), previousDeadHostState,
                    new DeadHostState(previousDeadHostState))) {
                if (logger.isDebugEnabled()) {
                    logger.debug("updated [" + node + "] already in blacklist");
                }
                break;
            }
        }
        failureListener.onFailure(node);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static boolean isSuccessfulResponse(int statusCode) {
        return statusCode < 300;
    }

    private static boolean isRetryStatus(int statusCode) {
        switch(statusCode) {
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
    }

    private static Exception addSuppressedException(Exception suppressedException, Exception currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
        return currentException;
    }

    private static HttpRequestBase createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch(method.toUpperCase(Locale.ROOT)) {
            case HttpDeleteWithEntity.METHOD_NAME:
                return addRequestBody(new HttpDeleteWithEntity(uri), entity);
            case HttpGetWithEntity.METHOD_NAME:
                return addRequestBody(new HttpGetWithEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    private static HttpRequestBase addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase)httpRequest).setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest.getMethod() + " with body is not supported");
            }
        }
        return httpRequest;
    }

    static URI buildUri(String pathPrefix, String path, Map<String, String> params) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            String fullPath;
            if (pathPrefix != null && pathPrefix.isEmpty() == false) {
                if (pathPrefix.endsWith("/") && path.startsWith("/")) {
                    fullPath = pathPrefix.substring(0, pathPrefix.length() - 1) + path;
                } else if (pathPrefix.endsWith("/") || path.startsWith("/")) {
                    fullPath = pathPrefix + path;
                } else {
                    fullPath = pathPrefix + "/" + path;
                }
            } else {
                fullPath = path;
            }

            URIBuilder uriBuilder = new URIBuilder(fullPath);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Listener used in any async call to wrap the provided user listener (or SyncResponseListener in sync calls).
     * Allows to track potential failures coming from the different retry attempts and returning to the original listener
     * only when we got a response (successful or not to be retried) or there are no hosts to retry against.
     */
    static class FailureTrackingResponseListener {
        private final ResponseListener responseListener;
        private volatile Exception exception;

        FailureTrackingResponseListener(ResponseListener responseListener) {
            this.responseListener = responseListener;
        }

        /**
         * Notifies the caller of a response through the wrapped listener
         */
        void onSuccess(Response response) {
            responseListener.onSuccess(response);
        }

        /**
         * Tracks one last definitive failure and returns to the caller by notifying the wrapped listener
         */
        void onDefinitiveFailure(Exception exception) {
            trackFailure(exception);
            responseListener.onFailure(this.exception);
        }

        /**
         * Tracks an exception, which caused a retry hence we should not return yet to the caller
         */
        void trackFailure(Exception exception) {
            this.exception = addSuppressedException(this.exception, exception);
        }
    }

    /**
     * Listener used in any sync performRequest calls, it waits for a response or an exception back up to a timeout
     */
    static class SyncResponseListener implements ResponseListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        private final long timeout;

        SyncResponseListener(long timeout) {
            assert timeout > 0;
            this.timeout = timeout;
        }

        @Override
        public void onSuccess(Response response) {
            Objects.requireNonNull(response, "response must not be null");
            boolean wasResponseNull = this.response.compareAndSet(null, response);
            if (wasResponseNull == false) {
                throw new IllegalStateException("response is already set");
            }

            latch.countDown();
        }

        @Override
        public void onFailure(Exception exception) {
            Objects.requireNonNull(exception, "exception must not be null");
            boolean wasExceptionNull = this.exception.compareAndSet(null, exception);
            if (wasExceptionNull == false) {
                throw new IllegalStateException("exception is already set");
            }
            latch.countDown();
        }

        /**
         * Waits (up to a timeout) for some result of the request: either a response, or an exception.
         */
        Response get() throws IOException {
            try {
                //providing timeout is just a safety measure to prevent everlasting waits
                //the different client timeouts should already do their jobs
                if (latch.await(timeout, TimeUnit.MILLISECONDS) == false) {
                    throw new IOException("listener timeout after waiting for [" + timeout + "] ms");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("thread waiting for the response was interrupted", e);
            }

            Exception exception = this.exception.get();
            Response response = this.response.get();
            if (exception != null) {
                if (response != null) {
                    IllegalStateException e = new IllegalStateException("response and exception are unexpectedly set at the same time");
                    e.addSuppressed(exception);
                    throw e;
                }
                /*
                 * Wrap and rethrow whatever exception we received, copying the type
                 * where possible so the synchronous API looks as much as possible
                 * like the asynchronous API. We wrap the exception so that the caller's
                 * signature shows up in any exception we throw.
                 */
                if (exception instanceof ResponseException) {
                    throw new ResponseException((ResponseException) exception);
                }
                if (exception instanceof ConnectTimeoutException) {
                    ConnectTimeoutException e = new ConnectTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SocketTimeoutException) {
                    SocketTimeoutException e = new SocketTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof ConnectionClosedException) {
                    ConnectionClosedException e = new ConnectionClosedException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SSLHandshakeException) {
                    SSLHandshakeException e = new SSLHandshakeException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof ConnectException) {
                    ConnectException e = new ConnectException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof IOException) {
                    throw new IOException(exception.getMessage(), exception);
                }
                if (exception instanceof RuntimeException){
                    throw new RuntimeException(exception.getMessage(), exception);
                }
                throw new RuntimeException("error while performing request", exception);
            }

            if (response == null) {
                throw new IllegalStateException("response not set and no exception caught either");
            }
            return response;
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Notifies that the node provided as argument has just failed
         */
        public void onFailure(Node node) {}
    }

    /**
     * {@link NodeTuple} enables the {@linkplain Node}s and {@linkplain AuthCache}
     * to be set together in a thread safe, volatile way.
     */
    static class NodeTuple<T> {
        final T nodes;
        final AuthCache authCache;

        NodeTuple(final T nodes, final AuthCache authCache) {
            this.nodes = nodes;
            this.authCache = authCache;
        }
    }

    /**
     * Contains a reference to a blacklisted node and the time until it is
     * revived. We use this so we can do a single pass over the blacklist.
     */
    private static class DeadNode implements Comparable<DeadNode> {
        final Node node;
        final DeadHostState deadness;

        DeadNode(Node node, DeadHostState deadness) {
            this.node = node;
            this.deadness = deadness;
        }

        @Override
        public String toString() {
            return node.toString();
        }

        @Override
        public int compareTo(DeadNode rhs) {
            return deadness.compareTo(rhs.deadness);
        }
    }

    /**
     * Adapts an <code>Iterator&lt;DeadNodeAndRevival&gt;</code> into an
     * <code>Iterator&lt;Node&gt;</code>.
     */
    private static class DeadNodeIteratorAdapter implements Iterator<Node> {
        private final Iterator<DeadNode> itr;

        private DeadNodeIteratorAdapter(Iterator<DeadNode> itr) {
            this.itr = itr;
        }

        @Override
        public boolean hasNext() {
            return itr.hasNext();
        }

        @Override
        public Node next() {
            return itr.next().node;
        }

        @Override
        public void remove() {
            itr.remove();
        }
    }

    /**
     * Add all headers from the provided varargs argument to a {@link Request}. This only exists
     * to support methods that exist for backwards compatibility.
     */
    @Deprecated
    private static void addHeaders(Request request, Header... headers) {
        setOptions(request, RequestOptions.DEFAULT.getHttpAsyncResponseConsumerFactory(), headers);
    }

    /**
     * Add all headers from the provided varargs argument to a {@link Request}. This only exists
     * to support methods that exist for backwards compatibility.
     */
    @Deprecated
    private static void setOptions(Request request, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
            Header... headers) {
        Objects.requireNonNull(headers, "headers cannot be null");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Header header : headers) {
            Objects.requireNonNull(header, "header cannot be null");
            options.addHeader(header.getName(), header.getValue());
        }
        options.setHttpAsyncResponseConsumerFactory(httpAsyncResponseConsumerFactory);
        request.setOptions(options);
    }

    /**
     * Add all parameters from a map to a {@link Request}. This only exists
     * to support methods that exist for backwards compatibility.
     */
    @Deprecated
    private static void addParameters(Request request, Map<String, String> parameters) {
        Objects.requireNonNull(parameters, "parameters cannot be null");
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
    }
}
