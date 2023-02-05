//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.http.impl.execchain;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.auth.AuthProtocolState;
import org.apache.http.auth.AuthState;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.NonRepeatableRequestException;
import org.apache.http.client.UserTokenHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.BasicRouteDirector;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRouteDirector;
import org.apache.http.conn.routing.RouteTracker;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.HttpAuthenticator;
import org.apache.http.impl.conn.ConnectionShutdownException;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.util.Args;
import org.apache.http.util.EntityUtils;
import org.example.Federapp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Contract(
        threading = ThreadingBehavior.IMMUTABLE_CONDITIONAL
)
public class MainClientExec implements ClientExecChain {
    private final Log log;
    private final HttpRequestExecutor requestExecutor;
    private final HttpClientConnectionManager connManager;
    private final ConnectionReuseStrategy reuseStrategy;
    private final ConnectionKeepAliveStrategy keepAliveStrategy;
    private final HttpProcessor proxyHttpProcessor;
    private final AuthenticationStrategy targetAuthStrategy;
    private final AuthenticationStrategy proxyAuthStrategy;
    private final HttpAuthenticator authenticator;
    private final UserTokenHandler userTokenHandler;
    private final HttpRouteDirector routeDirector;

    public MainClientExec(HttpRequestExecutor requestExecutor, HttpClientConnectionManager connManager, ConnectionReuseStrategy reuseStrategy, ConnectionKeepAliveStrategy keepAliveStrategy, HttpProcessor proxyHttpProcessor, AuthenticationStrategy targetAuthStrategy, AuthenticationStrategy proxyAuthStrategy, UserTokenHandler userTokenHandler) {
        this.log = LogFactory.getLog(this.getClass());
        Args.notNull(requestExecutor, "HTTP request executor");
        Args.notNull(connManager, "Client connection manager");
        Args.notNull(reuseStrategy, "Connection reuse strategy");
        Args.notNull(keepAliveStrategy, "Connection keep alive strategy");
        Args.notNull(proxyHttpProcessor, "Proxy HTTP processor");
        Args.notNull(targetAuthStrategy, "Target authentication strategy");
        Args.notNull(proxyAuthStrategy, "Proxy authentication strategy");
        Args.notNull(userTokenHandler, "User token handler");
        this.authenticator = new HttpAuthenticator();
        this.routeDirector = new BasicRouteDirector();
        this.requestExecutor = requestExecutor;
        this.connManager = connManager;
        this.reuseStrategy = reuseStrategy;
        this.keepAliveStrategy = keepAliveStrategy;
        this.proxyHttpProcessor = proxyHttpProcessor;
        this.targetAuthStrategy = targetAuthStrategy;
        this.proxyAuthStrategy = proxyAuthStrategy;
        this.userTokenHandler = userTokenHandler;
    }

    public MainClientExec(HttpRequestExecutor requestExecutor, HttpClientConnectionManager connManager, ConnectionReuseStrategy reuseStrategy, ConnectionKeepAliveStrategy keepAliveStrategy, AuthenticationStrategy targetAuthStrategy, AuthenticationStrategy proxyAuthStrategy, UserTokenHandler userTokenHandler) {
        this(requestExecutor, connManager, reuseStrategy, keepAliveStrategy, new ImmutableHttpProcessor(new HttpRequestInterceptor[]{new RequestTargetHost()}), targetAuthStrategy, proxyAuthStrategy, userTokenHandler);
    }

    public CloseableHttpResponse execute(HttpRoute route, HttpRequestWrapper request, HttpClientContext context, HttpExecutionAware execAware) throws IOException, HttpException {
        Args.notNull(route, "HTTP route");
        Args.notNull(request, "HTTP request");
        Args.notNull(context, "HTTP context");

        ((AtomicInteger) Federapp.CONTAINER.get(Federapp.COUNT_HTTP_REQ_KEY)).getAndIncrement();
        ((ConcurrentLinkedQueue) Federapp.CONTAINER.get(Federapp.LIST_HTTP_REQ_KEY)).add(execAware.toString());

        AuthState targetAuthState = context.getTargetAuthState();
        if (targetAuthState == null) {
            targetAuthState = new AuthState();
            context.setAttribute("http.auth.target-scope", targetAuthState);
        }

        AuthState proxyAuthState = context.getProxyAuthState();
        if (proxyAuthState == null) {
            proxyAuthState = new AuthState();
            context.setAttribute("http.auth.proxy-scope", proxyAuthState);
        }

        if (request instanceof HttpEntityEnclosingRequest) {
            RequestEntityProxy.enhance((HttpEntityEnclosingRequest)request);
        }

        Object userToken = context.getUserToken();
        ConnectionRequest connRequest = this.connManager.requestConnection(route, userToken);
        if (execAware != null) {
            if (execAware.isAborted()) {
                connRequest.cancel();
                throw new RequestAbortedException("Request aborted");
            }

            execAware.setCancellable(connRequest);
        }

        RequestConfig config = context.getRequestConfig();

        HttpClientConnection managedConn;
        try {
            int timeout = config.getConnectionRequestTimeout();
            managedConn = connRequest.get(timeout > 0 ? (long)timeout : 0L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException var18) {
            Thread.currentThread().interrupt();
            throw new RequestAbortedException("Request aborted", var18);
        } catch (ExecutionException var19) {
            Throwable cause = var19.getCause();
            if (cause == null) {
                cause = var19;
            }

            throw new RequestAbortedException("Request execution failed", (Throwable)cause);
        }

        context.setAttribute("http.connection", managedConn);
        if (config.isStaleConnectionCheckEnabled() && managedConn.isOpen()) {
            //this.log.debug("Stale connection check");
            if (managedConn.isStale()) {
                //this.log.debug("Stale connection detected");
                managedConn.close();
            }
        }

        ConnectionHolder connHolder = new ConnectionHolder(this.log, this.connManager, managedConn);

        try {
            if (execAware != null) {
                execAware.setCancellable(connHolder);
            }

            int execCount = 1;

            HttpResponse response;
            while(true) {
                if (execCount > 1 && !RequestEntityProxy.isRepeatable(request)) {
                    throw new NonRepeatableRequestException("Cannot retry request with a non-repeatable request entity.");
                }

                if (execAware != null && execAware.isAborted()) {
                    throw new RequestAbortedException("Request aborted");
                }

                if (!managedConn.isOpen()) {
                    //this.log.debug("Opening connection " + route);

                    try {
                        this.establishRoute(proxyAuthState, managedConn, route, request, context);
                    } catch (TunnelRefusedException var20) {
                        if (this.log.isDebugEnabled()) {
                            //this.log.debug(var20.getMessage());
                        }

                        response = var20.getResponse();
                        break;
                    }
                }

                int timeout = config.getSocketTimeout();
                if (timeout >= 0) {
                    managedConn.setSocketTimeout(timeout);
                }

                if (execAware != null && execAware.isAborted()) {
                    throw new RequestAbortedException("Request aborted");
                }

                if (this.log.isDebugEnabled()) {
                    //this.log.debug("Executing request " + request.getRequestLine());
                }

                if (!request.containsHeader("Authorization")) {
                    if (this.log.isDebugEnabled()) {
                        //this.log.debug("Target auth state: " + targetAuthState.getState());
                    }

                    this.authenticator.generateAuthResponse(request, targetAuthState, context);
                }

                if (!request.containsHeader("Proxy-Authorization") && !route.isTunnelled()) {
                    if (this.log.isDebugEnabled()) {
                        //this.log.debug("Proxy auth state: " + proxyAuthState.getState());
                    }

                    this.authenticator.generateAuthResponse(request, proxyAuthState, context);
                }

                context.setAttribute("http.request", request);
                response = this.requestExecutor.execute(request, managedConn, context);
                if (this.reuseStrategy.keepAlive(response, context)) {
                    long duration = this.keepAliveStrategy.getKeepAliveDuration(response, context);
                    if (this.log.isDebugEnabled()) {
                        String s;
                        if (duration > 0L) {
                            s = "for " + duration + " " + TimeUnit.MILLISECONDS;
                        } else {
                            s = "indefinitely";
                        }

                        //this.log.debug("Connection can be kept alive " + s);
                    }

                    connHolder.setValidFor(duration, TimeUnit.MILLISECONDS);
                    connHolder.markReusable();
                } else {
                    connHolder.markNonReusable();
                }

                if (!this.needAuthentication(targetAuthState, proxyAuthState, route, response, context)) {
                    break;
                }

                HttpEntity entity = response.getEntity();
                if (connHolder.isReusable()) {
                    EntityUtils.consume(entity);
                } else {
                    managedConn.close();
                    if (proxyAuthState.getState() == AuthProtocolState.SUCCESS && proxyAuthState.isConnectionBased()) {
                        //this.log.debug("Resetting proxy auth state");
                        proxyAuthState.reset();
                    }

                    if (targetAuthState.getState() == AuthProtocolState.SUCCESS && targetAuthState.isConnectionBased()) {
                        //this.log.debug("Resetting target auth state");
                        targetAuthState.reset();
                    }
                }

                HttpRequest original = request.getOriginal();
                if (!original.containsHeader("Authorization")) {
                    request.removeHeaders("Authorization");
                }

                if (!original.containsHeader("Proxy-Authorization")) {
                    request.removeHeaders("Proxy-Authorization");
                }

                ++execCount;
            }

            if (userToken == null) {
                userToken = this.userTokenHandler.getUserToken(context);
                context.setAttribute("http.user-token", userToken);
            }

            if (userToken != null) {
                connHolder.setState(userToken);
            }

            HttpEntity entity = response.getEntity();
            if (entity != null && entity.isStreaming()) {
                return new HttpResponseProxy(response, connHolder);
            } else {
                connHolder.releaseConnection();
                return new HttpResponseProxy(response, (ConnectionHolder)null);
            }
        } catch (ConnectionShutdownException var21) {
            InterruptedIOException ioex = new InterruptedIOException("Connection has been shut down");
            ioex.initCause(var21);
            throw ioex;
        } catch (HttpException var22) {
            connHolder.abortConnection();
            throw var22;
        } catch (IOException var23) {
            connHolder.abortConnection();
            if (proxyAuthState.isConnectionBased()) {
                proxyAuthState.reset();
            }

            if (targetAuthState.isConnectionBased()) {
                targetAuthState.reset();
            }

            throw var23;
        } catch (RuntimeException var24) {
            connHolder.abortConnection();
            if (proxyAuthState.isConnectionBased()) {
                proxyAuthState.reset();
            }

            if (targetAuthState.isConnectionBased()) {
                targetAuthState.reset();
            }

            throw var24;
        } catch (Error var25) {
            this.connManager.shutdown();
            throw var25;
        }
    }

    void establishRoute(AuthState proxyAuthState, HttpClientConnection managedConn, HttpRoute route, HttpRequest request, HttpClientContext context) throws HttpException, IOException {
        RequestConfig config = context.getRequestConfig();
        int timeout = config.getConnectTimeout();
        RouteTracker tracker = new RouteTracker(route);

        int step;
        do {
            HttpRoute fact = tracker.toRoute();
            step = this.routeDirector.nextStep(route, fact);
            Boolean secure = null;
            switch(step) {
                case -1:
                    throw new HttpException("Unable to establish route: planned = " + route + "; current = " + fact);
                case 0:
                    this.connManager.routeComplete(managedConn, route, context);
                    break;
                case 1:
                    this.connManager.connect(managedConn, route, timeout > 0 ? timeout : 0, context);
                    tracker.connectTarget(route.isSecure());
                    break;
                case 2:
                    this.connManager.connect(managedConn, route, timeout > 0 ? timeout : 0, context);
                    HttpHost proxy = route.getProxyHost();
                    tracker.connectProxy(proxy, route.isSecure() && !route.isTunnelled());
                    break;
                case 3:
                     secure = this.createTunnelToTarget(proxyAuthState, managedConn, route, request, context);
                    //this.log.debug("Tunnel to target created.");
                    tracker.tunnelTarget(secure);
                    break;
                case 4:
                    int hop = fact.getHopCount() - 1;
                    secure = this.createTunnelToProxy(route, hop, context);
                    //this.log.debug("Tunnel to proxy created.");
                    tracker.tunnelProxy(route.getHopTarget(hop), secure);
                    break;
                case 5:
                    this.connManager.upgrade(managedConn, route, context);
                    tracker.layerProtocol(route.isSecure());
                    break;
                default:
                    throw new IllegalStateException("Unknown step indicator " + step + " from RouteDirector.");
            }
        } while(step > 0);

    }

    private boolean createTunnelToTarget(AuthState proxyAuthState, HttpClientConnection managedConn, HttpRoute route, HttpRequest request, HttpClientContext context) throws HttpException, IOException {
        RequestConfig config = context.getRequestConfig();
        int timeout = config.getConnectTimeout();
        HttpHost target = route.getTargetHost();
        HttpHost proxy = route.getProxyHost();
        HttpResponse response = null;
        String authority = target.toHostString();
        HttpRequest connect = new BasicHttpRequest("CONNECT", authority, request.getProtocolVersion());
        this.requestExecutor.preProcess(connect, this.proxyHttpProcessor, context);

        int status;
        HttpEntity entity;
        while(response == null) {
            if (!managedConn.isOpen()) {
                this.connManager.connect(managedConn, route, timeout > 0 ? timeout : 0, context);
            }

            connect.removeHeaders("Proxy-Authorization");
            this.authenticator.generateAuthResponse(connect, proxyAuthState, context);
            response = this.requestExecutor.execute(connect, managedConn, context);
            this.requestExecutor.postProcess(response, this.proxyHttpProcessor, context);
            status = response.getStatusLine().getStatusCode();
            if (status < 200) {
                throw new HttpException("Unexpected response to CONNECT request: " + response.getStatusLine());
            }

            if (config.isAuthenticationEnabled() && this.authenticator.isAuthenticationRequested(proxy, response, this.proxyAuthStrategy, proxyAuthState, context) && this.authenticator.handleAuthChallenge(proxy, response, this.proxyAuthStrategy, proxyAuthState, context)) {
                if (this.reuseStrategy.keepAlive(response, context)) {
                    //this.log.debug("Connection kept alive");
                    entity = response.getEntity();
                    EntityUtils.consume(entity);
                } else {
                    managedConn.close();
                }

                response = null;
            }
        }

        status = response.getStatusLine().getStatusCode();
        if (status > 299) {
            entity = response.getEntity();
            if (entity != null) {
                response.setEntity(new BufferedHttpEntity(entity));
            }

            managedConn.close();
            throw new TunnelRefusedException("CONNECT refused by proxy: " + response.getStatusLine(), response);
        } else {
            return false;
        }
    }

    private boolean createTunnelToProxy(HttpRoute route, int hop, HttpClientContext context) throws HttpException {
        throw new HttpException("Proxy chains are not supported.");
    }

    private boolean needAuthentication(AuthState targetAuthState, AuthState proxyAuthState, HttpRoute route, HttpResponse response, HttpClientContext context) {
        RequestConfig config = context.getRequestConfig();
        if (config.isAuthenticationEnabled()) {
            HttpHost target = context.getTargetHost();
            if (target == null) {
                target = route.getTargetHost();
            }

            if (target.getPort() < 0) {
                target = new HttpHost(target.getHostName(), route.getTargetHost().getPort(), target.getSchemeName());
            }

            boolean targetAuthRequested = this.authenticator.isAuthenticationRequested(target, response, this.targetAuthStrategy, targetAuthState, context);
            HttpHost proxy = route.getProxyHost();
            if (proxy == null) {
                proxy = route.getTargetHost();
            }

            boolean proxyAuthRequested = this.authenticator.isAuthenticationRequested(proxy, response, this.proxyAuthStrategy, proxyAuthState, context);
            if (targetAuthRequested) {
                return this.authenticator.handleAuthChallenge(target, response, this.targetAuthStrategy, targetAuthState, context);
            }

            if (proxyAuthRequested) {
                return this.authenticator.handleAuthChallenge(proxy, response, this.proxyAuthStrategy, proxyAuthState, context);
            }
        }

        return false;
    }
}
