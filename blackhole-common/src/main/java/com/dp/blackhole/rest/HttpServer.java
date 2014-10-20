package com.dp.blackhole.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionManager;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.AbstractSessionManager;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory "/static/" -> points to common static
 * files (src/webapps/static) "/" -> the jsp server code from
 * (src/webapps/<name>)
 */
public final class HttpServer {
    public static final Log LOG = LogFactory.getLog(HttpServer.class);

    static final String HTTP_MAX_THREADS = "http.max.threads";

    // The ServletContext attribute where the daemon Configuration
    // gets stored.
    public static final String CONF_CONTEXT_ATTRIBUTE = "blackhole.conf";
    public static final String NO_CACHE_FILTER = "NoCacheFilter";

    public static final String BIND_ADDRESS = "bind.address";

    protected final Server webServer;

    private static class ConnectorInfo {
        /**
         * Boolean flag to determine whether the HTTP server should clean up the
         * listener in stop().
         */
        private final boolean isManaged;
        private final Connector connector;

        private ConnectorInfo(boolean isManaged, Connector connector) {
            this.isManaged = isManaged;
            this.connector = connector;
        }
    }

    private final List<ConnectorInfo> listeners = new ArrayList<HttpServer.ConnectorInfo>();

    protected final WebAppContext webAppContext;
    protected final boolean findPort;
    protected final List<String> filterNames = new ArrayList<String>();
    static final String STATE_DESCRIPTION_ALIVE = " - alive";
    static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";

    /**
     * Class to construct instances of HTTP server with specific options.
     */
    public static class Builder {
        private ArrayList<URI> endpoints = new ArrayList<URI>();
        private Connector connector;
        private String name;
        private String[] pathSpecs;
        private boolean findPort;
        private String hostName;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Add an endpoint that the HTTP server should listen to.
         * 
         * @param endpoint
         *            the endpoint of that the HTTP server should listen to. The
         *            scheme specifies the protocol (i.e. HTTP / HTTPS), the
         *            host specifies the binding address, and the port specifies
         *            the listening port. Unspecified or zero port means that
         *            the server can listen to any port.
         */
        public Builder addEndpoint(URI endpoint) {
            endpoints.add(endpoint);
            return this;
        }

        /**
         * Set the hostname of the http server. The hostname of the first
         * listener will be used if the name is unspecified.
         */
        public Builder hostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder setConnector(Connector connector) {
            this.connector = connector;
            return this;
        }

        public Builder setPathSpec(String[] pathSpec) {
            this.pathSpecs = pathSpec;
            return this;
        }

        public HttpServer build() throws IOException {
            if (this.name == null) {
                throw new IllegalArgumentException("name is not set");
            }

            if (endpoints.size() == 0 && connector == null) {
                throw new IllegalArgumentException("No endpoints specified");
            }

            if (hostName == null) {
                hostName = endpoints.size() == 0 ? connector.getHost()
                        : endpoints.get(0).getHost();
            }

            HttpServer server = new HttpServer(this);

            if (connector != null) {
                server.addUnmanagedListener(connector);
            }

            for (URI ep : endpoints) {
                Connector conn = null;
                String scheme = ep.getScheme();
                if ("http".equals(scheme)) {
                    conn = HttpServer.createDefaultChannelConnector();
                } else if ("https".equals(scheme)) {
                    conn = new SslSocketConnector();
                } else {
                    throw new IllegalArgumentException(
                            "unknown scheme for endpoint:" + ep);
                }
                conn.setHost(ep.getHost());
                conn.setPort(ep.getPort() == -1 ? 0 : ep.getPort());
                server.addManagedListener(conn);
            }
            server.loadListeners();
            return server;
        }
    }

    private HttpServer(final Builder b) throws IOException {
        final String appDir = getWebAppsPath(b.name);
        this.webServer = new Server();
        this.webAppContext = createWebAppContext(b.name, appDir);
        this.findPort = b.findPort;
        initializeWebServer(b.name, b.hostName, b.pathSpecs);
    }

    private void initializeWebServer(String name, String hostName,
            String[] pathSpecs)
            throws FileNotFoundException, IOException {

        int maxThreads = 2;
        // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
        // default value (currently 250).
        QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool()
                : new QueuedThreadPool(maxThreads);
        threadPool.setDaemon(true);
        webServer.setThreadPool(threadPool);

        SessionManager sm = webAppContext.getSessionHandler()
                .getSessionManager();
        if (sm instanceof AbstractSessionManager) {
            AbstractSessionManager asm = (AbstractSessionManager) sm;
            asm.setHttpOnly(true);
            asm.setSecureCookies(true);
        }

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        webServer.setHandler(contexts);
        webServer.addHandler(webAppContext);

        if (pathSpecs != null) {
            for (String path : pathSpecs) {
                LOG.info("adding path spec: " + path);
                addFilterPathMapping(path, webAppContext);
            }
        }
    }

    private void addUnmanagedListener(Connector connector) {
        listeners.add(new ConnectorInfo(false, connector));
    }

    private void addManagedListener(Connector connector) {
        listeners.add(new ConnectorInfo(true, connector));
    }

    public static WebAppContext createWebAppContext(String name,
           final String appDir) {
        WebAppContext ctx = new WebAppContext();
        ctx.setDisplayName(name);
        ctx.setContextPath("/");
        ctx.setWar(appDir + "/" + name);
        return ctx;
    }

    public static Connector createDefaultChannelConnector() {
        SelectChannelConnector ret = new SelectChannelConnector();
        ret.setLowResourceMaxIdleTime(10000);
        ret.setAcceptQueueSize(128);
        ret.setResolveNames(false);
        ret.setUseDirectBuffers(false);
        if (System.getProperty("os.name").startsWith("Windows")) {
            // result of setting the SO_REUSEADDR flag is different on Windows
            // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
            // without this 2 NN's can start on the same machine and listen on
            // the same port with indeterminate routing of incoming requests to
            // them
            ret.setReuseAddress(false);
        }
        ret.setHeaderBufferSize(1024 * 64);
        return ret;
    }

    public void addContext(Context ctxt) throws IOException {
        webServer.addHandler(ctxt);
    }

    /**
     * Set a value in the webapp context. These values are available to the jsp
     * pages as "application.getAttribute(name)".
     * 
     * @param name
     *            The name of the attribute
     * @param value
     *            The value of the attribute
     */
    public void setAttribute(String name, Object value) {
        webAppContext.setAttribute(name, value);
    }

    /**
     * Add a Jersey resource package.
     * 
     * @param packageName
     *            The Java package name containing the Jersey resource.
     * @param pathSpec
     *            The path spec for the servlet
     */
    public void addJerseyResourcePackage(final String packageName,
            final String pathSpec) {
        LOG.info("addJerseyResourcePackage: packageName=" + packageName
                + ", pathSpec=" + pathSpec);
        final ServletHolder sh = new ServletHolder(ServletContainer.class);
        sh.setInitParameter(
                "com.sun.jersey.config.property.resourceConfigClass",
                "com.sun.jersey.api.core.PackagesResourceConfig");
        sh.setInitParameter("com.sun.jersey.config.property.packages",
                packageName);
        sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature",
                "true");
        webAppContext.addServlet(sh, pathSpec);
    }

    /**
     * Add a servlet in the server.
     * 
     * @param name
     *            The name of the servlet (can be passed as null)
     * @param pathSpec
     *            The path spec for the servlet
     * @param clazz
     *            The servlet class
     */
    public void addServlet(String name, String pathSpec,
            Class<? extends HttpServlet> clazz) {
        addInternalServlet(name, pathSpec, clazz);
        addFilterPathMapping(pathSpec, webAppContext);
    }

    /**
     * Add an internal servlet in the server. Note: This method is to be used for
     * adding servlets that facilitate internal communication and not for user
     * facing functionality. For + * servlets added using this method, filters
     * are not enabled.
     * 
     * @param name
     *            The name of the servlet (can be passed as null)
     * @param pathSpec
     *            The path spec for the servlet
     * @param clazz
     *            The servlet class
     */
    public void addInternalServlet(String name, String pathSpec,
            Class<? extends HttpServlet> clazz) {
        ServletHolder holder = new ServletHolder(clazz);
        if (name != null) {
            holder.setName(name);
        }
        webAppContext.addServlet(holder, pathSpec);
    }
    
    public void addFilter(String name, String classname,
            Map<String, String> parameters) {
        final String[] ALL_URLS = { "/*" };
        defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
                + ") to context " + webAppContext.getDisplayName());
    }

    /**
     * Define a filter for a context and set up default url mappings.
     */
    public static void defineFilter(Context ctx, String name, String classname,
            Map<String, String> parameters, String[] urls) {

        FilterHolder holder = new FilterHolder();
        holder.setName(name);
        holder.setClassName(classname);
        holder.setInitParameters(parameters);
        FilterMapping fmap = new FilterMapping();
        fmap.setPathSpecs(urls);
        fmap.setDispatches(Handler.ALL);
        fmap.setFilterName(name);
        ServletHandler handler = ctx.getServletHandler();
        handler.addFilter(holder, fmap);
    }

    /**
     * Add the path spec to the filter path mapping.
     * 
     * @param pathSpec
     *            The path spec
     * @param webAppCtx
     *            The WebApplicationContext to add to
     */
    protected void addFilterPathMapping(String pathSpec, Context webAppCtx) {
        ServletHandler handler = webAppCtx.getServletHandler();
        for (String name : filterNames) {
            FilterMapping fmap = new FilterMapping();
            fmap.setPathSpec(pathSpec);
            fmap.setFilterName(name);
            fmap.setDispatches(Handler.ALL);
            handler.addFilterMapping(fmap);
        }
    }

    /**
     * Get the value in the webapp context.
     * 
     * @param name
     *            The name of the attribute
     * @return The value of the attribute
     */
    public Object getAttribute(String name) {
        return webAppContext.getAttribute(name);
    }

    public WebAppContext getWebAppContext() {
        return this.webAppContext;
    }

    /**
     * Get the pathname to the webapps files.
     * 
     * @param appName
     *            eg "secondary" or "datanode"
     * @return the pathname as a URL
     * @throws FileNotFoundException
     *             if 'webapps' directory cannot be found on CLASSPATH.
     */
    protected String getWebAppsPath(String appName)
            throws FileNotFoundException {
        URL url = getClass().getClassLoader().getResource("webapps/" + appName);
        if (url == null)
            throw new FileNotFoundException("webapps/" + appName
                    + " not found in CLASSPATH");
        String urlString = url.toString();
        return urlString.substring(0, urlString.lastIndexOf('/'));
    }

    /**
     * Get the port that the server is on
     * 
     * @return the port
     */
    @Deprecated
    public int getPort() {
        return webServer.getConnectors()[0].getLocalPort();
    }

    /**
     * Get the address that corresponds to a particular connector.
     * 
     * @return the corresponding address for the connector, or null if there's
     *         no such connector or the connector is not bounded.
     */
    public InetSocketAddress getConnectorAddress(int index) {
        if (index > webServer.getConnectors().length)
            return null;

        Connector c = webServer.getConnectors()[index];
        if (c.getLocalPort() == -1) {
            // The connector is not bounded
            return null;
        }

        return new InetSocketAddress(c.getHost(), c.getLocalPort());
    }

    /**
     * Set the min, max number of worker threads (simultaneous connections).
     */
    public void setThreads(int min, int max) {
        QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
        pool.setMinThreads(min);
        pool.setMaxThreads(max);
    }

    /**
     * Start the server. Does not wait for the server to start.
     */
    public void start() throws IOException {
        try {
            try {
                openListeners();
                webServer.start();
            } catch (IOException ex) {
                LOG.info("HttpServer.start() threw a non Bind IOException", ex);
                throw ex;
            } catch (MultiException ex) {
                LOG.info("HttpServer.start() threw a MultiException", ex);
                throw ex;
            }
            // Make sure there is no handler failures.
            Handler[] handlers = webServer.getHandlers();
            for (int i = 0; i < handlers.length; i++) {
                if (handlers[i].isFailed()) {
                    throw new IOException(
                            "Problem in starting http server. Server handlers failed");
                }
            }
            // Make sure there are no errors initializing the context.
            Throwable unavailableException = webAppContext
                    .getUnavailableException();
            if (unavailableException != null) {
                // Have to stop the webserver, or else its non-daemon threads
                // will hang forever.
                webServer.stop();
                throw new IOException("Unable to initialize WebAppContext",
                        unavailableException);
            }
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw (IOException) new InterruptedIOException(
                    "Interrupted while starting HTTP server").initCause(e);
        } catch (Exception e) {
            throw new IOException("Problem starting http server", e);
        }
    }

    private void loadListeners() {
        for (ConnectorInfo conn : listeners) {
            webServer.addConnector(conn.connector);
        }
    }

    /**
     * Open the main listener for the server
     * 
     * @throws Exception
     */
    void openListeners() throws Exception {
        for (ConnectorInfo conn : listeners) {
            Connector connector = conn.connector;
            if (!conn.isManaged || conn.connector.getLocalPort() != -1) {
                // This listener is either started externally or has been bound
                continue;
            }
            int port = connector.getPort();
            while (true) {
                // jetty has a bug where you can't reopen a listener that
                // previously
                // failed to open w/o issuing a close first, even if the port is
                // changed
                try {
                    connector.close();
                    connector.open();
                    LOG.info("Jetty bound to port " + connector.getLocalPort());
                    break;
                } catch (BindException ex) {
                    if (port == 0 || !findPort) {
                        BindException be = new BindException("Port in use: "
                                + connector.getHost() + ":" + connector.getPort());
                        be.initCause(ex);
                        throw be;
                    }
                }
                // try the next port number
                connector.setPort(++port);
                Thread.sleep(100);
            }
        }
    }

    /**
     * stop the server
     */
    public void stop() throws Exception {
        MultiException exception = null;
        for (ConnectorInfo conn : listeners) {
            if (!conn.isManaged) {
                continue;
            }

            try {
                conn.connector.close();
            } catch (Exception e) {
                LOG.error("Error while stopping listener for webapp"
                        + webAppContext.getDisplayName(), e);
                exception = addMultiException(exception, e);
            }
        }

        try {
            // clear & stop webAppContext attributes to avoid memory leaks.
            webAppContext.clearAttributes();
            webAppContext.stop();
        } catch (Exception e) {
            LOG.error("Error while stopping web app context for webapp "
                    + webAppContext.getDisplayName(), e);
            exception = addMultiException(exception, e);
        }

        try {
            webServer.stop();
        } catch (Exception e) {
            LOG.error("Error while stopping web server for webapp "
                    + webAppContext.getDisplayName(), e);
            exception = addMultiException(exception, e);
        }

        if (exception != null) {
            exception.ifExceptionThrow();
        }

    }

    private MultiException addMultiException(MultiException exception,
            Exception e) {
        if (exception == null) {
            exception = new MultiException();
        }
        exception.add(e);
        return exception;
    }

    public void join() throws InterruptedException {
        webServer.join();
    }

    /**
     * Test for the availability of the web server
     * 
     * @return true if the web server is started, false otherwise
     */
    public boolean isAlive() {
        return webServer != null && webServer.isStarted();
    }

    /**
     * Return the host and port of the HttpServer, if live
     * 
     * @return the classname and any HTTP URL
     */
    @Override
    public String toString() {
        if (listeners.size() == 0) {
            return "Inactive HttpServer";
        } else {
            StringBuilder sb = new StringBuilder("HttpServer (").append(
                    isAlive() ? STATE_DESCRIPTION_ALIVE
                            : STATE_DESCRIPTION_NOT_LIVE).append(
                    "), listening at:");
            for (ConnectorInfo conn : listeners) {
                Connector l = conn.connector;
                sb.append(l.getHost()).append(":").append(l.getPort())
                        .append("/,");
            }
            return sb.toString();
        }
    }

    // /**
    // * A very simple servlet to serve up a text representation of the current
    // * stack traces. It both returns the stacks to the caller and logs them.
    // * Currently the stack traces are done sequentially rather than exactly
    // the
    // * same data.
    // */
    // public static class StackServlet extends HttpServlet {
    // private static final long serialVersionUID = -6284183679759467039L;
    //
    // @Override
    // public void doGet(HttpServletRequest request, HttpServletResponse
    // response)
    // throws ServletException, IOException {
    // if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
    // request, response)) {
    // return;
    // }
    // response.setContentType("text/plain; charset=UTF-8");
    // PrintWriter out = response.getWriter();
    // ReflectionUtils.printThreadInfo(out, "");
    // out.close();
    // ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    // }
    // }
}
