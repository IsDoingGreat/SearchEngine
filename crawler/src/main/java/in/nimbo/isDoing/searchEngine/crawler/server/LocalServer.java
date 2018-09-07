package in.nimbo.isDoing.searchEngine.crawler.server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class LocalServer {
    private static final Logger logger = LoggerFactory.getLogger(LocalServer.class);
    private Server server;

    public void start() throws Exception {
        int port = Integer.parseInt(Engine.getConfigs().get("crawler.webserver.port", "50001"));
        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(Paths.get(".").toAbsolutePath().toString());
        server.setHandler(context);


        context.addServlet(ReloadServlet.class, "/reload/");
        context.addServlet(ConfigsServlet.class, "/config/");
        context.addServlet(StatusServlet.class, "/");

        server.start();
        logger.info("WebServerHandler Service Created");
    }
}

