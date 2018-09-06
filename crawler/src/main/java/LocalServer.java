import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Paths;

public class LocalServer {
    private static final Logger logger = LoggerFactory.getLogger(LocalServer.class);
    Server server;

    public void start() {
        int port = Integer.parseInt(Engine.getConfigs().get("crawler.webserver.port", "50001"));
        server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(Paths.get(".").toAbsolutePath().toString());
        server.setHandler(context);


        context.addServlet(DefaultServlet.class, "/cmd");
        context.addServlet(DefaultServlet.class, "/");
        logger.info("WebServerHandler Service Created");

    }
}

class CmdServlet extends HttpServlet{
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        CrawlerService crawler = (CrawlerService) Engine.getInstance().getService("crawler");
        
    }
}
