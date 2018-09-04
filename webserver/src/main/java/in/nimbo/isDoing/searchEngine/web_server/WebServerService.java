package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class WebServerService implements Service {

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput());
        new WebServerService().start();
    }

    private final static Logger logger = LoggerFactory.getLogger(WebServerService.class);
    Server server;

    public WebServerService() throws IOException {
        logger.info("Creating WebServerHandler Service...");
        server = new Server(9090);
        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(Paths.get(".").toAbsolutePath().toString());
        server.setHandler(context);


        context.addServlet(WebSearchServlet.class, "/search/");
        context.addServlet(SystemStatusServlet.class, "/status/*");
        context.addServlet(DefaultServlet.class, "/");
        logger.info("WebServerHandler Service Created");
    }


    @Override
    public void start() {
        logger.info("Starting WebServerHandler Service...");
        Engine.getOutput().show("Starting WebServerHandler Service...");

        try {
            server.start();
        } catch (Exception e) {
            logger.error("WebServerHandler Error", e);
            Engine.getOutput().show("WebServerHandler Stopped...");
        }
    }

    @Override
    public void stop() {
        try {
            server.stop();
            logger.info("Stopping WebServerHandler Service");
            Engine.getOutput().show("Stopping WebServerHandler Service");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status status() {
        return null;
    }

    @Override
    public String getName() {
        return "webServer";
    }
}