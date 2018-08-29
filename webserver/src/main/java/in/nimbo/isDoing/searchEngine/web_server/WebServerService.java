package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WebServerService implements Service {

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput());
        new WebServerService().start();
    }

    private final static Logger logger = LoggerFactory.getLogger(WebServerService.class);
    Server server;

    public WebServerService() throws IOException {
        logger.info("Creating WebServerHandler Service...");
        //coding ...
        logger.info("WebServerHandler Service Created");
        server = new Server(9090);
        server.setHandler(new WebServerHandler());
    }

    public WebServerService(Handler handler) throws IOException {
        logger.info("Creating WebServerHandler Service...");
        //coding ...
        logger.info("WebServerHandler Service Created");
        server = new Server(8080);
        server.setHandler(new HandlerList(handler, new WebServerHandler()));
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