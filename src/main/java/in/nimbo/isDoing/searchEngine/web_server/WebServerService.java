package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.jetty.server.Server;

import java.io.IOException;

public class WebServerService implements Service {
    private final static Logger logger = LoggerFactory.getLogger(in.nimbo.isDoing.searchEngine.web_server.WebServerService.class);
    Server server = new Server(8080);

    public WebServerService() throws IOException {
        logger.info("Creating WebServer Service...");
        //coding ...
        logger.info("WebServer Service Created");
    }

    @Override
    public void start() {
        logger.info("Starting WebServer Service...");
        Engine.getOutput().show("Starting WebServer Service...");

        server.setHandler(new WebServer());
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        //twitterReader.stopGetTweets();
        logger.info("WebServer Service Stopped");
        Engine.getOutput().show("WebServer Service Stopped");
    }

    @Override
    public Status status() {
        return null;
    }

    @Override
    public String getName() {
        return "WebServer";
    }
}