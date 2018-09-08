package in.nimbo.isDoing.searchEngine.web_server;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.SystemConfigs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.pipeline.Console.ConsoleOutput;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class WebServerService implements Service {

    private final static Logger logger = LoggerFactory.getLogger(WebServerService.class);
    Server server;
    public WebServerService() {
        logger.info("Creating WebServerHandler Service...");
        String rootDir = Engine.getConfigs().get("webserver.root");
        if(!Files.isDirectory(Paths.get(rootDir))) {
            logger.error("root does not exists");
            Engine.getOutput().show(Output.Type.ERROR, "root Does Not Exists");
            return;
        }
        server = new Server(Integer.parseInt(Engine.getConfigs().get("webserver.port")));
        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(Paths.get(rootDir).toAbsolutePath().toString());
        server.setHandler(context);


        context.addServlet(ShowTwitterTrendsServlet.class, "/tweetTrends/");
        context.addServlet(ShowNewsTrendsServlet.class, "/newsTrends/");
        context.addServlet(WebSearchServlet.class, "/search/");
        context.addServlet(NewsSearchServlet.class, "/news/");
        context.addServlet(SystemStatusServlet.class, "/status/*");
        context.addServlet(DefaultServlet.class, "/");
        logger.info("WebServerHandler Service Created");
    }

    public static void main(String[] args) throws Exception {
        Engine.start(new ConsoleOutput(), new SystemConfigs("webserver"));
        Engine.getInstance().startService(new WebServerService());
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
    public Map<String, Object> status() {
        return null;
    }

    @Override
    public String getName() {
        return "webServer";
    }
}