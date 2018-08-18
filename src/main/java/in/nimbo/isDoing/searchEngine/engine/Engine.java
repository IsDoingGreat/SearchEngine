package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.elastic.ElasticClient;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import in.nimbo.isDoing.searchEngine.engine.interfaces.Service;
import in.nimbo.isDoing.searchEngine.hbase.HBaseClient;
import in.nimbo.isDoing.searchEngine.news_reader.NewsReaderService;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import in.nimbo.isDoing.searchEngine.twitter_reader.TwitterReaderService;
import in.nimbo.isDoing.searchEngine.web_server.WebServerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static in.nimbo.isDoing.searchEngine.pipeline.Output.Type.ERROR;

public class Engine {
    private static final Logger logger = LoggerFactory.getLogger(Engine.class.getSimpleName());
    private static volatile Engine instance;
    private Output output;
    private Configs configs;
    private Map<String, Service> services = new ConcurrentHashMap<>();

    Engine(Output output, Configs config) {
        this.output = output;
        this.configs = config;
    }

    public synchronized static Engine start(Output out) throws Exception {
        return start(out, new SystemConfigs());
    }

    public synchronized static Engine start(Output out, Configs configs) throws Exception {
        if (instance != null)
            throw new RuntimeException("Engine has already started");

        instance = new Engine(out, configs);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (instance != null) {
                try {
                    Engine.getInstance().stopAll();
                    try {
                        ElasticClient.close();
                    } catch (IOException e) {
                        logger.error("Closing Elastic With Error", e);
                        Engine.getOutput().show(ERROR, "Closing Elastic With Error");
                    }

                    try {
                        HBaseClient.close();
                    } catch (IOException e) {
                        logger.error("Closing HBase With Error", e);
                        Engine.getOutput().show(ERROR, "Closing HBase With Error");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
        out.show("Server Started");
        System.out.println("Server Started");
        return instance;
    }

    public static Configs getConfigs() {
        Objects.requireNonNull(instance, "Engine Not Started");
        return instance.configs;
    }

    public static Output getOutput() {
        Objects.requireNonNull(instance, "Engine Not Started");
        return instance.output;
    }

    public static synchronized Engine getInstance() {
        if (instance == null)
            throw new RuntimeException("Engine not started");

        return instance;
    }

    public static void shutdown() {
        instance.stopAll();
        instance = null;
    }

    public void startService(String name) {
        try {
            if (services.get(name) != null) {
                output.show(Output.Type.ERROR, "service Already Running");
            } else {
                switch (name) {
                    case "crawler":
                        startService(new CrawlerService());
                        break;
                    case "twitterReader":
                        startService(new TwitterReaderService());
                        break;
                    case "webServer":
                        startService(new WebServerService());
                        break;
                    case "newsReader":
                        startService(new NewsReaderService());
                        break;
                    default:
                        output.show(Output.Type.ERROR, "Service Not Found");
                        break;
                }
            }
        } catch (Exception e) {
            output.show(Output.Type.ERROR, e.toString());
            logger.error("Error in Creating Service", e);
        }
    }

    public void startService(Service service) {
        try {
            if (services.get(service.getName()) == null) {
                service.start();
                services.put(service.getName(), service);
            } else
                output.show(Output.Type.ERROR, "service Already Running");
        } catch (Exception e) {
            logger.error("Error During starting Service.", e);
            output.show(Output.Type.ERROR, e.toString());
            output.show(Output.Type.WARN, "Stopping Service");
            service.stop();
        }

    }

    public Service getService(String name) {
        if (services.containsKey(name))
            return services.get(name);
        else
            throw new RuntimeException("Service Not Found");
    }

    public void stopAll() {
        try {
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                stopService(entry.getKey());
            }
        } catch (Exception e) {
            logger.error("Error Stopping Service", e);
            output.show("Error Stopping Service");

        }
    }

    public void stopService(String serviceName) {
        if (services.get(serviceName) == null) {
            output.show(Output.Type.ERROR, "service is not running");
            return;
        }

        try {
            services.get(serviceName).stop();
            services.entrySet().removeIf(entries -> entries.getKey().equals(serviceName));
        } catch (Exception e) {
            logger.error("Error During Stopping Service.", e);
            getOutput().show(Output.Type.ERROR, "Error During Stopping Service." +
                    "Please See Logs");
        }
    }

    public void status(Service service) {
        getOutput().show(service.status());
    }

    public void status(String service) {
        if (services.get(service) == null) {
            output.show(Output.Type.ERROR, "service not Running");
        } else {
            status(services.get(service));
        }
    }
}
