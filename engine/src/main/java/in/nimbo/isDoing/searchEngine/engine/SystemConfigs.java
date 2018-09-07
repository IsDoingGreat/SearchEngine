package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class SystemConfigs implements Configs {
    private static final Logger logger = LoggerFactory.getLogger(SystemConfigs.class);
    private volatile Properties configs = null;
    private String name;
    private Path loadedPath = null;

    public SystemConfigs(String name) throws Exception {
        this.name = name;
        load();
        logger.info("config file loaded {}", configs);
    }

    @Override
    public void load() throws Exception {
        Properties newConfigs = new Properties(configs);
        if (System.getenv("IS_DOING_CONFIG_DIR") != null) {
            Path systemConfigDir = Paths.get(System.getenv("IS_DOING_CONFIG_DIR"));
            if (testConfigPaths(systemConfigDir, "System Config")) {
                loadedPath = systemConfigDir;
                Path engineFile = systemConfigDir.resolve("engine.properties");
                Path configFile = systemConfigDir.resolve(name + ".properties");
                newConfigs.load(Files.newBufferedReader(engineFile));
                newConfigs.load(Files.newBufferedReader(configFile));
            }
        } else {
            Path configDir = Paths.get("./configs/");
            if (testConfigPaths(configDir, "Local Config")) {
                loadedPath = configDir;
                Path engineFile = configDir.resolve("engine.properties");
                Path configFile = configDir.resolve(name + ".properties");
                newConfigs.load(Files.newBufferedReader(engineFile));
                newConfigs.load(Files.newBufferedReader(configFile));
            } else {
                throw new RuntimeException("Cant Find Any Configs!!");
            }
        }
        configs = newConfigs;
    }

    private boolean testConfigPaths(Path dir, String type) {
        Path engineFile = dir.resolve("engine.properties");
        Path configFile = dir.resolve(name + ".properties");

        if (!Files.exists(dir)) {
            logger.warn(type + " Path Does Not Exists");
            return false;
        } else if (!Files.exists(engineFile)) {
            logger.warn(engineFile + " Path Does Not Exists");
            return false;
        } else if (!Files.exists(configFile)) {
            logger.warn(configFile + " Path Does Not Exists");
            return false;
        }

        return true;
    }

    @Override
    public String get(String key) {
        String property = configs.getProperty(key);
        if (property != null)
            return property;

        throw new RuntimeException("Config {" + key + "} Not Found");
    }

    @Override
    public String get(String key, String value) {
        return configs.getProperty(key, value);
    }

    @Override
    public Path getLoadedPath() {
        return loadedPath;
    }

    @Override
    public Properties getMap() {
        return configs;
    }
}
