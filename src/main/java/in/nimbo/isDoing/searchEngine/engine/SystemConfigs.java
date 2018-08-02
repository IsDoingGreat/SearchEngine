package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.engine.interfaces.Configs;

import java.io.IOException;
import java.util.Properties;

public class SystemConfigs implements Configs {
    private Properties configs = new Properties();
    public SystemConfigs() throws IOException {
        configs.load(getClass().getResourceAsStream("/configs.properties"));
        configs.setProperty("exitRequested", "false");
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
}
