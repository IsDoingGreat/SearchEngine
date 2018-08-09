package in.nimbo.isDoing.searchEngine.crawler.presister;

import in.nimbo.isDoing.searchEngine.crawler.page.Page;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.pipeline.Output;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class SeedPersistor implements PagePersister {
    private Map<String, String> seeds = new ConcurrentHashMap<>();

    public SeedPersistor() {
        Engine.getOutput().show("Starting Seed Generator");
    }

    @Override
    public void insert(Page page) throws Exception {
        seeds.putIfAbsent(page.getUrl().getHost(), page.getUrl().toExternalForm());
    }

    @Override
    public void stop() {
        Engine.getOutput().show("Stopping Seed Generator");
        try {
            Path path = Paths.get("./generatedSeed.txt");
            Files.write(path, seeds.values(), StandardOpenOption.CREATE_NEW);
        } catch (Exception e) {
            Engine.getOutput().show(Output.Type.ERROR, e.getMessage());
        }
    }
}
