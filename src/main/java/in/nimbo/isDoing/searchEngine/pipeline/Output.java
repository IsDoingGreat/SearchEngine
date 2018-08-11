package in.nimbo.isDoing.searchEngine.pipeline;

import in.nimbo.isDoing.searchEngine.engine.Status;

public interface Output {
    void show(String object);

    void show(Type type, String object);

    void show(Status status);

    enum Type {INFO, ERROR,WARN,STATUS,SUCCESS}
}
