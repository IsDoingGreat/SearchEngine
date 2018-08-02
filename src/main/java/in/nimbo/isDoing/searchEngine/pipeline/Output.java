package in.nimbo.isDoing.searchEngine.pipeline;

public interface Output {
    void show(String object);

    void show(Type type, String object);

    enum Type {INFO, ERROR}
}
