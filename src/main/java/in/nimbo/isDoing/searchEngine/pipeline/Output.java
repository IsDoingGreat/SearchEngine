package in.nimbo.isDoing.searchEngine.pipeline;

public interface Output {
    void out(String object);

    void out(Type type, String object);

    enum Type {INFO, ERROR}
}
