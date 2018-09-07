package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.engine.interfaces.Stateful;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Status {
    private String title;
    private String description;
    private List<String> lines = new ArrayList<>();
    private List<Map<String, Object>> subSections = new ArrayList<Map<String, Object>>();

    public Status(String title, String description, String status, List<Status> subSections) {
        this.title = title;
        this.description = description;
    }

    public Status(String title, String description) {
        this.title = title;
        this.description = description;
    }

    public static Map<String, Object> get(Object object) {
        if (object instanceof Stateful)
            return ((Stateful) object).status();
        else
            return null;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getLines() {
        return lines;
    }

    public void addLine(String status) {
        lines.add(status);
    }

    public List<Map<String, Object>> getSubSections() {
        return subSections;
    }

    public void addSubSections(Map<String, Object> subSection) {
        if (subSection == null)
            return;

        this.subSections.add(subSection);
    }
}
