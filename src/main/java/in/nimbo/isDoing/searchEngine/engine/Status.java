package in.nimbo.isDoing.searchEngine.engine;

import in.nimbo.isDoing.searchEngine.engine.interfaces.HaveStatus;

import java.util.ArrayList;
import java.util.List;

public class Status {
    private String title;
    private String description;
    private List<String> lines = new ArrayList<>();
    private List<Status> subSections = new ArrayList<>();

    public Status(String title, String description, String status, List<Status> subSections) {
        this.title = title;
        this.description = description;
    }

    public Status(String title, String description) {
        this.title = title;
        this.description = description;
    }

    public static Status get(Object object) {
        if (object instanceof HaveStatus)
            return ((HaveStatus) object).status();
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

    public List<Status> getSubSections() {
        return subSections;
    }

    public void addSubSections(Status subSection) {
        if (subSection == null)
            return;

        this.subSections.add(subSection);
    }
}
