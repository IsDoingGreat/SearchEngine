package in.nimbo.isDoing.searchEngine.web_server.controller;

import in.nimbo.isDoing.searchEngine.engine.Engine;
import in.nimbo.isDoing.searchEngine.engine.Status;
import in.nimbo.isDoing.searchEngine.pipeline.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class CrawlerStatus implements WebController {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerStatus.class);
    private HttpServletResponse response;

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            this.response = response;
            Status status = Engine.getInstance().getService("crawler").status();
            if (request.getParameter("c") != null) {
                Engine.getOutput().show(status);
            }
            response.getWriter().println("<pre>");
            show(status, 0);
        } catch (Exception e) {
            logger.error("Error", e);
            response.getWriter().println("Error Happend:" + e.getMessage());
        }
    }

    private void show(Status status, int depth) throws IOException {
        if (status == null)
            return;

        String tabs = new String(new char[depth]).replace("\0", "\t");
        response.getWriter().println("[" + Output.Type.STATUS + "] " + tabs + status.getTitle() + " : " + status.getDescription());

        for (String line : status.getLines()) {
            response.getWriter().println(tabs + line);
        }

        for (Status subSection : status.getSubSections()) {
            show(subSection, depth + 1);
        }
        response.getWriter().println();
    }
}
