package in.nimbo.isDoing.searchEngine.crawler.server;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.Engine;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public class ConfigsServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (req.getParameter("reload") != null && req.getParameter("reload").equals("true")) {
            try {
                Engine.getConfigs().load();
            } catch (Exception e) {
                resp.getWriter().print(e.getMessage());
            }
        } else {
            Map map = Engine.getConfigs().getMap();
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(map);
            resp.setContentType("application/json");
            resp.setContentLength(json.getBytes().length);
            resp.getWriter().write(json);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if (req.getParameter("option") == null || req.getParameter("option").isEmpty())
            resp.getWriter().print("Error, pass option");

        if (req.getParameter("value") == null || req.getParameter("value").isEmpty())
            resp.getWriter().print("Error, pass value");

        Engine.getConfigs().getMap().setProperty(req.getParameter("option"), req.getParameter("value"));
        CrawlerService crawler = (CrawlerService) Engine.getInstance().getService("crawler");
        crawler.reload();
        resp.getWriter().write(req.getParameter("option")+"="+ req.getParameter("value"));
    }
}
