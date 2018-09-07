package in.nimbo.isDoing.searchEngine.crawler.server;

import in.nimbo.isDoing.searchEngine.crawler.CrawlerService;
import in.nimbo.isDoing.searchEngine.engine.Engine;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ReloadServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) {
        CrawlerService crawler = (CrawlerService) Engine.getInstance().getService("crawler");
        try {
            if (req.getParameter("cmd").equals("true")) {
                Engine.getConfigs().load();
                resp.getWriter().print("cmd=true should be passed");
            }
            else {
                resp.getWriter().print("cmd=true should be passed");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
