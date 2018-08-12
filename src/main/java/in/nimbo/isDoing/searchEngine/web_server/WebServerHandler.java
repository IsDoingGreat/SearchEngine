package in.nimbo.isDoing.searchEngine.web_server;


import in.nimbo.isDoing.searchEngine.web_server.controller.webSearch;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WebServerHandler extends AbstractHandler {
    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        if (target.equals("/"))
            new webSearch().handle(request,response);
        else
            response.getWriter().println(target);


//        out.println(request.getParameterMap());
        baseRequest.setHandled(true);
    }
}