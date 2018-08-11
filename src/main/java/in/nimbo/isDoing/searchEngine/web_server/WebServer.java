package in.nimbo.isDoing.searchEngine.web_server;



import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class WebServer extends AbstractHandler
{
    final String greeting;
    final String body;

    public WebServer()
    {
        this("Hello World");
    }

    public WebServer( String greeting )
    {
        this(greeting, null);
    }

    public WebServer( String greeting, String body )
    {
        this.greeting = greeting;
        this.body = body;
    }

    @Override
    public void handle( String target,
                        Request baseRequest,
                        HttpServletRequest request,
                        HttpServletResponse response ) throws IOException, ServletException {
        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter out = response.getWriter();

        out.println("<h1>" + greeting + "</h1>");
        if (body != null)
        {
            out.println(body);
        }

        out.println(request.getParameterMap());
        baseRequest.setHandled(true);
    }
}