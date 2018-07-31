package in.nimbo.isDoing.searchEngine;

import in.nimbo.isDoing.searchEngine.interfaces.UrlQueue;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class App implements Runnable {

    public static void main(String[] args) throws IOException, InterruptedException {
//        Document doc = Jsoup.connect(new URL("https://medium.com/").toExternalForm()).validateTLSCertificates(false).get();
//        System.out.println(doc.text());
        new Crawler().start();
        Thread.sleep(1000);
    }

    @Override
    public void run() {
        initSystem();
    }


    public void initSystem() {

    }
}
