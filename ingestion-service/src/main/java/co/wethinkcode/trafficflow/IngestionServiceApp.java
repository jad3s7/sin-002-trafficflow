package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.io.InputStream;

public class IngestionServiceApp {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7020);

        app.get("/health", ctx -> ctx.result("OK"));

        // Line 1: Open CSV file from resources
        // Opened the intersections-legacy.csv file from the resources folder using ClassLoader
        // to get an InputStream, which can be used to read the CSV data
        InputStream csvStream = IngestionServiceApp.class.getClassLoader()
                .getResourceAsStream("intersections-legacy.csv");

        // TODO: read and clean src/main/resources/intersections-legacy.csv (intersections, districts, signal types data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
    }
}
