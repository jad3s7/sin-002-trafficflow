package co.wethinkcode.trafficflow.model;

public class RouteRequest {
    public String from;
    public String to;

    public RouteRequest() {}

    public RouteRequest(String from, String to) {
        this.from = from;
        this.to = to;
    }
}
