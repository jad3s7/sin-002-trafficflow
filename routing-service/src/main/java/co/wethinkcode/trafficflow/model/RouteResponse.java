package co.wethinkcode.trafficflow.model;

public class RouteResponse {
    public String from;
    public String to;
    public int congestionLevel;
    public int estimatedTravelTime;
    public String status;

    public RouteResponse() {}

    public RouteResponse(String from, String to, int congestionLevel, int estimatedTravelTime, String status) {
        this.from = from;
        this.to = to;
        this.congestionLevel = congestionLevel;
        this.estimatedTravelTime = estimatedTravelTime;
        this.status = status;
    }
}
