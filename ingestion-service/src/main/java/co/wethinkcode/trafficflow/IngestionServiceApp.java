package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class IngestionServiceApp {

    // Class-level variable to store the cleaned, deduplicated intersection data
    // This allows the data to be accessible by the REST endpoint
    private static List<Intersection> finalCleanedData = new ArrayList<>();

    // Simple data class to hold cleaned intersection record
    static class Intersection {
        String id;
        String district;
        String signalType;
        Boolean active;

        Intersection() {}

        Intersection(String id, String district, String signalType, Boolean active) {
            this.id = id;
            this.district = district;
            this.signalType = signalType;
            this.active = active;
        }

        public String getId() { return id; }
        public String getDistrict() { return district; }
        public String getSignalType() { return signalType; }
        public Boolean getActive() { return active; }
    }

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7020);

        app.get("/health", ctx -> ctx.result("OK"));

        // Line 1: Open CSV file from resources
        // Opened the intersections-legacy.csv file from the resources folder using ClassLoader
        // to get an InputStream, which can be used to read the CSV data
        InputStream csvStream = IngestionServiceApp.class.getClassLoader()
                .getResourceAsStream("intersections-legacy.csv");

        // Line 2: Read the header row to identify column names
        // Wrapped the InputStream in a BufferedReader to read the file line by line
        // Read the first line (header) and split it by comma to get column names
        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
        try {
            String headerLine = reader.readLine();
            String[] columnNames = headerLine.split(",");

            // Line 3: For each data row:
            // Extract raw values for intersection_id, district, signal_type, active_flag
            // Apply trimming to remove whitespace
            // Apply casing normalization (IDs to uppercase, names to title case)
            // Parse boolean flag using a mapping function
            // Store cleaned record in a collection
            List<Intersection> cleanedRecords = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");

                // Extract raw values (handling potential array index issues)
                String rawId = values.length > 0 ? values[0] : "";
                String rawDistrict = values.length > 1 ? values[1] : "";
                String rawSignalType = values.length > 2 ? values[2] : "";
                String rawActiveFlag = values.length > 3 ? values[3] : "";

                // Apply trimming to remove whitespace (leading, trailing, and double spaces)
                String trimmedId = trimAllSpaces(rawId);
                String trimmedDistrict = trimAllSpaces(rawDistrict);
                String trimmedSignalType = trimAllSpaces(rawSignalType);
                String trimmedActiveFlag = trimAllSpaces(rawActiveFlag);

                // Check for empty strings and placeholder values in district and signal type fields
                // before applying casing normalization - return null for these cases
                String normalizedDistrict = isMissingOrPlaceholder(trimmedDistrict) ? null : toTitleCase(trimmedDistrict);
                String normalizedSignalType = isMissingOrPlaceholder(trimmedSignalType) ? null : trimmedSignalType.toLowerCase();

                // Apply casing normalization (IDs to uppercase)
                String normalizedId = trimmedId.toUpperCase();

                // Parse boolean flag using a mapping function
                Boolean normalizedActive = normalizeBoolean(trimmedActiveFlag);

                // Store cleaned record in a collection
                Intersection record = new Intersection(
                    normalizedId,
                    normalizedDistrict,
                    normalizedSignalType,
                    normalizedActive
                );
                cleanedRecords.add(record);
            }

            // Line 4: Remove duplicates by normalizing IDs and comparing other fields
            // Use a Map with normalized ID as key to automatically handle duplicates
            // If the same ID appears again (with different casing), only keep one record
            Map<String, Intersection> deduplicatedMap = new HashMap<>();
            for (Intersection record : cleanedRecords) {
                String normalizedId = record.id.toUpperCase();
                // Only add if not already present (first occurrence wins)
                if (!deduplicatedMap.containsKey(normalizedId)) {
                    deduplicatedMap.put(normalizedId, record);
                }
            }
            // Convert map values back to a list
            List<Intersection> deduplicatedRecords = new ArrayList<>(deduplicatedMap.values());

            // Line 5: Return the cleaned, deduplicated list
            // Store the final cleaned and deduplicated data in the class-level variable
            // so it can be accessed by the REST endpoint for other services to consume
            finalCleanedData = deduplicatedRecords;

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Expose the cleaned data via REST endpoint for other services to consume
        app.get("/intersections", ctx -> ctx.json(finalCleanedData));
    }

    // Helper method to trim all spaces (leading, trailing, and double spaces)
    private static String trimAllSpaces(String input) {
        if (input == null) {
            return null;
        }
        return input.trim().replaceAll("\\s+", " ");
    }

    // Helper method to check if a value is missing or a placeholder
    // Returns true for empty strings, null, or known placeholder values
    private static boolean isMissingOrPlaceholder(String value) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        String normalized = value.toLowerCase();
        return normalized.equals("unknown") ||
               normalized.equals("n/a") ||
               normalized.equals("tbd") ||
               normalized.equals("-") ||
               normalized.equals("nan");
    }

    // Helper method to convert string to title case
    private static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }
        return input.substring(0, 1).toUpperCase() + input.substring(1).toLowerCase();
    }

    // Helper method to normalize boolean values
    private static Boolean normalizeBoolean(String flag) {
        if (isMissingOrPlaceholder(flag)) {
            return null;
        }
        String normalized = flag.toLowerCase();
        if (normalized.equals("y") || normalized.equals("yes") || normalized.equals("1") || normalized.equals("true")) {
            return true;
        }
        if (normalized.equals("n") || normalized.equals("no") || normalized.equals("0") || normalized.equals("false")) {
            return false;
        }
        return null;
    }
}
