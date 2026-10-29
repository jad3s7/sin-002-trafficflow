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
    // so it can be served by the REST endpoint.
    private static List<Intersection> finalCleanedData = new ArrayList<>();

    // Simple data class to hold a cleaned intersection record.
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

        InputStream csvStream = IngestionServiceApp.class.getClassLoader()
                .getResourceAsStream("intersections-legacy.csv");

        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
        try {
            // Consume the header row.
            reader.readLine();

            List<Intersection> cleanedRecords = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");

                String rawId         = values.length > 0 ? values[0] : "";
                String rawDistrict   = values.length > 1 ? values[1] : "";
                String rawSignalType = values.length > 2 ? values[2] : "";
                String rawActiveFlag = values.length > 3 ? values[3] : "";

                // Trim padding / collapse double spaces first.
                String trimmedId         = trimAllSpaces(rawId);
                String trimmedDistrict   = trimAllSpaces(rawDistrict);
                String trimmedSignalType = trimAllSpaces(rawSignalType);
                String trimmedActiveFlag = trimAllSpaces(rawActiveFlag);

                // Casing normalization — collapse each field to one canonical form:
                //   id         -> UPPERCASE       ("int-1001"       -> "INT-1001")
                //   district   -> Title Case      ("DOWNTOWN plaza" -> "Downtown Plaza")
                //   signalType -> lowercase       ("4-WAY"          -> "4-way")
                String normalizedId         = normalizeId(trimmedId);
                String normalizedDistrict   = normalizeDistrict(trimmedDistrict);
                String normalizedSignalType = normalizeSignalType(trimmedSignalType);

                Boolean normalizedActive = normalizeBoolean(trimmedActiveFlag);

                cleanedRecords.add(new Intersection(
                        normalizedId,
                        normalizedDistrict,
                        normalizedSignalType,
                        normalizedActive
                ));
            }

            // Deduplicate by (already-normalized) id — first occurrence wins.
            Map<String, Intersection> deduplicated = new HashMap<>();
            for (Intersection record : cleanedRecords) {
                deduplicated.putIfAbsent(record.id, record);
            }
            finalCleanedData = new ArrayList<>(deduplicated.values());

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Expose cleaned data for other services to consume.
        app.get("/intersections", ctx -> ctx.json(finalCleanedData));
    }

    // --- Casing normalization helpers ------------------------------------

    /** IDs -> UPPERCASE, so "int-1001" and "INT-1001" collapse to one form. */
    private static String normalizeId(String raw) {
        return raw == null ? "" : raw.toUpperCase();
    }

    /** Districts -> per-word Title Case, e.g. "DOWNTOWN plaza" -> "Downtown Plaza". */
    private static String normalizeDistrict(String raw) {
        if (isMissingOrPlaceholder(raw)) return null;
        return toTitleCase(raw);
    }

    /** Signal types -> lowercase, so "4-WAY" and "4-way" collapse to one form. */
    private static String normalizeSignalType(String raw) {
        if (isMissingOrPlaceholder(raw)) return null;
        return raw.toLowerCase();
    }

    // --- Generic helpers -------------------------------------------------

    private static String trimAllSpaces(String input) {
        if (input == null) return null;
        return input.trim().replaceAll("\\s+", " ");
    }

    private static boolean isMissingOrPlaceholder(String value) {
        if (value == null || value.isEmpty()) return true;
        String normalized = value.toLowerCase();
        return normalized.equals("unknown")
                || normalized.equals("n/a")
                || normalized.equals("tbd")
                || normalized.equals("-")
                || normalized.equals("nan");
    }

    /**
     * Title-case each whitespace-separated word:
     *   "DOWNTOWN"        -> "Downtown"
     *   "DOWNTOWN plaza"  -> "Downtown Plaza"
     *   "north side"      -> "North Side"
     */
    private static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) return null;
        String[] words = input.toLowerCase().split("\\s+");
        StringBuilder sb = new StringBuilder(input.length());
        for (String word : words) {
            if (word.isEmpty()) continue;
            if (sb.length() > 0) sb.append(' ');
            sb.append(Character.toUpperCase(word.charAt(0)));
            sb.append(word.substring(1));
        }
        return sb.toString();
    }

    private static Boolean normalizeBoolean(String flag) {
        if (isMissingOrPlaceholder(flag)) return null;
        String normalized = flag.toLowerCase();
        if (normalized.equals("y") || normalized.equals("yes")
                || normalized.equals("1") || normalized.equals("true")) {
            return true;
        }
        if (normalized.equals("n") || normalized.equals("no")
                || normalized.equals("0") || normalized.equals("false")) {
            return false;
        }
        return null;
    }
}