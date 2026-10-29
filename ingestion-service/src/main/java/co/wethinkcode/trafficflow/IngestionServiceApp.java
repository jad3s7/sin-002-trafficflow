package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IngestionServiceApp {

    private static List<Intersection> finalCleanedData = new ArrayList<>();

    private static final Set<String> TRUE_VALUES  = Set.of("y", "yes", "1", "true",  "t");
    private static final Set<String> FALSE_VALUES = Set.of("n", "no",  "0", "false", "f");

    static class Intersection {
        String id;
        String district;
        String signalType;
        Boolean active;
        List<String> missingFields;

        Intersection() { this.missingFields = new ArrayList<>(); }

        Intersection(String id, String district, String signalType, Boolean active) {
            this.id = id;
            this.district = district;
            this.signalType = signalType;
            this.active = active;
            this.missingFields = new ArrayList<>();
            if (id == null || id.isEmpty())  missingFields.add("id");
            if (district == null)             missingFields.add("district");
            if (signalType == null)           missingFields.add("signalType");
            if (active == null)               missingFields.add("active");
        }

        public String getId()                   { return id; }
        public String getDistrict()             { return district; }
        public String getSignalType()           { return signalType; }
        public Boolean getActive()              { return active; }
        public List<String> getMissingFields()  { return missingFields; }
    }

    public static void main(String[] args) {
        boolean loaded = false;
        try {
            finalCleanedData = loadIntersections();
            loaded = true;
        } catch (Exception e) {
            System.err.println("[ingestion] failed to load intersections: " + e.getMessage());
            e.printStackTrace();
        }
        final boolean dataLoaded = loaded;

        Javalin app = Javalin.create().start(7020);

        // Catch-all error envelope.
        app.exception(Exception.class, (e, ctx) -> {
            System.err.println("[ingestion] unhandled exception: " + e);
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "internal server error"));
        });
        app.error(404, ctx -> ctx.status(404).json(
                Map.of("error", "not found", "path", ctx.path())));
        app.error(405, ctx -> ctx.status(405).json(
                Map.of("error", "method not allowed",
                        "method", ctx.req().getMethod(),
                        "path",   ctx.path())));

        app.get("/health", ctx -> {
            if (dataLoaded) ctx.result("OK");
            else            ctx.status(503).result("INTERSECTION DATA NOT LOADED");
        });

        app.get("/intersections", ctx -> {
            if (!dataLoaded) {
                ctx.status(503).json(Map.of("error", "Intersection data not loaded"));
                return;
            }
            ctx.json(finalCleanedData);
        });
    }

    private static List<Intersection> loadIntersections() throws Exception {
        InputStream csvStream = IngestionServiceApp.class.getClassLoader()
                .getResourceAsStream("intersections-legacy.csv");
        if (csvStream == null) {
            throw new IllegalStateException("intersections-legacy.csv not found on classpath");
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream));
        reader.readLine();

        List<Intersection> cleanedRecords = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");

            String rawId         = values.length > 0 ? values[0] : "";
            String rawDistrict   = values.length > 1 ? values[1] : "";
            String rawSignalType = values.length > 2 ? values[2] : "";
            String rawActiveFlag = values.length > 3 ? values[3] : "";

            String trimmedId         = normalizeWhitespace(rawId);
            String trimmedDistrict   = normalizeWhitespace(rawDistrict);
            String trimmedSignalType = normalizeWhitespace(rawSignalType);
            String trimmedActiveFlag = normalizeWhitespace(rawActiveFlag);

            String normalizedId         = normalizeId(trimmedId);
            String normalizedDistrict   = normalizeDistrict(trimmedDistrict);
            String normalizedSignalType = normalizeSignalType(trimmedSignalType);
            Boolean normalizedActive    = normalizeBoolean(trimmedActiveFlag);

            Intersection record = new Intersection(
                    normalizedId, normalizedDistrict, normalizedSignalType, normalizedActive);
            if (!record.missingFields.isEmpty()) {
                System.out.println("[ingestion] incomplete record id="
                        + (record.id == null ? "<none>" : record.id)
                        + " missing=" + record.missingFields);
            }
            cleanedRecords.add(record);
        }

        List<Intersection> result = deduplicate(cleanedRecords);

        long incomplete = result.stream()
                .filter(r -> !r.missingFields.isEmpty())
                .count();
        System.out.println("[ingestion] parsed " + cleanedRecords.size()
                + " rows; " + result.size() + " unique intersections; "
                + incomplete + " incomplete");

        return result;
    }

    // --- Dedup + casing + whitespace + boolean + placeholder helpers ---
    // (unchanged from previous sub-step — see earlier commit)

    private static List<Intersection> deduplicate(List<Intersection> records) {
        Map<String, Intersection> byId = new LinkedHashMap<>();
        int anonIdCounter = 0;
        for (Intersection r : records) {
            if (r.id == null || r.id.isEmpty()) {
                byId.put("__anon__" + (anonIdCounter++), r);
                continue;
            }
            Intersection existing = byId.get(r.id);
            if (existing == null) byId.put(r.id, r);
            else {
                System.out.println("[ingestion] duplicate id '" + r.id + "' — merging");
                byId.put(r.id, merge(existing, r));
            }
        }

        Map<String, Intersection> byFingerprint = new LinkedHashMap<>();
        int anonFpCounter = 0;
        for (Intersection r : byId.values()) {
            String fp = fingerprint(r);
            if (fp == null) {
                String key = (r.id != null)
                        ? ("__id__:" + r.id)
                        : ("__anonfp__" + (anonFpCounter++));
                byFingerprint.put(key, r);
                continue;
            }
            Intersection existing = byFingerprint.get(fp);
            if (existing == null) byFingerprint.put(fp, r);
            else {
                System.out.println("[ingestion] suspected duplicate (fingerprint '"
                        + fp + "'): keeping id " + existing.id
                        + ", merging in id " + r.id);
                byFingerprint.put(fp, merge(existing, r));
            }
        }

        return new ArrayList<>(byFingerprint.values());
    }

    private static String fingerprint(Intersection r) {
        if (r.district == null || r.signalType == null) return null;
        return r.district + "|" + r.signalType;
    }

    private static Intersection merge(Intersection earlier, Intersection later) {
        String id = earlier.id != null ? earlier.id : later.id;

        String district = earlier.district != null ? earlier.district : later.district;
        if (earlier.district != null && later.district != null
                && !earlier.district.equals(later.district)) {
            System.out.println("[ingestion] district conflict on " + id
                    + ": '" + earlier.district + "' vs '" + later.district
                    + "' — keeping '" + earlier.district + "'");
        }

        String signalType = earlier.signalType != null ? earlier.signalType : later.signalType;
        if (earlier.signalType != null && later.signalType != null
                && !earlier.signalType.equals(later.signalType)) {
            System.out.println("[ingestion] signalType conflict on " + id
                    + ": '" + earlier.signalType + "' vs '" + later.signalType
                    + "' — keeping '" + earlier.signalType + "'");
        }

        Boolean active = earlier.active != null ? earlier.active : later.active;
        if (earlier.active != null && later.active != null
                && !earlier.active.equals(later.active)) {
            System.out.println("[ingestion] active conflict on " + id
                    + ": " + earlier.active + " vs " + later.active
                    + " — keeping " + earlier.active);
        }

        return new Intersection(id, district, signalType, active);
    }

    private static String normalizeId(String raw) {
        if (isMissingOrPlaceholder(raw)) return null;
        return raw.toUpperCase();
    }

    private static String normalizeDistrict(String raw) {
        if (isMissingOrPlaceholder(raw)) return null;
        return toTitleCase(raw);
    }

    private static String normalizeSignalType(String raw) {
        if (isMissingOrPlaceholder(raw)) return null;
        return raw.toLowerCase();
    }

    private static Boolean normalizeBoolean(String flag) {
        if (isMissingOrPlaceholder(flag)) return null;
        String normalized = flag.toLowerCase();
        if (TRUE_VALUES.contains(normalized))  return true;
        if (FALSE_VALUES.contains(normalized)) return false;
        System.out.println("[ingestion] unrecognised active_flag value '"
                + flag + "' — treating as unknown");
        return null;
    }

    private static String normalizeWhitespace(String input) {
        if (input == null) return null;
        return input.strip().replaceAll("\\p{javaWhitespace}+", " ");
    }

    private static boolean isMissingOrPlaceholder(String value) {
        if (value == null || value.isEmpty()) return true;
        String normalized = value.toLowerCase();
        return normalized.equals("unknown") || normalized.equals("n/a")
                || normalized.equals("tbd")     || normalized.equals("-")
                || normalized.equals("nan")     || normalized.equals("null")
                || normalized.equals("nil")     || normalized.equals("none")
                || normalized.equals("undefined");
    }

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
}