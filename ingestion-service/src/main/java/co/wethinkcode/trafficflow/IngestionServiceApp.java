package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

                // Step 2: trim padding / collapse double spaces.
                String trimmedId         = normalizeWhitespace(rawId);
                String trimmedDistrict   = normalizeWhitespace(rawDistrict);
                String trimmedSignalType = normalizeWhitespace(rawSignalType);
                String trimmedActiveFlag = normalizeWhitespace(rawActiveFlag);

                // Step 1: casing normalization — collapse each field to one form:
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

            // Step 3: detect and collapse duplicate records for the same
            // real-world intersection. See `deduplicate` for the strategy.
            finalCleanedData = deduplicate(cleanedRecords);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Expose cleaned data for other services to consume.
        app.get("/intersections", ctx -> ctx.json(finalCleanedData));
    }

    // --- Step 3: duplicate detection & collapse ---------------------------

    /**
     * Collapse records that represent the same real-world intersection.
     *
     * Pass 1 — same normalized id. Strongest signal; id is the primary key.
     * Pass 2 — same (district, signalType) across different ids. Softer signal
     *          that catches id typos; every collapse is logged so it is
     *          observable and reversible if the heuristic misfires for a
     *          particular dataset.
     *
     * Order is preserved (LinkedHashMap) so the output is deterministic:
     * the earliest surviving record keeps its position.
     */
    private static List<Intersection> deduplicate(List<Intersection> records) {
        // Pass 1: collapse on normalized id.
        Map<String, Intersection> byId = new LinkedHashMap<>();
        for (Intersection r : records) {
            if (r.id == null || r.id.isEmpty()) {
                // No id — nothing to key on; let it through to the
                // fingerprint pass below (which will also skip it).
                byId.put("__anon__" + byId.size(), r);
                continue;
            }
            Intersection existing = byId.get(r.id);
            if (existing == null) {
                byId.put(r.id, r);
            } else {
                System.out.println("[ingestion] duplicate id '" + r.id + "' — merging");
                byId.put(r.id, merge(existing, r));
            }
        }

        // Pass 2: collapse on (district, signalType) across differing ids.
        Map<String, Intersection> byFingerprint = new LinkedHashMap<>();
        for (Intersection r : byId.values()) {
            String fp = fingerprint(r);
            if (fp == null) {
                // No usable fingerprint (district or signalType missing) —
                // we can't confidently say it's a duplicate, so keep it.
                byFingerprint.put("__id__:" + r.id, r);
                continue;
            }
            Intersection existing = byFingerprint.get(fp);
            if (existing == null) {
                byFingerprint.put(fp, r);
            } else {
                System.out.println("[ingestion] suspected duplicate (fingerprint '"
                        + fp + "'): keeping id " + existing.id
                        + ", merging in id " + r.id);
                byFingerprint.put(fp, merge(existing, r));
            }
        }

        return new ArrayList<>(byFingerprint.values());
    }

    /**
     * Fingerprint used for the softer "same real-world intersection" check.
     * Deliberately ignores `active` (the field most likely to be stale between
     * duplicate rows) and returns null when we can't say anything useful.
     */
    private static String fingerprint(Intersection r) {
        if (r.district == null || r.signalType == null) return null;
        return r.district + "|" + r.signalType;
    }

    /**
     * Combine two records that have been classified as duplicates:
     *   - keep the earlier record's non-null values
     *   - fill any nulls from the later record
     *   - log any non-null conflict so it's visible in the ingestion output
     *
     * "First occurrence wins" on genuine conflicts keeps the behaviour
     * deterministic and matches the CSV row order.
     */
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

    // --- Whitespace normalization ----------------------------------------

    /**
     * Normalize a raw CSV cell's whitespace:
     *   - strip leading and trailing whitespace
     *   - collapse runs of internal whitespace to a single space
     *
     * Unicode-aware via String#strip() + \p{javaWhitespace}+ so it also covers
     * tabs, newlines, and non-ASCII spaces — not just the ASCII space character.
     *
     *   "  downtown  plaza  "   -> "downtown plaza"
     *   "\tNorth\t\tSide\n"     -> "North Side"
     *   "   "                    -> ""
     *   null                     -> null
     */
    private static String normalizeWhitespace(String input) {
        if (input == null) return null;
        return input.strip().replaceAll("\\p{javaWhitespace}+", " ");
    }

    // --- Generic helpers -------------------------------------------------

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