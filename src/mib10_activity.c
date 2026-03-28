#include "mib10_app.h"
#include "gb_db.h"

#include <math.h>
#include <stdio.h>

#include "xiaomi.pb-c.h"
static uint32_t crc32_ieee(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int b = 0; b < 8; b++) {
            if (crc & 1u) {
                crc = (crc >> 1) ^ 0xEDB88320u;
            } else {
                crc >>= 1;
            }
        }
    }
    return ~crc;
}

static gchar *fmt_unix_ts(uint32_t ts) {
    GDateTime *dt = g_date_time_new_from_unix_local((gint64)ts);
    if (dt == NULL) {
        return g_strdup_printf("%u", ts);
    }
    gchar *out = g_date_time_format(dt, "%Y-%m-%d %H:%M:%S");
    g_date_time_unref(dt);
    return out;
}

static void log_sleep_samples_summary(AppState *state, const char *tag, const GArray *samples) {
    if (samples == NULL || samples->len == 0) {
        gchar *line = g_strdup_printf("%s: no samples", tag);
        append_log(state, line);
        g_free(line);
        return;
    }

    gboolean seen = FALSE;
    guint first_m = 0, last_m = 0;
    guint c_deep = 0, c_light = 0, c_rem = 0, c_awake = 0, c_other = 0;
    for (guint i = 0; i < samples->len; i++) {
        const RecordedSample *s = &g_array_index(samples, RecordedSample, i);
        if (!seen || s->minute_of_day < first_m) first_m = s->minute_of_day;
        if (!seen || s->minute_of_day > last_m) last_m = s->minute_of_day;
        seen = TRUE;
        switch (s->sleep_stage) {
            case 2: c_deep++; break;
            case 3: c_light++; break;
            case 4: c_rem++; break;
            case 5: c_awake++; break;
            default: c_other++; break;
        }
    }

    gchar *line = g_strdup_printf(
        "%s: first=%02u:%02u last=%02u:%02u stages{deep=%u light=%u rem=%u awake=%u other=%u} samples=%u",
        tag,
        first_m / 60u, first_m % 60u,
        last_m / 60u, last_m % 60u,
        c_deep, c_light, c_rem, c_awake, c_other,
        samples->len
    );
    append_log(state, line);
    g_free(line);
}

static void xiaomi_complex_parser_init(XiaomiComplexParser *p, const uint8_t *header, size_t header_len) {
    memset(p, 0, sizeof(*p));
    p->header = header;
    p->header_len = header_len;
}

static void xiaomi_complex_parser_reset(XiaomiComplexParser *p) {
    p->group = 0;
    p->current_group_bits = 0;
    p->current_val = 0;
    p->current_exists = FALSE;
}

static uint8_t xiaomi_header_nibble(const XiaomiComplexParser *p, size_t group) {
    const size_t header_byte = group / 2;
    if (header_byte >= p->header_len) return 0;
    if ((group % 2) == 0) {
        return (p->header[header_byte] >> 4) & 0x0F;
    }
    return p->header[header_byte] & 0x0F;
}

static gboolean xiaomi_complex_next_group(XiaomiComplexParser *p, int nbits, const uint8_t *payload, size_t payload_len, size_t *cursor) {
    const uint8_t nibble = xiaomi_header_nibble(p, p->group);
    p->group++;
    p->current_group_bits = nbits;
    p->current_val = 0;
    p->current_exists = (nibble & 0x8) != 0;
    if (!p->current_exists) {
        return FALSE;
    }

    if (nbits == 8) {
        if (*cursor + 1 > payload_len) return FALSE;
        p->current_val = payload[*cursor];
        *cursor += 1;
        return TRUE;
    } else if (nbits == 16) {
        if (*cursor + 2 > payload_len) return FALSE;
        p->current_val = mib10_read_u16_le(payload + *cursor);
        *cursor += 2;
        return TRUE;
    } else if (nbits == 32) {
        if (*cursor + 4 > payload_len) return FALSE;
        p->current_val = mib10_read_u32_le(payload + *cursor);
        *cursor += 4;
        return TRUE;
    }
    return FALSE;
}

static gboolean xiaomi_complex_has(const XiaomiComplexParser *p, int idx) {
    if (idx < 0 || idx > 2) return FALSE;
    const uint8_t nibble = xiaomi_header_nibble(p, p->group - 1);
    return (nibble & (1u << (2 - idx))) != 0;
}

static uint32_t xiaomi_complex_get(const XiaomiComplexParser *p, int idx, int nbits) {
    const int shift = p->current_group_bits - idx - nbits;
    if (shift < 0 || nbits <= 0 || nbits >= 31) return 0;
    return (p->current_val >> shift) & ((1u << nbits) - 1u);
}

typedef struct {
    gboolean valid;
    uint32_t minutes;
    uint32_t step_points;
    uint32_t distance_points;
    uint32_t hr_points;
    size_t final_cursor;
    uint32_t score;
} DailyDetailsProbe;

static int expected_daily_details_header_size(int version) {
    if (version == 1 || version == 2) return 4;
    if (version == 3) return 5;
    if (version == 4) return 6;
    return 0;
}

static DailyDetailsProbe probe_daily_details_header(const uint8_t *payload,
                                                    size_t payload_len,
                                                    int version,
                                                    int header_size) {
    DailyDetailsProbe out = {0};
    if (header_size <= 0) return out;
    if (payload_len < (size_t)(8 + header_size + 4)) return out;

    const uint8_t *header = payload + 8;
    const size_t payload_end = payload_len - 4;
    size_t cursor = (size_t)(8 + header_size);

    XiaomiComplexParser p;
    xiaomi_complex_parser_init(&p, header, (size_t)header_size);

    while (cursor < payload_end) {
        const size_t before = cursor;
        xiaomi_complex_parser_reset(&p);

        int include_extra_entry = 0;
        if (xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor)) {
            if (xiaomi_complex_has(&p, 1)) {
                include_extra_entry = (int)xiaomi_complex_get(&p, 1, 1);
            }
            if (xiaomi_complex_has(&p, 2)) {
                const int steps = (int)xiaomi_complex_get(&p, 2, 14);
                if (steps > 0) out.step_points++;
            }
        }

        if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 2)) {
            (void)xiaomi_complex_get(&p, 2, 6);
        }
        (void)xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor);
        if (xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
            const uint32_t raw_d = xiaomi_complex_get(&p, 0, 16);
            if (raw_d > 0u) out.distance_points++;
        }

        if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
            const uint32_t hr = xiaomi_complex_get(&p, 0, 8);
            if (hr > 0u && hr < 240u) out.hr_points++;
        }
        (void)xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor);
        (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);

        if (version >= 3) {
            (void)xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor);
            (void)xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor);
        }
        if (include_extra_entry == 1) {
            if (cursor >= payload_end) {
                return out;
            }
            cursor += 1;
        }
        if (version >= 4) {
            (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);
            (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);
        }

        if (cursor <= before) {
            return out;
        }
        out.minutes++;
        if (out.minutes > 2880u) {
            return out;
        }
    }

    out.final_cursor = cursor;
    out.valid = (cursor == payload_end && out.minutes > 0);
    if (out.valid) {
        out.score = out.step_points * 16u + out.distance_points * 8u + out.hr_points * 2u + out.minutes;
    }
    return out;
}

static int resolve_daily_details_header_size(AppState *state,
                                             int version,
                                             const uint8_t *payload,
                                             size_t payload_len,
                                             DailyDetailsProbe *chosen_probe) {
    const int expected = expected_daily_details_header_size(version);
    DailyDetailsProbe best = {0};
    int best_header = 0;

    if (expected > 0) {
        best = probe_daily_details_header(payload, payload_len, version, expected);
        if (best.valid) {
            best_header = expected;
        }
    }

    const gboolean should_search =
        (expected <= 0) ||
        !best.valid ||
        (best.step_points == 0u && best.distance_points == 0u);

    if (should_search) {
        for (int hs = 4; hs <= 12; hs++) {
            const DailyDetailsProbe cand = probe_daily_details_header(payload, payload_len, version, hs);
            if (!cand.valid) continue;

            if (!best.valid ||
                cand.score > best.score ||
                (cand.score == best.score && (best_header == 0 || hs < best_header))) {
                best = cand;
                best_header = hs;
            }
        }
    }

    if (!best.valid || best_header <= 0) {
        return 0;
    }

    if (chosen_probe != NULL) {
        *chosen_probe = best;
    }

    if (expected <= 0) {
        gchar *line = g_strdup_printf(
            "Daily details auto-detected header size=%d for version=%d (rows=%u steps=%u dist=%u hr=%u).",
            best_header, version, best.minutes, best.step_points, best.distance_points, best.hr_points
        );
        append_log(state, line);
        g_free(line);
    } else if (best_header != expected) {
        gchar *line = g_strdup_printf(
            "Daily details switched header size %d -> %d for version=%d (rows=%u steps=%u dist=%u hr=%u).",
            expected, best_header, version, best.minutes, best.step_points, best.distance_points, best.hr_points
        );
        append_log(state, line);
        g_free(line);
    }

    return best_header;
}

static void parse_daily_details_for_log(AppState *state,
                                        uint32_t ts,
                                        int version,
                                        const uint8_t *payload,
                                        size_t payload_len) {
    DailyDetailsProbe probe = {0};
    const int header_size = resolve_daily_details_header_size(state, version, payload, payload_len, &probe);
    if (header_size <= 0) {
        append_log(state, "Daily details parser: unsupported/unknown header layout for this payload.");
        return;
    }

    if (payload_len < (size_t)(8 + header_size + 4)) {
        append_log(state, "Daily details parser: payload too short.");
        return;
    }

    const uint8_t *header = payload + 8;
    size_t cursor = (size_t)(8 + header_size);
    const size_t payload_end = payload_len - 4; /* strip CRC32 */

    XiaomiComplexParser p;
    xiaomi_complex_parser_init(&p, header, (size_t)header_size);

    uint32_t minutes = 0;
    uint32_t hr_count = 0;
    uint32_t hr_sum = 0;
    uint32_t hr_min = 255;
    uint32_t hr_max = 0;
    uint32_t step_points = 0;
    uint32_t distance_points = 0;

    GString *hr_track = g_string_new("");
    GArray *samples = g_array_new(FALSE, FALSE, sizeof(RecordedSample));
    while (cursor < payload_end) {
        const size_t before = cursor;
        xiaomi_complex_parser_reset(&p);
        minutes++;
        RecordedSample sample = {0};
        sample.steps = -1;
        sample.active_calories = -1;
        sample.distance_cm = -1;
        sample.heart_rate = -1;
        sample.energy = -1;
        sample.spo2 = -1;
        sample.stress = -1;
        sample.sleep_stage = -1;
        sample.sleep_source = 0;

        int include_extra_entry = 0;
        if (xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor)) {
            if (xiaomi_complex_has(&p, 1)) {
                include_extra_entry = (int)xiaomi_complex_get(&p, 1, 1);
            }
            if (xiaomi_complex_has(&p, 2)) {
                sample.steps = (int)xiaomi_complex_get(&p, 2, 14);
                if (sample.steps > 0) {
                    step_points++;
                }
            }
        }

        if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 2)) {
            sample.active_calories = (int)xiaomi_complex_get(&p, 2, 6);
        }
        (void)xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor);
        if (xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
            /* DailyDetailsParser: sample.setDistanceCm(complexParser.get(0, 16) * 100) */
            const uint32_t raw_d = xiaomi_complex_get(&p, 0, 16);
            sample.distance_cm = (int)(raw_d * 100u);
            if (sample.distance_cm > 0) {
                distance_points++;
            }
        }

        if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
            const uint32_t hr = xiaomi_complex_get(&p, 0, 8);
            sample.heart_rate = (int)hr;
            if (hr > 0u && hr < 240u) {
                hr_count++;
                hr_sum += hr;
                if (hr < hr_min) hr_min = hr;
                if (hr > hr_max) hr_max = hr;
                if (hr_track->len < 350) {
                    if (hr_track->len > 0) g_string_append(hr_track, ",");
                    g_string_append_printf(hr_track, "%u", hr);
                }
            }
        }

        if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
            sample.energy = (int)xiaomi_complex_get(&p, 0, 8);
        }
        (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);

        if (version >= 3) {
            if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
                sample.spo2 = (int)xiaomi_complex_get(&p, 0, 8);
            }
            if (xiaomi_complex_next_group(&p, 8, payload, payload_end, &cursor) && xiaomi_complex_has(&p, 0)) {
                const int st = (int)xiaomi_complex_get(&p, 0, 8);
                /* DailyDetailsParser: if (stress != 255) sample.setStress(stress); */
                sample.stress = (st != 255) ? st : -1;
            }
        }
        if (include_extra_entry == 1) {
            if (cursor < payload_end) {
                cursor += 1;
            } else {
                break;
            }
        }
        if (version >= 4) {
            (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);
            (void)xiaomi_complex_next_group(&p, 16, payload, payload_end, &cursor);
        }
        if (cursor <= before) {
            append_log(state, "Daily details parser stalled (no cursor progress); aborting this payload.");
            break;
        }
        g_array_append_val(samples, sample);
    }

    if (probe.valid && probe.minutes > 0 && minutes != probe.minutes) {
        gchar *line_probe = g_strdup_printf(
            "Daily details note: probe rows=%u but parsed rows=%u (payload likely partially decoded).",
            probe.minutes, minutes
        );
        append_log(state, line_probe);
        g_free(line_probe);
    }

    if (cursor != payload_end) {
        gchar *line_tail = g_strdup_printf(
            "Daily details parser did not consume full payload (%zu/%zu bytes).",
            cursor, payload_end
        );
        append_log(state, line_tail);
        g_free(line_tail);
    }

    gchar *ts_txt = fmt_unix_ts(ts);
    gchar *line = g_strdup_printf("Daily detail %s: minutes=%u, HR samples=%u, HR min/avg/max=%u/%u/%u",
                                  ts_txt,
                                  minutes,
                                  hr_count,
                                  hr_count ? hr_min : 0,
                                  hr_count ? (hr_sum / hr_count) : 0,
                                  hr_count ? hr_max : 0);
    append_log(state, line);
    g_free(line);

    if (hr_count > 0) {
        gchar *track = g_strdup_printf("HR track: %s%s",
                                       hr_track->str,
                                       (hr_track->len >= 350) ? ",..." : "");
        append_log(state, track);
        g_free(track);
    }

    if (samples->len > 0) {
        /* Matches DailyDetailsParser.java: first payload row = file-id timestamp (local minute),
         * each following row +1 minute. Split across calendar days when a file spans midnight. */
        GDateTime *wall = g_date_time_new_from_unix_local((gint64)ts);
        if (wall != NULL) {
            GDateTime *start_min = g_date_time_new(
                g_date_time_get_timezone(wall),
                g_date_time_get_year(wall),
                g_date_time_get_month(wall),
                g_date_time_get_day_of_month(wall),
                g_date_time_get_hour(wall),
                g_date_time_get_minute(wall),
                0.0);
            g_date_time_unref(wall);
            if (start_min != NULL) {
                GArray *act_rows = g_array_sized_new(FALSE, FALSE, sizeof(GbXiaomiActivityRow), samples->len);
                for (guint i = 0; i < samples->len; i++) {
                    GDateTime *row_ts = g_date_time_add_minutes(start_min, (gint)i);
                    gint64 unix_sec = g_date_time_to_unix(row_ts);
                    g_date_time_unref(row_ts);
                    const RecordedSample *src = &g_array_index(samples, RecordedSample, i);
                    /* Mirror DailyDetailsParser + XiaomiActivitySample defaults (ActivitySample.NOT_MEASURED = -1). */
                    GbXiaomiActivityRow r = {0};
                    r.timestamp_sec = (int32_t)unix_sec;
                    r.raw_intensity = -1;
                    r.raw_kind = -1;
                    r.heart_rate = src->heart_rate;
                    r.stress = src->stress;
                    r.spo2 = src->spo2;
                    r.distance_cm = src->distance_cm >= 0 ? src->distance_cm : -1;
                    r.active_calories = src->active_calories >= 0 ? src->active_calories : -1;
                    r.energy = src->energy >= 0 ? src->energy : -1;
                    r.steps = src->steps >= 0 ? src->steps : -1;
                    g_array_append_val(act_rows, r);
                }
                (void)gb_db_replace_xiaomi_activity_batch((const GbXiaomiActivityRow *)act_rows->data, act_rows->len);
                g_array_unref(act_rows);

                GHashTable *by_day = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, (GDestroyNotify)g_array_unref);
                for (guint i = 0; i < samples->len; i++) {
                    GDateTime *row_t = g_date_time_add_minutes(start_min, (gint)i);
                    gchar *dk = g_date_time_format(row_t, "%Y-%m-%d");
                    guint16 mod = (guint16)(g_date_time_get_hour(row_t) * 60 + g_date_time_get_minute(row_t));
                    g_date_time_unref(row_t);

                    RecordedSample rs = g_array_index(samples, RecordedSample, i);
                    rs.minute_of_day = mod;

                    GArray *bucket = g_hash_table_lookup(by_day, dk);
                    if (bucket == NULL) {
                        bucket = g_array_new(FALSE, FALSE, sizeof(RecordedSample));
                        g_hash_table_insert(by_day, dk, bucket);
                    } else {
                        g_free(dk);
                    }
                    g_array_append_val(bucket, rs);
                }
                g_date_time_unref(start_min);

                GHashTableIter it;
                gpointer k, v;
                g_hash_table_iter_init(&it, by_day);
                while (g_hash_table_iter_next(&it, &k, &v)) {
                    GArray *bucket = v;
                    if (bucket != NULL && bucket->len > 0) {
                        recorded_store_samples_for_day_key(state, (const gchar *)k,
                                                           (const RecordedSample *)bucket->data, bucket->len);
                    }
                }
                g_hash_table_unref(by_day);
            }
        }
    }

    if (step_points == 0 && distance_points == 0) {
        append_log(state,
                   "Daily details decoded but contained no minute step/distance points. "
                   "This usually means the band sent only daily summary data or the details format changed.");
    }

    if (!state->sleep_hint_logged) {
        state->sleep_hint_logged = TRUE;
        append_log(state,
                   "Sleep charts need activity files with subtype SLEEP or SLEEP_STAGES; "
                   "this sync only listed ACTIVITY_DAILY IDs from the band.");
    }
    g_free(ts_txt);
    g_string_free(hr_track, TRUE);
    g_array_unref(samples);
}

static void parse_daily_summary_for_log(AppState *state,
                                        uint32_t ts,
                                        int8_t tz,
                                        int version,
                                        const uint8_t *payload,
                                        size_t payload_len) {
    int header_size = 0;
    if (version == 3) header_size = 3;
    else if (version == 5) header_size = 4;
    else {
        append_log(state, "Daily summary parser: unsupported version.");
        return;
    }
    if (payload_len < (size_t)(8 + header_size + 30)) {
        append_log(state, "Daily summary parser: payload too short.");
        return;
    }

    size_t off = (size_t)(8 + header_size);
    const uint32_t steps = mib10_read_u32_le(payload + off); off += 4;
    off += 3; /* unknown bytes */
    const uint8_t hr_rest = payload[off++];
    const uint8_t hr_max = payload[off++];
    const uint32_t hr_max_ts = mib10_read_u32_le(payload + off); off += 4;
    const uint8_t hr_min = payload[off++];
    const uint32_t hr_min_ts = mib10_read_u32_le(payload + off); off += 4;
    const uint8_t hr_avg = payload[off++];
    const uint8_t stress_avg = payload[off++];
    const uint8_t stress_max = payload[off++];
    const uint8_t stress_min = payload[off++];
    const uint32_t standing_bits = (uint32_t)payload[off] | ((uint32_t)payload[off + 1] << 8) | ((uint32_t)payload[off + 2] << 16);
    off += 3;
    const uint16_t calories = mib10_read_u16_le(payload + off); off += 2;
    off += 3;
    const uint8_t spo2_max = payload[off++];
    const uint32_t spo2_max_ts = mib10_read_u32_le(payload + off); off += 4;
    const uint8_t spo2_min = payload[off++];
    const uint32_t spo2_min_ts = mib10_read_u32_le(payload + off); off += 4;
    const uint8_t spo2_avg = payload[off++];

    gboolean has_training = FALSE;
    int t_day = 0, t_week = 0, t_level = 0, v_light = 0, v_mod = 0, v_high = 0, v_cur = 0;
    if (version > 3 && off + 10 <= payload_len) {
        t_day = (int)mib10_read_u16_le(payload + off);
        off += 2;
        t_week = (int)mib10_read_u16_le(payload + off);
        off += 2;
        t_level = (int)payload[off++];
        v_light = (int)payload[off++];
        v_mod = (int)payload[off++];
        v_high = (int)payload[off++];
        v_cur = (int)mib10_read_u16_le(payload + off);
        off += 2;
        has_training = TRUE;
    }

    unsigned int standing_hours = 0;
    for (int i = 0; i < 24; i++) {
        if ((standing_bits >> i) & 1u) standing_hours++;
    }

    gchar *ts_txt = fmt_unix_ts(ts);
    gchar *hr_max_txt = fmt_unix_ts(hr_max_ts);
    gchar *hr_min_txt = fmt_unix_ts(hr_min_ts);
    gchar *line = g_strdup_printf("Daily summary %s: steps=%u calories=%u HR(rest/avg/min/max)=%u/%u/%u/%u stress=%u/%u/%u standingHours=%u",
                                  ts_txt, steps, calories, hr_rest, hr_avg, hr_min, hr_max, stress_avg, stress_max, stress_min,
                                  standing_hours);
    append_log(state, line);
    g_free(line);
    line = g_strdup_printf("  HR extrema timestamps: min@%s max@%s", hr_min_txt, hr_max_txt);
    append_log(state, line);
    g_free(line);
    g_free(hr_min_txt);
    g_free(hr_max_txt);
    g_free(ts_txt);

    GbDailySummaryRow dr = {0};
    dr.timestamp_ms = (int64_t)ts * 1000LL;
    dr.timezone = (int)tz;
    dr.steps = (int)steps;
    dr.hr_resting = (int)hr_rest;
    dr.hr_max = (int)hr_max;
    dr.hr_max_ts_sec = (int32_t)hr_max_ts;
    dr.hr_min = (int)hr_min;
    dr.hr_min_ts_sec = (int32_t)hr_min_ts;
    dr.hr_avg = (int)hr_avg;
    dr.stress_avg = (int)stress_avg;
    dr.stress_max = (int)stress_max;
    dr.stress_min = (int)stress_min;
    dr.standing_bits = (int)standing_bits;
    dr.calories = (int)calories;
    dr.spo2_max = (int)spo2_max;
    dr.spo2_max_ts_sec = (int32_t)spo2_max_ts;
    dr.spo2_min = (int)spo2_min;
    dr.spo2_min_ts_sec = (int32_t)spo2_min_ts;
    dr.spo2_avg = (int)spo2_avg;
    dr.has_training_vitality = has_training;
    dr.training_load_day = t_day;
    dr.training_load_week = t_week;
    dr.training_load_level = t_level;
    dr.vitality_increase_light = v_light;
    dr.vitality_increase_moderate = v_mod;
    dr.vitality_increase_high = v_high;
    dr.vitality_current = v_cur;
    (void)gb_db_replace_daily_summary(&dr);
    recorded_store_daily_summary_steps(state, ts, steps);
}

static gboolean sleep_header_valid_bit(const uint8_t *header, size_t header_len, int idx) {
    if (idx < 0) return FALSE;
    size_t b = (size_t)idx / 8u;
    if (b >= header_len) return FALSE;
    return (header[b] & (1u << (7 - (idx % 8)))) != 0;
}

static void parse_sleep_stages_v2_for_log(AppState *state,
                                          uint32_t ts,
                                          const uint8_t *payload,
                                          size_t payload_len) {
    if (payload_len < 8 + 7 + 2 + 4 + 4 + 3 + 2 + 2 + 2 + 2 + 1 + 4) {
        append_log(state, "Sleep stages parser: payload too short.");
        return;
    }
    size_t off = 8;
    off += 7;
    const uint16_t sleep_duration = mib10_read_u16_le(payload + off); off += 2;
    const uint32_t bed_time = mib10_read_u32_le(payload + off); off += 4;
    const uint32_t wake_time = mib10_read_u32_le(payload + off); off += 4;
    off += 3;
    const uint16_t deep = mib10_read_u16_le(payload + off); off += 2;
    const uint16_t light = mib10_read_u16_le(payload + off); off += 2;
    const uint16_t rem = mib10_read_u16_le(payload + off); off += 2;
    const uint16_t awake = mib10_read_u16_le(payload + off); off += 2;
    off += 1;

    GArray *raw_phases = g_array_new(FALSE, FALSE, sizeof(GbRawSleepPhaseChange));
    GArray *samples = g_array_new(FALSE, FALSE, sizeof(RecordedSample));
    uint32_t prev_t = 0;
    int prev_stage = -1;
    while (off + 5 <= payload_len - 4) {
        uint32_t t = mib10_read_u32_le(payload + off); off += 4;
        uint8_t stage = payload[off++];
        if (t == 0) continue;
        GbRawSleepPhaseChange ph = { t, stage };
        g_array_append_val(raw_phases, ph);
        if (prev_t != 0 && prev_stage >= 0 && t > prev_t) {
            for (uint32_t sec = prev_t; sec < t; sec += 60) {
                RecordedSample s = {0};
                s.minute_of_day = (uint16_t)((sec / 60u) % 1440u);
                s.steps = s.active_calories = s.distance_cm = s.heart_rate = s.energy = s.spo2 = s.stress = -1;
                s.sleep_stage = prev_stage;
                s.sleep_source = 1;
                g_array_append_val(samples, s);
            }
        }
        prev_t = t;
        prev_stage = stage;
    }
    if (prev_t != 0 && prev_stage >= 0 && wake_time > prev_t) {
        for (uint32_t sec = prev_t; sec < wake_time; sec += 60) {
            RecordedSample s = {0};
            s.minute_of_day = (uint16_t)((sec / 60u) % 1440u);
            s.steps = s.active_calories = s.distance_cm = s.heart_rate = s.energy = s.spo2 = s.stress = -1;
            s.sleep_stage = prev_stage;
            s.sleep_source = 1;
            g_array_append_val(samples, s);
        }
    }

    if (bed_time != 0 && wake_time != 0 && sleep_duration != 0) {
        (void)gb_db_replace_sleep_stages_v2(bed_time,
                                           wake_time,
                                           sleep_duration,
                                           deep,
                                           light,
                                           rem,
                                           awake,
                                           raw_phases->len > 0 ? (const GbRawSleepPhaseChange *)raw_phases->data : NULL,
                                           raw_phases->len);
    }
    g_array_unref(raw_phases);

    recorded_store_daily_details(state, ts, (const RecordedSample *)samples->data, samples->len);
    log_sleep_samples_summary(state, "Sleep stages v2 parsed", samples);
    g_array_unref(samples);

    gchar *bed_txt = fmt_unix_ts(bed_time);
    gchar *wake_txt = fmt_unix_ts(wake_time);
    gchar *line = g_strdup_printf("Sleep stages: bed=%s wake=%s total=%u deep=%u light=%u rem=%u awake=%u",
                                  bed_txt, wake_txt, sleep_duration, deep, light, rem, awake);
    append_log(state, line);
    g_free(line);
    append_log(state, "Sleep stage timeline stored in dedicated Sleep chart.");
    g_free(bed_txt);
    g_free(wake_txt);
}

static gboolean read_sleep_stage_packet_header(const uint8_t *buf, size_t len, size_t *off) {
    while (*off + 17 <= len) {
        if (buf[*off] == 0xFF && buf[*off + 1] == 0xFC && buf[*off + 2] == 0xFA && buf[*off + 3] == 0xFB) {
            *off += 4;
            return TRUE;
        }
        *off += 1;
    }
    return FALSE;
}

/* Match Gadgetbridge SleepDetailsParser.decodeStage() mapping. */
static int decode_sleep_stage_like_gb(int raw_stage) {
    switch (raw_stage) {
        case 0: return 5; /* AWAKE */
        case 1: return 3; /* LIGHT */
        case 2: return 2; /* DEEP */
        case 3: return 4; /* REM */
        case 4: return 0; /* NOT_SLEEP */
        default: return 1; /* N/A */
    }
}

static void parse_sleep_details_for_log(AppState *state,
                                        uint32_t ts,
                                        int version,
                                        const uint8_t *payload,
                                        size_t payload_len) {
    int header_size = 0;
    if (version >= 1 && version <= 4) header_size = 1;
    else if (version == 5) header_size = 2;
    else {
        append_log(state, "Sleep details parser: unsupported version.");
        return;
    }
    if (payload_len < (size_t)(8 + header_size + 9 + 4)) {
        append_log(state, "Sleep details parser: payload too short.");
        return;
    }

    size_t off = 8;
    const uint8_t *header = payload + off;
    off += (size_t)header_size;
    int header_idx = 0;

    const uint8_t is_awake = payload[off++]; header_idx++;
    const uint32_t bed_time = mib10_read_u32_le(payload + off); off += 4; header_idx++;
    const uint32_t wake_time = mib10_read_u32_le(payload + off); off += 4; header_idx++;
    if (version >= 4) {
        if (sleep_header_valid_bit(header, (size_t)header_size, header_idx) && off < payload_len - 4) {
            off += 1;
        }
        header_idx++;
    }
    if (version >= 5) {
        if (off + 17 > payload_len - 4) return;
        off += 9;
        off += 4;
        off += 4;
        header_idx += 5;
    }

    GArray *samples = g_array_new(FALSE, FALSE, sizeof(RecordedSample));
    GArray *stage_rows = g_array_new(FALSE, FALSE, sizeof(GbSleepStageRow));
    GArray *pulse_rows = g_array_new(FALSE, FALSE, sizeof(GbHeartPulseRow));

    if (sleep_header_valid_bit(header, (size_t)header_size, header_idx) && off + 4 <= payload_len - 4) {
        uint16_t unit = mib10_read_u16_le(payload + off); off += 2;
        uint16_t count = mib10_read_u16_le(payload + off); off += 2;
        uint32_t first_record = bed_time;
        if (count > 0 && version >= 2 && off + 4 <= payload_len - 4) {
            first_record = mib10_read_u32_le(payload + off);
            off += 4;
        }
        for (uint16_t i = 0; i < count && off < payload_len - 4; i++) {
            uint8_t hr = payload[off++];
            if (hr > 0 && hr < 240) {
                uint32_t sec = first_record + (uint32_t)unit * (uint32_t)i;
                RecordedSample s = {0};
                s.minute_of_day = (uint16_t)((sec / 60u) % 1440u);
                s.steps = s.active_calories = s.distance_cm = s.energy = s.spo2 = s.stress = s.sleep_stage = -1;
                s.heart_rate = hr;
                s.sleep_source = 1;
                g_array_append_val(samples, s);
            }
        }
    }
    header_idx++;

    if (sleep_header_valid_bit(header, (size_t)header_size, header_idx) && off + 4 <= payload_len - 4) {
        uint16_t unit = mib10_read_u16_le(payload + off); off += 2;
        uint16_t count = mib10_read_u16_le(payload + off); off += 2;
        uint32_t first_record = bed_time;
        if (count > 0 && version >= 2 && off + 4 <= payload_len - 4) {
            first_record = mib10_read_u32_le(payload + off);
            off += 4;
        }
        for (uint16_t i = 0; i < count && off < payload_len - 4; i++) {
            uint8_t spo2 = payload[off++];
            if (spo2 > 0 && spo2 <= 100) {
                uint32_t sec = first_record + (uint32_t)unit * (uint32_t)i;
                RecordedSample s = {0};
                s.minute_of_day = (uint16_t)((sec / 60u) % 1440u);
                s.steps = s.active_calories = s.distance_cm = s.energy = s.heart_rate = s.stress = s.sleep_stage = -1;
                s.spo2 = spo2;
                s.sleep_source = 1;
                g_array_append_val(samples, s);
            }
        }
    }
    header_idx++;

    if (version >= 3) {
        if (sleep_header_valid_bit(header, (size_t)header_size, header_idx) && off + 4 <= payload_len - 4) {
            uint16_t unit = mib10_read_u16_le(payload + off); off += 2;
            uint16_t count = mib10_read_u16_le(payload + off); off += 2;
            if (count > 0 && version >= 2 && off + 4 <= payload_len - 4) {
                off += 4;
            }
            size_t skip = (size_t)count * 4u;
            if (off + skip <= payload_len - 4) off += skip;
        }
        header_idx++;
    }

    /* parse stage packets for stage ranges + RR intervals */
    long long last_hb_ms = 0;
    size_t pos = off;
    while (read_sleep_stage_packet_header(payload, payload_len - 4, &pos)) {
        if (pos + 13 > payload_len - 4) break;
        uint8_t header_len = payload[pos++]; (void)header_len;
        uint64_t ts_raw = 0;
        for (int i = 0; i < 8; i++) ts_raw = (ts_raw << 8) | payload[pos + i];
        pos += 8;
        pos += 1; /* parity */
        uint8_t type = payload[pos++];
        uint16_t data_len = ((uint16_t)payload[pos] << 8) | payload[pos + 1];
        pos += 2;
        if (pos + data_len > payload_len - 4) break;
        const uint8_t *d = payload + pos;
        pos += data_len;

        if (type == 1) {
            long long tms = (long long)ts_raw;
            if (llabs(last_hb_ms - tms) > 30000LL) {
                last_hb_ms = tms;
                GbHeartPulseRow hp0 = { last_hb_ms };
                g_array_append_val(pulse_rows, hp0);
            }
            for (uint16_t i = 0; i < data_len; i++) {
                int delta = d[i];
                if (delta <= 0) continue;
                last_hb_ms += (long long)delta * 10LL;
                GbHeartPulseRow hp = { last_hb_ms };
                g_array_append_val(pulse_rows, hp);
                int rr_ms = delta * 10;
                int hr = rr_ms > 0 ? (60000 / rr_ms) : -1;
                if (hr > 0 && hr < 240) {
                    uint32_t sec = (uint32_t)(last_hb_ms / 1000LL);
                    RecordedSample s = {0};
                    s.minute_of_day = (uint16_t)((sec / 60u) % 1440u);
                    s.steps = s.active_calories = s.distance_cm = s.energy = s.spo2 = s.stress = s.sleep_stage = -1;
                    s.heart_rate = hr;
                    s.sleep_source = 1;
                    g_array_append_val(samples, s);
                }
            }
        } else if (type == 17) {
            int64_t current_ms = (int64_t)ts_raw * 1000LL;
            uint32_t cur_sec = (uint32_t)ts_raw;
            for (uint16_t i = 0; i + 1 < data_len; i += 2) {
                uint16_t val = ((uint16_t)d[i] << 8) | d[i + 1];
                int stage = decode_sleep_stage_like_gb((val >> 12) & 0x0F);
                int offset_min = val & 0x0FFF;
                GbSleepStageRow gr = { current_ms, stage };
                g_array_append_val(stage_rows, gr);
                current_ms += (int64_t)offset_min * 60000LL;
                for (int m = 0; m < offset_min; m++) {
                    RecordedSample s = {0};
                    s.minute_of_day = (uint16_t)(((cur_sec / 60u) + (uint32_t)m) % 1440u);
                    s.steps = s.active_calories = s.distance_cm = s.energy = s.heart_rate = s.spo2 = s.stress = -1;
                    s.sleep_stage = stage;
                    s.sleep_source = 1;
                    g_array_append_val(samples, s);
                }
                cur_sec += (uint32_t)offset_min * 60u;
            }
        }
    }

    if (bed_time != 0 && wake_time != 0) {
        (void)gb_db_replace_sleep_details_header((int64_t)bed_time * 1000LL,
                                                   (int64_t)wake_time * 1000LL,
                                                   (int)is_awake,
                                                   -1,
                                                   -1,
                                                   -1,
                                                   -1,
                                                   -1);
    }
    if (stage_rows->len > 0) {
        (void)gb_db_replace_sleep_stage_samples((const GbSleepStageRow *)stage_rows->data, stage_rows->len);
    }
    if (pulse_rows->len > 0) {
        (void)gb_db_replace_heart_pulse_samples((const GbHeartPulseRow *)pulse_rows->data, pulse_rows->len);
    }
    g_array_unref(stage_rows);
    g_array_unref(pulse_rows);

    if (samples->len > 0) {
        recorded_store_daily_details(state, ts, (const RecordedSample *)samples->data, samples->len);
        log_sleep_samples_summary(state, "Sleep details parsed", samples);
    }
    g_array_unref(samples);

    gchar *bed_txt = fmt_unix_ts(bed_time);
    gchar *wake_txt = fmt_unix_ts(wake_time);
    gchar *line = g_strdup_printf("Sleep details: bed=%s wake=%s awakeFlag=%u", bed_txt, wake_txt, is_awake);
    append_log(state, line);
    g_free(line);
    append_log(state, "Sleep stage timeline stored in dedicated Sleep chart.");
    g_free(bed_txt);
    g_free(wake_txt);
}

const char *activity_subtype_name(int type, int subtype) {
    if (type == 0) {
        switch (subtype) {
            case 0x00: return "ACTIVITY_DAILY";
            case 0x03: return "SLEEP_STAGES";
            case 0x06: return "MANUAL_SAMPLES";
            case 0x08: return "SLEEP";
            default: return "ACTIVITY_UNKNOWN";
        }
    }
    if (type == 1) {
        switch (subtype) {
            case 0x01: return "SPORT_OUTDOOR_RUNNING";
            case 0x08: return "SPORT_FREESTYLE";
            default: return "SPORT_UNKNOWN";
        }
    }
    return "UNKNOWN";
}
void handle_activity_payload(AppState *state, const uint8_t *payload, size_t payload_len) {
    const uint8_t *file_id = NULL; /* first 7 bytes */
    gboolean ack_allowed = FALSE;
    gboolean advance_fetch_queue = FALSE;

    if (payload_len >= 7) {
        file_id = payload;
        if (state->activity_fetch_inflight &&
            memcmp(file_id, state->activity_fetch_current_id, 7) == 0) {
            advance_fetch_queue = TRUE;
        }
    } else if (state->activity_fetch_inflight) {
        /*
         * Corrupted payload with no recoverable file id - avoid deadlocking the fetch queue.
         * This mirrors GB's behavior of moving to the next fetch on malformed payloads.
         */
        advance_fetch_queue = TRUE;
    }

    if (payload_len < 13) {
        append_log(state, "Activity payload too short.");
        goto finish;
    }

    const uint32_t actual_crc = crc32_ieee(payload, payload_len - 4);
    const uint32_t expected_crc = mib10_read_u32_le(payload + payload_len - 4);
    if (actual_crc != expected_crc) {
        gchar *line = g_strdup_printf("Activity payload CRC mismatch: got=%08X expected=%08X", actual_crc, expected_crc);
        append_log(state, line);
        g_free(line);
        goto finish;
    }
    ack_allowed = TRUE;

    (void)gb_db_ensure_device(state->selected_address);

    const uint32_t ts = mib10_read_u32_le(payload);
    const int8_t tz = (int8_t)payload[4];
    const int version = payload[5];
    const int flags = payload[6];
    const int type = (flags >> 7) & 1;
    const int subtype = (flags & 127) >> 2;
    const int detail = flags & 3;

    const char *name = activity_subtype_name(type, subtype);
    gchar *line = g_strdup_printf("Activity payload: ts=%u tz=%d type=%d subtype=%d(%s) detail=%d ver=%d bytes=%zu (CRC ok)",
                                  ts, tz, type, subtype, name, detail, version, payload_len);
    append_log(state, line);
    g_free(line);

    if (type == 0 && subtype == 0 && detail == 0) {
        parse_daily_details_for_log(state, ts, version, payload, payload_len);
    } else if (type == 0 && subtype == 0 && detail == 1) {
        parse_daily_summary_for_log(state, ts, tz, version, payload, payload_len);
    } else if (type == 0 && subtype == 3) {
        if (version == 2) {
            parse_sleep_stages_v2_for_log(state, ts, payload, payload_len);
        } else {
            append_log(state, "Sleep stages parser: unsupported version.");
        }
    } else if (type == 0 && subtype == 8) {
        parse_sleep_details_for_log(state, ts, version, payload, payload_len);
    } else {
        append_log(state, "No parser for this activity payload type/subtype/detail yet.");
    }

finish:
    if (state->auth_initialized && ack_allowed && file_id != NULL) {
        (void)send_activity_fetch_ack(state, file_id, 7);
        if (state->activity_fetch_inflight &&
            memcmp(file_id, state->activity_fetch_current_id, 7) != 0) {
            mib10_log_fetch_transition(state, "ack-sidecar", file_id);
        } else {
            mib10_log_fetch_transition(state, "ack", file_id);
        }
    }

    if (advance_fetch_queue && state->activity_fetch_inflight) {
        mib10_log_fetch_transition(state, ack_allowed ? "advance-next" : "drop-next", file_id);
        if (state->activity_fetch_timeout_source_id != 0) {
            g_source_remove(state->activity_fetch_timeout_source_id);
            state->activity_fetch_timeout_source_id = 0;
        }
        state->activity_fetch_inflight = FALSE;
        memset(state->activity_fetch_current_id, 0, sizeof(state->activity_fetch_current_id));
        trigger_next_activity_fetch(state);
    } else if (state->activity_fetch_inflight &&
               ack_allowed &&
               file_id != NULL &&
               memcmp(file_id, state->activity_fetch_current_id, 7) != 0) {
        mib10_log_fetch_transition(state, "waiting-current", state->activity_fetch_current_id);
    }
}
