#include "gb_db.h"

#include <sqlite3.h>
#include <string.h>

#include <glib.h>

static sqlite3 *s_db;
static GMutex s_lock;
static gsize s_lock_once;
static gint64 s_device_id;
static gint64 s_user_id;
static gboolean s_opened;

static void gb_db_mutex_ensure(void) {
    if (g_once_init_enter(&s_lock_once)) {
        g_mutex_init(&s_lock);
        g_once_init_leave(&s_lock_once, 1);
    }
}

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        g_warning("gb_db SQL error: %s - %s", sql, err ? err : sqlite3_errmsg(db));
        sqlite3_free(err);
        return rc;
    }
    return SQLITE_OK;
}

static gboolean ensure_schema(sqlite3 *db) {
    static const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS \"DEVICE\" ("
        "\"_id\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
        "\"name\" TEXT NOT NULL,"
        "\"manufacturer\" TEXT NOT NULL,"
        "\"identifier\" TEXT NOT NULL,"
        "\"type\" INTEGER NOT NULL,"
        "\"typeName\" TEXT NOT NULL,"
        "\"model\" TEXT,"
        "\"alias\" TEXT,"
        "\"parentFolder\" TEXT"
        ");",
        "CREATE UNIQUE INDEX IF NOT EXISTS \"IDX_GB_DEVICE_IDENTIFIER\" ON \"DEVICE\" (\"identifier\");",
        "CREATE TABLE IF NOT EXISTS \"USER\" ("
        "\"_id\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
        "\"name\" TEXT NOT NULL,"
        "\"birthday\" INTEGER NOT NULL,"
        "\"gender\" INTEGER NOT NULL"
        ");",
        "CREATE TABLE IF NOT EXISTS \"XIAOMI_ACTIVITY_SAMPLE\" ("
        "\"timestamp\" INTEGER NOT NULL,"
        "\"deviceId\" INTEGER NOT NULL,"
        "\"userId\" INTEGER NOT NULL,"
        "\"rawIntensity\" INTEGER NOT NULL,"
        "\"steps\" INTEGER NOT NULL,"
        "\"rawKind\" INTEGER NOT NULL,"
        "\"heartRate\" INTEGER NOT NULL,"
        "\"stress\" INTEGER,"
        "\"spo2\" INTEGER,"
        "\"distanceCm\" INTEGER NOT NULL,"
        "\"activeCalories\" INTEGER NOT NULL,"
        "\"energy\" INTEGER NOT NULL,"
        "PRIMARY KEY (\"timestamp\",\"deviceId\")"
        ");",
        "CREATE TABLE IF NOT EXISTS \"XIAOMI_DAILY_SUMMARY_SAMPLE\" ("
        "\"timestamp\" INTEGER NOT NULL,"
        "\"deviceId\" INTEGER NOT NULL,"
        "\"userId\" INTEGER NOT NULL,"
        "\"timezone\" INTEGER NOT NULL,"
        "\"steps\" INTEGER NOT NULL,"
        "\"hrResting\" INTEGER NOT NULL,"
        "\"hrMax\" INTEGER NOT NULL,"
        "\"hrMaxTs\" INTEGER NOT NULL,"
        "\"hrMin\" INTEGER NOT NULL,"
        "\"hrMinTs\" INTEGER NOT NULL,"
        "\"hrAvg\" INTEGER NOT NULL,"
        "\"stressAvg\" INTEGER NOT NULL,"
        "\"stressMax\" INTEGER NOT NULL,"
        "\"stressMin\" INTEGER NOT NULL,"
        "\"standing\" INTEGER NOT NULL,"
        "\"calories\" INTEGER NOT NULL,"
        "\"spo2Max\" INTEGER NOT NULL,"
        "\"spo2MaxTs\" INTEGER NOT NULL,"
        "\"spo2Min\" INTEGER NOT NULL,"
        "\"spo2MinTs\" INTEGER NOT NULL,"
        "\"spo2Avg\" INTEGER NOT NULL,"
        "\"trainingLoadDay\" INTEGER,"
        "\"trainingLoadWeek\" INTEGER,"
        "\"trainingLoadLevel\" INTEGER,"
        "\"vitalityIncreaseLight\" INTEGER,"
        "\"vitalityIncreaseModerate\" INTEGER,"
        "\"vitalityIncreaseHigh\" INTEGER,"
        "\"vitalityCurrent\" INTEGER,"
        "PRIMARY KEY (\"timestamp\",\"deviceId\")"
        ");",
        "CREATE TABLE IF NOT EXISTS \"XIAOMI_SLEEP_TIME_SAMPLE\" ("
        "\"timestamp\" INTEGER NOT NULL,"
        "\"deviceId\" INTEGER NOT NULL,"
        "\"userId\" INTEGER NOT NULL,"
        "\"wakeupTime\" INTEGER,"
        "\"isAwake\" INTEGER NOT NULL,"
        "\"totalDuration\" INTEGER,"
        "\"deepSleepDuration\" INTEGER,"
        "\"lightSleepDuration\" INTEGER,"
        "\"remSleepDuration\" INTEGER,"
        "\"awakeDuration\" INTEGER,"
        "PRIMARY KEY (\"timestamp\",\"deviceId\")"
        ");",
        "CREATE TABLE IF NOT EXISTS \"XIAOMI_SLEEP_STAGE_SAMPLE\" ("
        "\"timestamp\" INTEGER NOT NULL,"
        "\"deviceId\" INTEGER NOT NULL,"
        "\"userId\" INTEGER NOT NULL,"
        "\"stage\" INTEGER,"
        "PRIMARY KEY (\"timestamp\",\"deviceId\")"
        ");",
        "CREATE TABLE IF NOT EXISTS \"HEART_PULSE_SAMPLE\" ("
        "\"timestamp\" INTEGER NOT NULL,"
        "\"deviceId\" INTEGER NOT NULL,"
        "\"userId\" INTEGER NOT NULL,"
        "PRIMARY KEY (\"timestamp\",\"deviceId\")"
        ");",
        NULL,
    };
    for (const char **p = stmts; *p; p++) {
        if (exec_sql(db, *p) != SQLITE_OK) {
            return FALSE;
        }
    }
    return TRUE;
}

static gboolean ensure_user_row(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT \"_id\" FROM \"USER\" ORDER BY \"_id\" LIMIT 1;", -1, &st, NULL) != SQLITE_OK) {
        return FALSE;
    }
    gint64 uid = 0;
    int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) {
        uid = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);
    if (uid > 0) {
        s_user_id = uid;
        return TRUE;
    }
    if (sqlite3_prepare_v2(db, "INSERT INTO \"USER\" (\"name\",\"birthday\",\"gender\") VALUES ('User',0,0);", -1, &st, NULL) !=
        SQLITE_OK) {
        return FALSE;
    }
    rc = sqlite3_step(st);
    sqlite3_finalize(st);
    if (rc != SQLITE_DONE) {
        return FALSE;
    }
    s_user_id = sqlite3_last_insert_rowid(db);
    return s_user_id > 0;
}

gboolean gb_db_open(void) {
    gb_db_mutex_ensure();
    g_mutex_lock(&s_lock);
    if (s_opened) {
        g_mutex_unlock(&s_lock);
        return TRUE;
    }
    s_device_id = 0;
    s_user_id = 0;

    gchar *dir = g_build_filename(g_get_user_data_dir(), "mi-band10-viewer", NULL);
    g_mkdir_with_parents(dir, 0755);
    gchar *path = g_build_filename(dir, "gadgetbridge_compat.db", NULL);
    g_free(dir);

    int rc = sqlite3_open_v2(path, &s_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
    g_free(path);
    if (rc != SQLITE_OK || s_db == NULL) {
        g_warning("gb_db: sqlite3_open failed: %s", s_db ? sqlite3_errmsg(s_db) : "null db");
        if (s_db) {
            sqlite3_close(s_db);
            s_db = NULL;
        }
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    sqlite3_busy_timeout(s_db, 5000);
    exec_sql(s_db, "PRAGMA journal_mode=WAL;");
    exec_sql(s_db, "PRAGMA foreign_keys=OFF;");

    if (!ensure_schema(s_db) || !ensure_user_row(s_db)) {
        sqlite3_close(s_db);
        s_db = NULL;
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    s_opened = TRUE;
    g_mutex_unlock(&s_lock);
    return TRUE;
}

void gb_db_close(void) {
    g_mutex_lock(&s_lock);
    if (s_db != NULL) {
        sqlite3_close(s_db);
        s_db = NULL;
    }
    s_opened = FALSE;
    s_device_id = 0;
    s_user_id = 0;
    g_mutex_unlock(&s_lock);
}

gboolean gb_db_ensure_device(const char *bluetooth_address) {
    gb_db_mutex_ensure();
    if (bluetooth_address == NULL || bluetooth_address[0] == '\0') {
        return FALSE;
    }
    if (s_db == NULL && !gb_db_open()) {
        return FALSE;
    }

    gchar *id = g_strdup(bluetooth_address);
    g_strstrip(id);
    for (gchar *p = id; *p; p++) {
        *p = (gchar)g_ascii_toupper((guchar)*p);
    }

    g_mutex_lock(&s_lock);
    if (s_db == NULL) {
        g_free(id);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s_db, "SELECT \"_id\" FROM \"DEVICE\" WHERE \"identifier\" = ? LIMIT 1;", -1, &st, NULL) !=
        SQLITE_OK) {
        g_free(id);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    gint64 did = 0;
    if (sqlite3_step(st) == SQLITE_ROW) {
        did = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);

    if (did <= 0) {
        if (sqlite3_prepare_v2(
                s_db,
                "INSERT INTO \"DEVICE\" (\"name\",\"manufacturer\",\"identifier\",\"type\",\"typeName\") "
                "VALUES ('Mi Band 10','Xiaomi',?,0,'XIAOMI');",
                -1,
                &st,
                NULL) != SQLITE_OK) {
            g_free(id);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            g_free(id);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        sqlite3_finalize(st);
        did = sqlite3_last_insert_rowid(s_db);
    }

    s_device_id = did;
    g_free(id);
    g_mutex_unlock(&s_lock);
    return did > 0;
}

static gboolean can_write(void) {
    return s_db != NULL && s_device_id > 0 && s_user_id > 0;
}

gboolean gb_db_replace_xiaomi_activity_batch(const GbXiaomiActivityRow *rows, size_t n) {
    if (rows == NULL || n == 0) {
        return TRUE;
    }
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    sqlite3_stmt *st = NULL;
    const char *sql = "INSERT OR REPLACE INTO \"XIAOMI_ACTIVITY_SAMPLE\" ("
                      "\"timestamp\",\"deviceId\",\"userId\",\"rawIntensity\",\"steps\",\"rawKind\",\"heartRate\","
                      "\"stress\",\"spo2\",\"distanceCm\",\"activeCalories\",\"energy\") "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
    if (sqlite3_prepare_v2(s_db, sql, -1, &st, NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    if (sqlite3_exec(s_db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_finalize(st);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    for (size_t i = 0; i < n; i++) {
        const GbXiaomiActivityRow *r = &rows[i];
        sqlite3_bind_int(st, 1, r->timestamp_sec);
        sqlite3_bind_int64(st, 2, s_device_id);
        sqlite3_bind_int64(st, 3, s_user_id);
        sqlite3_bind_int(st, 4, r->raw_intensity);
        sqlite3_bind_int(st, 5, r->steps);
        sqlite3_bind_int(st, 6, r->raw_kind);
        sqlite3_bind_int(st, 7, r->heart_rate);
        if (r->stress < 0) {
            sqlite3_bind_null(st, 8);
        } else {
            sqlite3_bind_int(st, 8, r->stress);
        }
        if (r->spo2 < 0) {
            sqlite3_bind_null(st, 9);
        } else {
            sqlite3_bind_int(st, 9, r->spo2);
        }
        sqlite3_bind_int(st, 10, r->distance_cm);
        sqlite3_bind_int(st, 11, r->active_calories);
        sqlite3_bind_int(st, 12, r->energy);
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            sqlite3_exec(s_db, "ROLLBACK;", NULL, NULL, NULL);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        sqlite3_reset(st);
        sqlite3_clear_bindings(st);
    }
    sqlite3_finalize(st);
    sqlite3_exec(s_db, "COMMIT;", NULL, NULL, NULL);
    g_mutex_unlock(&s_lock);
    return TRUE;
}

gboolean gb_db_replace_daily_summary(const GbDailySummaryRow *row) {
    if (row == NULL) {
        return FALSE;
    }
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    sqlite3_stmt *st = NULL;
    const char *sql = "INSERT OR REPLACE INTO \"XIAOMI_DAILY_SUMMARY_SAMPLE\" ("
                      "\"timestamp\",\"deviceId\",\"userId\",\"timezone\",\"steps\",\"hrResting\",\"hrMax\",\"hrMaxTs\","
                      "\"hrMin\",\"hrMinTs\",\"hrAvg\",\"stressAvg\",\"stressMax\",\"stressMin\",\"standing\","
                      "\"calories\",\"spo2Max\",\"spo2MaxTs\",\"spo2Min\",\"spo2MinTs\",\"spo2Avg\","
                      "\"trainingLoadDay\",\"trainingLoadWeek\",\"trainingLoadLevel\","
                      "\"vitalityIncreaseLight\",\"vitalityIncreaseModerate\",\"vitalityIncreaseHigh\",\"vitalityCurrent\") "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    if (sqlite3_prepare_v2(s_db, sql, -1, &st, NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    sqlite3_bind_int64(st, 1, row->timestamp_ms);
    sqlite3_bind_int64(st, 2, s_device_id);
    sqlite3_bind_int64(st, 3, s_user_id);
    sqlite3_bind_int(st, 4, row->timezone);
    sqlite3_bind_int(st, 5, row->steps);
    sqlite3_bind_int(st, 6, row->hr_resting);
    sqlite3_bind_int(st, 7, row->hr_max);
    sqlite3_bind_int(st, 8, row->hr_max_ts_sec);
    sqlite3_bind_int(st, 9, row->hr_min);
    sqlite3_bind_int(st, 10, row->hr_min_ts_sec);
    sqlite3_bind_int(st, 11, row->hr_avg);
    sqlite3_bind_int(st, 12, row->stress_avg);
    sqlite3_bind_int(st, 13, row->stress_max);
    sqlite3_bind_int(st, 14, row->stress_min);
    sqlite3_bind_int(st, 15, row->standing_bits);
    sqlite3_bind_int(st, 16, row->calories);
    sqlite3_bind_int(st, 17, row->spo2_max);
    sqlite3_bind_int(st, 18, row->spo2_max_ts_sec);
    sqlite3_bind_int(st, 19, row->spo2_min);
    sqlite3_bind_int(st, 20, row->spo2_min_ts_sec);
    sqlite3_bind_int(st, 21, row->spo2_avg);

    if (row->has_training_vitality) {
        sqlite3_bind_int(st, 22, row->training_load_day);
        sqlite3_bind_int(st, 23, row->training_load_week);
        sqlite3_bind_int(st, 24, row->training_load_level);
        sqlite3_bind_int(st, 25, row->vitality_increase_light);
        sqlite3_bind_int(st, 26, row->vitality_increase_moderate);
        sqlite3_bind_int(st, 27, row->vitality_increase_high);
        sqlite3_bind_int(st, 28, row->vitality_current);
    } else {
        for (int k = 22; k <= 28; k++) {
            sqlite3_bind_null(st, k);
        }
    }

    gboolean ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    g_mutex_unlock(&s_lock);
    return ok;
}

static gboolean sleep_time_should_skip_insert(int64_t bed_ms, int64_t new_wake_ms) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(
            s_db,
            "SELECT \"wakeupTime\" FROM \"XIAOMI_SLEEP_TIME_SAMPLE\" WHERE \"timestamp\" = ? AND \"deviceId\" = ?;",
            -1,
            &st,
            NULL) != SQLITE_OK) {
        return FALSE;
    }
    sqlite3_bind_int64(st, 1, bed_ms);
    sqlite3_bind_int64(st, 2, s_device_id);
    gboolean skip = FALSE;
    if (sqlite3_step(st) == SQLITE_ROW) {
        if (sqlite3_column_type(st, 0) != SQLITE_NULL) {
            gint64 old_wake = sqlite3_column_int64(st, 0);
            if (old_wake > new_wake_ms) {
                skip = TRUE;
            }
        }
    }
    sqlite3_finalize(st);
    return skip;
}

gboolean gb_db_replace_sleep_stages_v2(uint32_t bed_sec,
                                       uint32_t wake_sec,
                                       uint16_t total_min,
                                       uint16_t deep_min,
                                       uint16_t light_min,
                                       uint16_t rem_min,
                                       uint16_t awake_min,
                                       const GbRawSleepPhaseChange *phases,
                                       size_t n_phases) {
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    const int64_t bed_ms = (int64_t)bed_sec * 1000LL;
    const int64_t wake_ms = (int64_t)wake_sec * 1000LL;

    if (sleep_time_should_skip_insert(bed_ms, wake_ms)) {
        g_mutex_unlock(&s_lock);
        return TRUE;
    }

    sqlite3_stmt *st = NULL;
    const char *sql_time =
        "INSERT OR REPLACE INTO \"XIAOMI_SLEEP_TIME_SAMPLE\" ("
        "\"timestamp\",\"deviceId\",\"userId\",\"wakeupTime\",\"isAwake\",\"totalDuration\",\"deepSleepDuration\","
        "\"lightSleepDuration\",\"remSleepDuration\",\"awakeDuration\") "
        "VALUES (?,?,?,?,0,?,?,?,?,?);";
    if (sqlite3_prepare_v2(s_db, sql_time, -1, &st, NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_bind_int64(st, 1, bed_ms);
    sqlite3_bind_int64(st, 2, s_device_id);
    sqlite3_bind_int64(st, 3, s_user_id);
    sqlite3_bind_int64(st, 4, wake_ms);
    sqlite3_bind_int(st, 5, (int)total_min);
    sqlite3_bind_int(st, 6, (int)deep_min);
    sqlite3_bind_int(st, 7, (int)light_min);
    sqlite3_bind_int(st, 8, (int)rem_min);
    sqlite3_bind_int(st, 9, (int)awake_min);
    if (sqlite3_step(st) != SQLITE_DONE) {
        sqlite3_finalize(st);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_finalize(st);

    if (phases != NULL && n_phases > 0) {
        if (sqlite3_prepare_v2(s_db,
                               "INSERT OR REPLACE INTO \"XIAOMI_SLEEP_STAGE_SAMPLE\" ("
                               "\"timestamp\",\"deviceId\",\"userId\",\"stage\") VALUES (?,?,?,?);",
                               -1,
                               &st,
                               NULL) != SQLITE_OK) {
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        if (sqlite3_exec(s_db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) {
            sqlite3_finalize(st);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        for (size_t i = 0; i < n_phases; i++) {
            int64_t ts_ms = (int64_t)phases[i].t_sec * 1000LL;
            sqlite3_bind_int64(st, 1, ts_ms);
            sqlite3_bind_int64(st, 2, s_device_id);
            sqlite3_bind_int64(st, 3, s_user_id);
            sqlite3_bind_int(st, 4, phases[i].raw_stage);
            if (sqlite3_step(st) != SQLITE_DONE) {
                sqlite3_finalize(st);
                sqlite3_exec(s_db, "ROLLBACK;", NULL, NULL, NULL);
                g_mutex_unlock(&s_lock);
                return FALSE;
            }
            sqlite3_reset(st);
            sqlite3_clear_bindings(st);
        }
        sqlite3_finalize(st);
        sqlite3_exec(s_db, "COMMIT;", NULL, NULL, NULL);
    }

    g_mutex_unlock(&s_lock);
    return TRUE;
}

gboolean gb_db_replace_sleep_details_header(int64_t bed_ms,
                                            int64_t wake_ms,
                                            int is_awake,
                                            int total_min,
                                            int deep_min,
                                            int light_min,
                                            int rem_min,
                                            int awake_min) {
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }

    if (sleep_time_should_skip_insert(bed_ms, wake_ms)) {
        g_mutex_unlock(&s_lock);
        return TRUE;
    }

    sqlite3_stmt *st = NULL;
    const char *sql =
        "INSERT OR REPLACE INTO \"XIAOMI_SLEEP_TIME_SAMPLE\" ("
        "\"timestamp\",\"deviceId\",\"userId\",\"wakeupTime\",\"isAwake\",\"totalDuration\",\"deepSleepDuration\","
        "\"lightSleepDuration\",\"remSleepDuration\",\"awakeDuration\") "
        "VALUES (?,?,?,?,?,?,?,?,?,?);";
    if (sqlite3_prepare_v2(s_db, sql, -1, &st, NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_bind_int64(st, 1, bed_ms);
    sqlite3_bind_int64(st, 2, s_device_id);
    sqlite3_bind_int64(st, 3, s_user_id);
    sqlite3_bind_int64(st, 4, wake_ms);
    sqlite3_bind_int(st, 5, is_awake ? 1 : 0);
    if (total_min < 0) {
        sqlite3_bind_null(st, 6);
    } else {
        sqlite3_bind_int(st, 6, total_min);
    }
    if (deep_min < 0) {
        sqlite3_bind_null(st, 7);
    } else {
        sqlite3_bind_int(st, 7, deep_min);
    }
    if (light_min < 0) {
        sqlite3_bind_null(st, 8);
    } else {
        sqlite3_bind_int(st, 8, light_min);
    }
    if (rem_min < 0) {
        sqlite3_bind_null(st, 9);
    } else {
        sqlite3_bind_int(st, 9, rem_min);
    }
    if (awake_min < 0) {
        sqlite3_bind_null(st, 10);
    } else {
        sqlite3_bind_int(st, 10, awake_min);
    }

    gboolean ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    g_mutex_unlock(&s_lock);
    return ok;
}

gboolean gb_db_replace_sleep_stage_samples(const GbSleepStageRow *rows, size_t n) {
    if (rows == NULL || n == 0) {
        return TRUE;
    }
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s_db,
                           "INSERT OR REPLACE INTO \"XIAOMI_SLEEP_STAGE_SAMPLE\" ("
                           "\"timestamp\",\"deviceId\",\"userId\",\"stage\") VALUES (?,?,?,?);",
                           -1,
                           &st,
                           NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    if (sqlite3_exec(s_db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_finalize(st);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    for (size_t i = 0; i < n; i++) {
        sqlite3_bind_int64(st, 1, rows[i].timestamp_ms);
        sqlite3_bind_int64(st, 2, s_device_id);
        sqlite3_bind_int64(st, 3, s_user_id);
        sqlite3_bind_int(st, 4, rows[i].stage);
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            sqlite3_exec(s_db, "ROLLBACK;", NULL, NULL, NULL);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        sqlite3_reset(st);
        sqlite3_clear_bindings(st);
    }
    sqlite3_finalize(st);
    sqlite3_exec(s_db, "COMMIT;", NULL, NULL, NULL);
    g_mutex_unlock(&s_lock);
    return TRUE;
}

gboolean gb_db_replace_heart_pulse_samples(const GbHeartPulseRow *rows, size_t n) {
    if (rows == NULL || n == 0) {
        return TRUE;
    }
    g_mutex_lock(&s_lock);
    if (!can_write()) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s_db,
                           "INSERT OR REPLACE INTO \"HEART_PULSE_SAMPLE\" ("
                           "\"timestamp\",\"deviceId\",\"userId\") VALUES (?,?,?);",
                           -1,
                           &st,
                           NULL) != SQLITE_OK) {
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    if (sqlite3_exec(s_db, "BEGIN IMMEDIATE;", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_finalize(st);
        g_mutex_unlock(&s_lock);
        return FALSE;
    }
    for (size_t i = 0; i < n; i++) {
        sqlite3_bind_int64(st, 1, rows[i].timestamp_ms);
        sqlite3_bind_int64(st, 2, s_device_id);
        sqlite3_bind_int64(st, 3, s_user_id);
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            sqlite3_exec(s_db, "ROLLBACK;", NULL, NULL, NULL);
            g_mutex_unlock(&s_lock);
            return FALSE;
        }
        sqlite3_reset(st);
        sqlite3_clear_bindings(st);
    }
    sqlite3_finalize(st);
    sqlite3_exec(s_db, "COMMIT;", NULL, NULL, NULL);
    g_mutex_unlock(&s_lock);
    return TRUE;
}
