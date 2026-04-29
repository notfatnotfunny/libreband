#ifndef MIB10_APP_H
#define MIB10_APPH

#include <stdint.h>
#include <bluetooth/bluetooth.h>
#include <glib.h>
#include <gio/gio.h>
#include <gtk/gtk.h>
#include <adwaita.h>

#include <protobuf-c/protobuf-c.h>

#define DEFAULT_AUTH_KEY_HEX "85edca9c826e939125f38516c6d8c4b8"

static inline uint32_t mib10_read_u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline uint16_t mib10_read_u16_le(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

typedef struct {
    AdwApplication *app;
    GtkWidget *window;
    GtkWidget *stack;
    GtkWidget *page_pairing;
    GtkWidget *page_dashboard;
    GtkWidget *stats_row;
    GtkWidget *entry_address;
    GtkWidget *entry_authkey;
    GtkWidget *btn_refresh;
    GtkWidget *listbox_devices;
    GtkWidget *label_status;
    GtkTextBuffer *log_buffer;
    GtkWidget *hr_label;
    GtkWidget *steps_label;
    GtkWidget *calories_label;
    GtkWidget *standing_label;
    GtkWidget *battery_label;
    GtkWidget *moving_label;
    GtkWidget *spo2_label;
    GtkWidget *stress_label;
    GtkWidget *charging_label;
    GtkWidget *wearing_label;
    GtkWidget *sleep_label;
    GtkWidget *hr_bar;
    GtkWidget *steps_bar;
    GtkWidget *calories_bar;
    GtkWidget *standing_bar;
    GtkWidget *battery_bar;
    GtkWidget *moving_bar;
    GtkWidget *recorded_chart;
    GtkWidget *recorded_day_dropdown;
    GtkWidget *recorded_stat_dropdown;
    GtkWidget *recorded_chart_label;

    gchar *selected_address;
    gchar auth_key_hex[33];

    int sock_fd;
    GThread *reader_thread;
    gboolean reader_running;

    uint8_t phone_nonce[16];
    uint8_t watch_nonce[16];
    uint8_t decryption_key[16];
    uint8_t encryption_key[16];
    uint8_t decryption_nonce[4];
    uint8_t encryption_nonce[4];
    gboolean auth_initialized;
    uint8_t spp_v2_seq;
    GByteArray *rx_accumulator;
    GByteArray *activity_chunk_accumulator;
    GQueue *activity_fetch_queue;
    GHashTable *activity_fetch_seen;
    gboolean activity_fetch_inflight;
    uint8_t activity_fetch_current_id[7];
    guint activity_fetch_timeout_source_id;
    GHashTable *recorded_days;
    GHashTable *daily_summary_steps;
    gchar *selected_recorded_day;
    gint selected_recorded_stat;
    gboolean chart_hover_active;
    guint16 chart_hover_minute;
    double chart_touch_drag_start_x;
    gboolean chart_touch_dragging;
    GMutex recorded_lock;
    int protocol_version;
    guint activity_refetch_source_id;
    guint activity_refetch_round;
    guint activity_live_poll_source_id;
    guint activity_live_poll_round;
    gboolean activity_past_requested;
    gboolean sleep_hint_logged;
    gboolean populate_devices_busy;
    gboolean verbose_wire_log;
    gboolean verbose_fetch_log;
    gchar *pending_chart_label;
    guint chart_label_idle_id;
    GQueue *log_queue;
    GMutex log_lock;
    guint log_flush_id;
} AppState;

typedef struct {
    uint16_t minute_of_day;
    int steps;
    int active_calories;
    int distance_cm;
    int heart_rate;
    int energy;
    int spo2;
    int stress;
    int sleep_stage;
    int sleep_source;
} RecordedSample;

typedef struct {
    const uint8_t *header;
    size_t header_len;
    size_t group;
    int current_group_bits;
    uint32_t current_val;
    gboolean current_exists;
} XiaomiComplexParser;

typedef struct {
    AppState *state;
    bdaddr_t bdaddr;
    gchar *normalized;
} ConnectTask;

enum {
    REC_STAT_HEART_RATE = 0,
    REC_STAT_STEPS = 1,
    REC_STAT_CALORIES = 2,
    REC_STAT_SPO2 = 3,
    REC_STAT_STRESS = 4,
    REC_STAT_ENERGY = 5,
    REC_STAT_SLEEP = 6
};

void append_log(AppState *state, const gchar *line);
void set_status(AppState *state, const gchar *status);
void set_connect_busy(AppState *state, gboolean busy);
void switch_to_dashboard(AppState *state);
void switch_to_pairing(AppState *state);
void schedule_metrics_update(AppState *state,
                             gboolean has_hr, guint hr,
                             gboolean has_steps, guint steps,
                             gboolean has_calories, guint calories,
                             gboolean has_standing, guint standing,
                             gboolean has_battery, guint battery,
                             gboolean has_moving, guint moving,
                             const gchar *spo2_text,
                             const gchar *stress_text);
void schedule_state_update(AppState *state, const gchar *charging, const gchar *wearing, const gchar *sleep);
void recorded_store_samples_for_day_key(AppState *state, const gchar *day_key, const RecordedSample *samples, size_t count);
void recorded_store_daily_details(AppState *state, uint32_t ts, const RecordedSample *samples, size_t count);
void recorded_store_daily_summary_steps(AppState *state, uint32_t ts, guint steps);

void handle_activity_payload(AppState *state, const uint8_t *payload, size_t payload_len);
const char *activity_subtype_name(int type, int subtype);

gchar *format_hex(const uint8_t *data, size_t len);
gboolean looks_printable(const uint8_t *data, size_t len);
gboolean hex_to_bytes16(const char *hex, uint8_t out[16]);
uint16_t crc16_arc(const uint8_t *data, size_t len);
gboolean hmac_sha256(const uint8_t *key, size_t key_len,
                     const uint8_t *data, size_t data_len,
                     uint8_t out[32]);
gboolean compute_auth_step3_hmac(const uint8_t secret_key[16],
                                  const uint8_t phone_nonce[16],
                                  const uint8_t watch_nonce[16],
                                  uint8_t out64[64]);
gboolean aes_ccm_encrypt_4byte_tag(const uint8_t key[16],
                                   const uint8_t nonce12[12],
                                   const uint8_t *plaintext,
                                   size_t plaintext_len,
                                   uint8_t **out,
                                   size_t *out_len);
gboolean aes_ctr_crypt(const uint8_t key[16],
                       const uint8_t *ctr_16,
                       const uint8_t *input, size_t input_len,
                       uint8_t **out, size_t *out_len);
gboolean aes_ctr_encrypt(const uint8_t key[16],
                         const uint8_t *nonce_prefix,
                         const uint8_t *plaintext, size_t plaintext_len,
                         uint8_t **out, size_t *out_len);
void pb_append_varint(GByteArray *arr, uint64_t value);
void pb_append_tag(GByteArray *arr, uint32_t field, uint32_t wire_type);
void pb_append_len_bytes(GByteArray *arr, uint32_t field, const uint8_t *data, size_t len);
void pb_append_u32(GByteArray *arr, uint32_t field, uint32_t value);
void pb_append_float(GByteArray *arr, uint32_t field, float value);
gboolean pb_read_varint(const uint8_t *buf, size_t len, size_t *idx, uint64_t *out);
gboolean pb_skip_field(const uint8_t *buf, size_t len, size_t *idx, uint32_t wire_type);
void summarize_pb_message(const ProtobufCMessage *msg, GString *out, int depth);

gboolean bluez_pair_trust_connect(AppState *state, const gchar *address);

void disconnect_socket(AppState *state);
void reset_activity_fetch_state(AppState *state);
void mib10_log_fetch_transition(AppState *state, const char *event, const uint8_t *file_id7);
void start_connect_for_address(AppState *state, const gchar *raw_address);
void mib10_request_history_resync(AppState *state);

gboolean send_activity_fetch_ack(AppState *state, const uint8_t *ids, size_t ids_len);
void trigger_next_activity_fetch(AppState *state);
void gb_run_post_auth_data_fetch(AppState *state);
gboolean parse_mac(const gchar *input, bdaddr_t *out_bdaddr, gchar **normalized);

gpointer connect_worker_main(gpointer user_data);

void mib10_activate(GApplication *application, gpointer user_data);
void mib10_app_shutdown(GApplication *app, gpointer user_data);

#endif
