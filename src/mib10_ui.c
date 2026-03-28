#include "mib10_app.h"
#include "gb_db.h"

#include <math.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <cairo.h>
#include <gio/gio.h>
#include <glib.h>
#include <gtk/gtk.h>
#include <adwaita.h>

#include "xiaomi.pb-c.h"

typedef struct {
    AppState *state;
    gchar *status;
} UiStatusLine;

typedef struct {
    AppState *state;
    uint32_t heart_rate;
    uint32_t steps;
    uint32_t calories;
    uint32_t standing_hours;
    uint32_t battery;
    uint32_t moving;
    gboolean has_heart_rate;
    gboolean has_steps;
    gboolean has_calories;
    gboolean has_standing_hours;
    gboolean has_battery;
    gboolean has_moving;
    gchar *spo2_text;
    gchar *stress_text;
} UiMetricsUpdate;

typedef struct {
    AppState *state;
    gchar *charging_text;
    gchar *wearing_text;
    gchar *sleep_text;
} UiStateUpdate;

typedef struct {
    AppState *state;
    gboolean busy;
} UiBusyState;

static void on_device_row_selected(GtkListBox *box, GtkListBoxRow *row, gpointer user_data) {
    (void)box;
    AppState *state = user_data;

    if (state->selected_address != NULL) {
        g_free(state->selected_address);
        state->selected_address = NULL;
    }

    if (row == NULL) {
        return;
    }

    const gchar *address = g_object_get_data(G_OBJECT(row), "address");
    if (address != NULL) {
        state->selected_address = g_strdup(address);
        gchar *line = g_strdup_printf("Selected: %s", address);
        append_log(state, line);
        g_free(line);
    }
}

static void on_device_row_activated(GtkListBox *box, GtkListBoxRow *row, gpointer user_data) {
    (void)box;
    AppState *state = user_data;
    if (row == NULL) return;
    const gchar *address = g_object_get_data(G_OBJECT(row), "address");
    if (address == NULL) return;
    start_connect_for_address(state, address);
}

/* GTK 4: avoid "Broken accounting of active state" when clearing focus/sensitivity while a widget is still "active" from a click. */
static void ui_defocus_if_in_subtree(GtkWidget *subtree) {
    if (subtree == NULL || !gtk_widget_get_mapped(subtree)) {
        return;
    }
    GtkRoot *root = gtk_widget_get_root(subtree);
    if (root == NULL) {
        return;
    }
    GtkWidget *focus = gtk_root_get_focus(root);
    if (focus != NULL && gtk_widget_is_ancestor(focus, subtree)) {
        gtk_root_set_focus(root, NULL);
    }
}

static void populate_device_list(AppState *state) {
    if (state->listbox_devices == NULL) {
        return;
    }
    if (state->populate_devices_busy) {
        return;
    }
    state->populate_devices_busy = TRUE;

    ui_defocus_if_in_subtree(state->listbox_devices);
    gtk_list_box_select_row(GTK_LIST_BOX(state->listbox_devices), NULL);
    gtk_list_box_remove_all(GTK_LIST_BOX(state->listbox_devices));
    gtk_widget_set_sensitive(state->listbox_devices, FALSE);

    const gchar *keep_selected = state->selected_address;

    GError *error = NULL;
    GDBusConnection *bus = g_bus_get_sync(G_BUS_TYPE_SYSTEM, NULL, &error);
    if (bus == NULL) {
        set_status(state, "Failed to open system bus.");
        if (error != NULL) {
            g_error_free(error);
        }
        gtk_widget_set_sensitive(state->listbox_devices, TRUE);
        state->populate_devices_busy = FALSE;
        return;
    }

    GVariant *reply = g_dbus_connection_call_sync(
        bus,
        "org.bluez",
        "/",
        "org.freedesktop.DBus.ObjectManager",
        "GetManagedObjects",
        NULL,
        G_VARIANT_TYPE("(a{oa{sa{sv}}})"),
        G_DBUS_CALL_FLAGS_NONE,
        -1,
        NULL,
        &error
    );

    if (reply == NULL) {
        set_status(state, error != NULL && error->message != NULL ? error->message : "Failed to list devices.");
        if (error != NULL) {
            g_error_free(error);
        }
        g_object_unref(bus);
        gtk_widget_set_sensitive(state->listbox_devices, TRUE);
        state->populate_devices_busy = FALSE;
        return;
    }

    GVariant *objects = g_variant_get_child_value(reply, 0);
    GVariantIter iter;
    g_variant_iter_init(&iter, objects);

    GtkListBoxRow *first_row = NULL;
    GtkListBoxRow *match_row = NULL;

    while (TRUE) {
        GVariant *entry = g_variant_iter_next_value(&iter);
        if (entry == NULL) {
            break;
        }

        gchar *path = NULL;
        GVariant *ifaces = NULL;
        g_variant_get(entry, "{o@a{sa{sv}}}", &path, &ifaces);

        GVariant *device_props = g_variant_lookup_value(ifaces, "org.bluez.Device1", G_VARIANT_TYPE("a{sv}"));
        if (device_props != NULL) {
            GVariantDict *dict = g_variant_dict_new(device_props);

            const gchar *address = NULL;
            gboolean have_address = g_variant_dict_lookup(dict, "Address", "&s", &address);

            gboolean paired = FALSE;
            (void)g_variant_dict_lookup(dict, "Paired", "b", &paired);

            gboolean trusted = FALSE;
            (void)g_variant_dict_lookup(dict, "Trusted", "b", &trusted);
            gboolean connected = FALSE;
            (void)g_variant_dict_lookup(dict, "Connected", "b", &connected);

            const gchar *name = NULL;
            (void)g_variant_dict_lookup(dict, "Name", "&s", &name);

            const gchar *alias = NULL;
            (void)g_variant_dict_lookup(dict, "Alias", "&s", &alias);

            if (have_address && address != NULL) {
                if (name == NULL || name[0] == '\0') {
                    name = alias;
                }
                if (name == NULL || name[0] == '\0') {
                    name = "(unknown)";
                }

                gchar *label = g_strdup_printf("%s\n%s  [paired:%s trusted:%s connected:%s]",
                                               name,
                                               address,
                                               paired ? "yes" : "no",
                                               trusted ? "yes" : "no",
                                               connected ? "yes" : "no");
                GtkWidget *row = gtk_list_box_row_new();
                GtkWidget *row_child = gtk_label_new(label);
                gtk_widget_set_margin_start(row_child, 6);
                gtk_widget_set_margin_end(row_child, 6);
                gtk_label_set_xalign(GTK_LABEL(row_child), 0.0f);
                gtk_list_box_row_set_child(GTK_LIST_BOX_ROW(row), row_child);
                g_object_set_data_full(G_OBJECT(row), "address", g_strdup(address), g_free);
                gtk_list_box_append(GTK_LIST_BOX(state->listbox_devices), row);
                g_free(label);

                if (first_row == NULL) {
                    first_row = GTK_LIST_BOX_ROW(row);
                }
                if (keep_selected != NULL && g_strcmp0(keep_selected, address) == 0) {
                    match_row = GTK_LIST_BOX_ROW(row);
                }
            }

            g_variant_dict_unref(dict);
            g_variant_unref(device_props);
        }

        g_variant_unref(ifaces);
        g_free(path);
        g_variant_unref(entry);
    }

    if (match_row != NULL) {
        gtk_list_box_select_row(GTK_LIST_BOX(state->listbox_devices), match_row);
    } else if (first_row != NULL) {
        gtk_list_box_select_row(GTK_LIST_BOX(state->listbox_devices), first_row);
    }

    g_variant_unref(objects);
    g_variant_unref(reply);
    g_object_unref(bus);

    gtk_widget_set_sensitive(state->listbox_devices, TRUE);
    set_status(state, "Select a device to pair/connect.");
    state->populate_devices_busy = FALSE;
}

static void on_refresh_clicked(GtkButton *button, gpointer user_data) {
    (void)button;
    AppState *state = user_data;
    if (state->auth_initialized && state->sock_fd >= 0) {
        append_log(state, "Refresh: repeating Gadgetbridge-style fetch (user info, configs, realtime, battery, history)…");
        gb_run_post_auth_data_fetch(state);
        return;
    }
    populate_device_list(state);
}

#define APP_LOG_MAX_CHARS 300000

static gboolean ui_flush_log_queue_idle(gpointer user_data) {
    AppState *state = user_data;
    GString *chunk = g_string_new(NULL);

    g_mutex_lock(&state->log_lock);
    while (state->log_queue != NULL && g_queue_get_length(state->log_queue) > 0) {
        gchar *ln = g_queue_pop_head(state->log_queue);
        g_mutex_unlock(&state->log_lock);
        g_string_append(chunk, ln);
        g_string_append_c(chunk, '\n');
        g_free(ln);
        g_mutex_lock(&state->log_lock);
    }
    state->log_flush_id = 0;
    g_mutex_unlock(&state->log_lock);

    if (chunk->len > 0 && state->log_buffer != NULL) {
        GtkTextIter end;
        gtk_text_buffer_begin_user_action(state->log_buffer);
        gtk_text_buffer_get_end_iter(state->log_buffer, &end);
        gtk_text_buffer_insert(state->log_buffer, &end, chunk->str, (gint)chunk->len);
        gint nchars = gtk_text_buffer_get_char_count(state->log_buffer);
        if (nchars > APP_LOG_MAX_CHARS) {
            GtkTextIter start, cut;
            gtk_text_buffer_get_start_iter(state->log_buffer, &start);
            gtk_text_buffer_get_iter_at_offset(state->log_buffer, &cut, nchars - APP_LOG_MAX_CHARS);
            gtk_text_buffer_delete(state->log_buffer, &start, &cut);
        }
        gtk_text_buffer_end_user_action(state->log_buffer);
    }
    g_string_free(chunk, TRUE);

    g_mutex_lock(&state->log_lock);
    if (state->log_queue != NULL && g_queue_get_length(state->log_queue) > 0 && state->log_flush_id == 0) {
        state->log_flush_id = g_idle_add(ui_flush_log_queue_idle, state);
    }
    g_mutex_unlock(&state->log_lock);
    return G_SOURCE_REMOVE;
}

void append_log(AppState *state, const gchar *line) {
    if (line == NULL) return;
    g_mutex_lock(&state->log_lock);
    if (state->log_queue == NULL) {
        state->log_queue = g_queue_new();
    }
    g_queue_push_tail(state->log_queue, g_strdup(line));
    if (state->log_flush_id == 0) {
        state->log_flush_id = g_idle_add(ui_flush_log_queue_idle, state);
    }
    g_mutex_unlock(&state->log_lock);
}

static gboolean ui_set_status_cb(gpointer user_data) {
    UiStatusLine *ctx = user_data;
    gtk_label_set_text(GTK_LABEL(ctx->state->label_status), ctx->status);
    g_free(ctx->status);
    g_free(ctx);
    return G_SOURCE_REMOVE;
}

static gboolean ui_set_busy_cb(gpointer user_data) {
    UiBusyState *ctx = user_data;
    if (ctx->busy && ctx->state->window != NULL) {
        GtkRoot *root = gtk_widget_get_root(ctx->state->window);
        GtkWidget *f = gtk_root_get_focus(root);
        if (f != NULL) {
            if (ctx->state->btn_refresh != NULL && f == ctx->state->btn_refresh) {
                gtk_root_set_focus(root, NULL);
            }
        }
    }
    if (ctx->state->btn_refresh != NULL) {
        gtk_widget_set_sensitive(ctx->state->btn_refresh, !ctx->busy);
    }
    if (ctx->state->listbox_devices != NULL) {
        if (ctx->busy) {
            ui_defocus_if_in_subtree(ctx->state->listbox_devices);
        }
        gtk_widget_set_sensitive(ctx->state->listbox_devices, !ctx->busy);
    }
    if (ctx->state->entry_authkey != NULL) {
        gtk_widget_set_sensitive(ctx->state->entry_authkey, !ctx->busy);
    }
    g_free(ctx);
    return G_SOURCE_REMOVE;
}

void set_connect_busy(AppState *state, gboolean busy) {
    UiBusyState *ctx = g_new0(UiBusyState, 1);
    ctx->state = state;
    ctx->busy = busy;
    g_idle_add(ui_set_busy_cb, ctx);
}

void set_status(AppState *state, const gchar *status) {
    UiStatusLine *ctx = g_new0(UiStatusLine, 1);
    ctx->state = state;
    ctx->status = g_strdup(status);
    g_idle_add(ui_set_status_cb, ctx);
}

static void metrics_set_bar(GtkWidget *bar, guint value, guint max) {
    if (bar == NULL || max == 0) {
        return;
    }
    double fraction = (double)value / (double)max;
    if (fraction < 0.0) fraction = 0.0;
    if (fraction > 1.0) fraction = 1.0;
    gtk_progress_bar_set_fraction(GTK_PROGRESS_BAR(bar), fraction);
}

static gboolean ui_update_metrics_cb(gpointer user_data) {
    UiMetricsUpdate *u = user_data;
    AppState *state = u->state;

    if (u->has_heart_rate && state->hr_label != NULL) {
        gchar *txt = g_strdup_printf("%u bpm", u->heart_rate);
        gtk_label_set_text(GTK_LABEL(state->hr_label), txt);
        g_free(txt);
        metrics_set_bar(state->hr_bar, u->heart_rate, 220);
    }
    if (u->has_steps && state->steps_label != NULL) {
        gchar *txt = g_strdup_printf("%u", u->steps);
        gtk_label_set_text(GTK_LABEL(state->steps_label), txt);
        g_free(txt);
        metrics_set_bar(state->steps_bar, u->steps, 12000);
    }
    if (u->has_calories && state->calories_label != NULL) {
        gchar *txt = g_strdup_printf("%u kcal", u->calories);
        gtk_label_set_text(GTK_LABEL(state->calories_label), txt);
        g_free(txt);
        metrics_set_bar(state->calories_bar, u->calories, 1000);
    }
    if (u->has_standing_hours && state->standing_label != NULL) {
        gchar *txt = g_strdup_printf("%u h", u->standing_hours);
        gtk_label_set_text(GTK_LABEL(state->standing_label), txt);
        g_free(txt);
        metrics_set_bar(state->standing_bar, u->standing_hours, 16);
    }
    if (u->has_battery && state->battery_label != NULL) {
        gchar *txt = g_strdup_printf("%u%%", u->battery);
        gtk_label_set_text(GTK_LABEL(state->battery_label), txt);
        g_free(txt);
        metrics_set_bar(state->battery_bar, u->battery, 100);
    }
    if (u->has_moving && state->moving_label != NULL) {
        gchar *txt = g_strdup_printf("%u", u->moving);
        gtk_label_set_text(GTK_LABEL(state->moving_label), txt);
        g_free(txt);
        metrics_set_bar(state->moving_bar, u->moving, 180);
    }
    if (u->spo2_text != NULL && state->spo2_label != NULL) {
        gtk_label_set_text(GTK_LABEL(state->spo2_label), u->spo2_text);
    }
    if (u->stress_text != NULL && state->stress_label != NULL) {
        gtk_label_set_text(GTK_LABEL(state->stress_label), u->stress_text);
    }

    g_free(u->spo2_text);
    g_free(u->stress_text);
    g_free(u);
    return G_SOURCE_REMOVE;
}

void schedule_metrics_update(AppState *state,
                                    gboolean has_hr, guint hr,
                                    gboolean has_steps, guint steps,
                                    gboolean has_calories, guint calories,
                                    gboolean has_standing, guint standing,
                                    gboolean has_battery, guint battery,
                                    gboolean has_moving, guint moving,
                                    const gchar *spo2_text,
                                    const gchar *stress_text) {
    UiMetricsUpdate *u = g_new0(UiMetricsUpdate, 1);
    u->state = state;
    u->has_heart_rate = has_hr;
    u->heart_rate = hr;
    u->has_steps = has_steps;
    u->steps = steps;
    u->has_calories = has_calories;
    u->calories = calories;
    u->has_standing_hours = has_standing;
    u->standing_hours = standing;
    u->has_battery = has_battery;
    u->battery = battery;
    u->has_moving = has_moving;
    u->moving = moving;
    u->spo2_text = spo2_text ? g_strdup(spo2_text) : NULL;
    u->stress_text = stress_text ? g_strdup(stress_text) : NULL;
    g_idle_add(ui_update_metrics_cb, u);
}

static gboolean ui_update_state_cb(gpointer user_data) {
    UiStateUpdate *u = user_data;
    if (u->charging_text && u->state->charging_label) {
        gtk_label_set_text(GTK_LABEL(u->state->charging_label), u->charging_text);
    }
    if (u->wearing_text && u->state->wearing_label) {
        gtk_label_set_text(GTK_LABEL(u->state->wearing_label), u->wearing_text);
    }
    if (u->sleep_text && u->state->sleep_label) {
        gtk_label_set_text(GTK_LABEL(u->state->sleep_label), u->sleep_text);
    }
    g_free(u->charging_text);
    g_free(u->wearing_text);
    g_free(u->sleep_text);
    g_free(u);
    return G_SOURCE_REMOVE;
}

void schedule_state_update(AppState *state, const gchar *charging, const gchar *wearing, const gchar *sleep) {
    UiStateUpdate *u = g_new0(UiStateUpdate, 1);
    u->state = state;
    u->charging_text = charging ? g_strdup(charging) : NULL;
    u->wearing_text = wearing ? g_strdup(wearing) : NULL;
    u->sleep_text = sleep ? g_strdup(sleep) : NULL;
    g_idle_add(ui_update_state_cb, u);
}


static const gchar *recorded_stat_name(gint stat) {
    switch (stat) {
        case REC_STAT_HEART_RATE: return "Heart rate";
        case REC_STAT_STEPS: return "Steps + Distance";
        case REC_STAT_CALORIES: return "Active calories";
        case REC_STAT_SPO2: return "SpO2";
        case REC_STAT_STRESS: return "Stress";
        case REC_STAT_ENERGY: return "Energy";
        case REC_STAT_SLEEP: return "Sleep";
        default: return "Unknown";
    }
}

static int recorded_sample_value(const RecordedSample *s, gint stat) {
    switch (stat) {
        case REC_STAT_HEART_RATE: return s->heart_rate;
        case REC_STAT_STEPS: return s->steps;
        case REC_STAT_CALORIES: return s->active_calories;
        case REC_STAT_SPO2: return s->spo2;
        case REC_STAT_STRESS: return s->stress;
        case REC_STAT_ENERGY: return s->energy;
        case REC_STAT_SLEEP: return s->sleep_stage;
        default: return -1;
    }
}

static void recorded_day_value_free(gpointer value) {
    if (value != NULL) {
        g_array_unref((GArray *)value);
    }
}

static GArray *recorded_create_day_array(void) {
    GArray *arr = g_array_sized_new(FALSE, FALSE, sizeof(RecordedSample), 1440);
    for (guint m = 0; m < 1440; m++) {
        RecordedSample s = {0};
        s.minute_of_day = (uint16_t)m;
        s.steps = -1;
        s.active_calories = -1;
        s.distance_cm = -1;
        s.heart_rate = -1;
        s.energy = -1;
        s.spo2 = -1;
        s.stress = -1;
        s.sleep_stage = -1;
        s.sleep_source = 0;
        g_array_append_val(arr, s);
    }
    return arr;
}

static gboolean apply_chart_label_idle(gpointer user_data) {
    AppState *state = user_data;
    state->chart_label_idle_id = 0;
    if (state->recorded_chart_label == NULL || state->pending_chart_label == NULL) {
        return G_SOURCE_REMOVE;
    }
    const char *shown = gtk_label_get_label(GTK_LABEL(state->recorded_chart_label));
    if (g_strcmp0(shown, state->pending_chart_label) == 0) {
        return G_SOURCE_REMOVE;
    }
    gtk_label_set_text(GTK_LABEL(state->recorded_chart_label), state->pending_chart_label);
    return G_SOURCE_REMOVE;
}

/* Defer label updates to idle; skip if text unchanged to avoid layout thrash / flicker on chart hover. */
static void chart_submit_caption(AppState *state, gchar *owned) {
    if (owned == NULL) {
        return;
    }
    if (state->pending_chart_label != NULL && g_strcmp0(state->pending_chart_label, owned) == 0) {
        g_free(owned);
        return;
    }
    g_free(state->pending_chart_label);
    state->pending_chart_label = owned;
    if (state->chart_label_idle_id == 0) {
        state->chart_label_idle_id = g_idle_add(apply_chart_label_idle, state);
    }
}

static void chart_submit_caption_literal(AppState *state, const gchar *text) {
    chart_submit_caption(state, g_strdup(text ? text : ""));
}

static void recorded_refresh_ui(AppState *state) {
    if (state->recorded_chart != NULL) {
        gtk_widget_queue_draw(state->recorded_chart);
    }
}

typedef struct {
    AppState *state;
    gchar *day_key;
    GArray *samples;
} RecordedStoreCtx;

typedef struct {
    AppState *state;
    gchar *day_key;
    guint steps;
} DailySummaryStoreCtx;

static gboolean ui_recorded_store_cb(gpointer user_data) {
    RecordedStoreCtx *ctx = user_data;
    AppState *state = ctx->state;

    g_mutex_lock(&state->recorded_lock);
    if (state->recorded_days == NULL) {
        state->recorded_days = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, recorded_day_value_free);
    }
    GArray *day = g_hash_table_lookup(state->recorded_days, ctx->day_key);
    if (day == NULL) {
        day = recorded_create_day_array();
        g_hash_table_insert(state->recorded_days, g_strdup(ctx->day_key), day);
    }
    for (guint i = 0; i < ctx->samples->len; i++) {
        const RecordedSample *src = &g_array_index(ctx->samples, RecordedSample, i);
        const guint idx = src->minute_of_day;
        if (idx >= day->len) continue;
        RecordedSample *dst = &g_array_index(day, RecordedSample, idx);
        if (src->steps >= 0) dst->steps = src->steps;
        if (src->active_calories >= 0) dst->active_calories = src->active_calories;
        if (src->distance_cm >= 0) dst->distance_cm = src->distance_cm;
        if (src->heart_rate >= 0) dst->heart_rate = src->heart_rate;
        if (src->energy >= 0) dst->energy = src->energy;
        if (src->spo2 >= 0) dst->spo2 = src->spo2;
        if (src->stress >= 0) dst->stress = src->stress;
        if (src->sleep_stage >= 0) dst->sleep_stage = src->sleep_stage;
        if (src->sleep_source > 0) dst->sleep_source = 1;
    }
    if (state->recorded_day_dropdown != NULL) {
        gboolean found = FALSE;
        guint idx = 0;
        GtkStringList *sl = GTK_STRING_LIST(gtk_drop_down_get_model(GTK_DROP_DOWN(state->recorded_day_dropdown)));
        guint n = g_list_model_get_n_items(G_LIST_MODEL(sl));
        if (n > 0) {
            gtk_string_list_splice(sl, 0, n, NULL);
        }
        ui_defocus_if_in_subtree(state->recorded_day_dropdown);
        GList *keys = g_hash_table_get_keys(state->recorded_days);
        keys = g_list_sort(keys, (GCompareFunc)g_strcmp0);
        for (GList *l = keys; l != NULL; l = l->next) {
            const gchar *k = l->data;
            gtk_string_list_append(sl, k);
            if (g_strcmp0(k, ctx->day_key) == 0) {
                found = TRUE;
                gtk_drop_down_set_selected(GTK_DROP_DOWN(state->recorded_day_dropdown), idx);
            }
            idx++;
        }
        g_list_free(keys);
        if (!found && idx > 0) {
            gtk_drop_down_set_selected(GTK_DROP_DOWN(state->recorded_day_dropdown), 0);
        }
    }
    g_mutex_unlock(&state->recorded_lock);

    g_free(state->selected_recorded_day);
    if (state->recorded_day_dropdown != NULL) {
        GObject *item = gtk_drop_down_get_selected_item(GTK_DROP_DOWN(state->recorded_day_dropdown));
        if (item != NULL && GTK_IS_STRING_OBJECT(item)) {
            const char *s = gtk_string_object_get_string(GTK_STRING_OBJECT(item));
            state->selected_recorded_day = (s != NULL && s[0] != '\0') ? g_strdup(s) : g_strdup(ctx->day_key);
        } else {
            state->selected_recorded_day = g_strdup(ctx->day_key);
        }
    } else {
        state->selected_recorded_day = g_strdup(ctx->day_key);
    }

    recorded_refresh_ui(state);
    g_array_unref(ctx->samples);
    g_free(ctx->day_key);
    g_free(ctx);
    return G_SOURCE_REMOVE;
}

static gboolean ui_daily_summary_store_cb(gpointer user_data) {
    DailySummaryStoreCtx *ctx = user_data;
    AppState *state = ctx->state;

    g_mutex_lock(&state->recorded_lock);
    if (state->daily_summary_steps == NULL) {
        state->daily_summary_steps = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
    }
    guint *steps_ptr = g_new(guint, 1);
    *steps_ptr = ctx->steps;
    g_hash_table_replace(state->daily_summary_steps, g_strdup(ctx->day_key), steps_ptr);
    g_mutex_unlock(&state->recorded_lock);

    recorded_refresh_ui(state);
    g_free(ctx->day_key);
    g_free(ctx);
    return G_SOURCE_REMOVE;
}

void recorded_store_samples_for_day_key(AppState *state, const gchar *day_key, const RecordedSample *samples, size_t count) {
    if (samples == NULL || count == 0 || day_key == NULL) return;
    GArray *arr = g_array_sized_new(FALSE, FALSE, sizeof(RecordedSample), (guint)count);
    g_array_append_vals(arr, samples, (guint)count);
    RecordedStoreCtx *ctx = g_new0(RecordedStoreCtx, 1);
    ctx->state = state;
    ctx->day_key = g_strdup(day_key);
    ctx->samples = arr;
    g_idle_add(ui_recorded_store_cb, ctx);
}

void recorded_store_daily_details(AppState *state, uint32_t ts, const RecordedSample *samples, size_t count) {
    if (samples == NULL || count == 0) return;
    GDateTime *dt = g_date_time_new_from_unix_local((gint64)ts);
    if (dt == NULL) return;
    gchar *day_key = g_date_time_format(dt, "%Y-%m-%d");
    g_date_time_unref(dt);
    recorded_store_samples_for_day_key(state, day_key, samples, count);
    g_free(day_key);
}

void recorded_store_daily_summary_steps(AppState *state, uint32_t ts, guint steps) {
    GDateTime *dt = g_date_time_new_from_unix_local((gint64)ts);
    if (dt == NULL) return;
    gchar *day_key = g_date_time_format(dt, "%Y-%m-%d");
    g_date_time_unref(dt);

    DailySummaryStoreCtx *ctx = g_new0(DailySummaryStoreCtx, 1);
    ctx->state = state;
    ctx->day_key = day_key;
    ctx->steps = steps;
    g_idle_add(ui_daily_summary_store_cb, ctx);
}

void switch_to_dashboard(AppState *state) {
    if (state->stack != NULL && state->page_dashboard != NULL) {
        gtk_stack_set_visible_child(GTK_STACK(state->stack), state->page_dashboard);
    }
}

void switch_to_pairing(AppState *state) {
    if (state->stack != NULL && state->page_pairing != NULL) {
        gtk_stack_set_visible_child(GTK_STACK(state->stack), state->page_pairing);
    }
}

void start_connect_for_address(AppState *state, const gchar *raw_address) {
    set_connect_busy(state, TRUE);
    disconnect_socket(state);

    if (raw_address == NULL || raw_address[0] == '\0') {
        set_status(state, "No device selected.");
        set_connect_busy(state, FALSE);
        return;
    }

    const gchar *auth_text = NULL;
    if (state->entry_authkey != NULL) {
        auth_text = gtk_editable_get_text(GTK_EDITABLE(state->entry_authkey));
    }
    if (auth_text == NULL || !hex_to_bytes16(auth_text, (uint8_t[16]){0})) {
        set_status(state, "Invalid auth key. Enter 32 hex chars.");
        set_connect_busy(state, FALSE);
        return;
    }
    g_strlcpy(state->auth_key_hex, auth_text, sizeof(state->auth_key_hex));

    bdaddr_t bdaddr = {0};
    gchar *normalized = NULL;
    if (!parse_mac(raw_address, &bdaddr, &normalized)) {
        set_status(state, "Invalid address format. Use AA:BB:CC:DD:EE:FF");
        set_connect_busy(state, FALSE);
        return;
    }

    ConnectTask *task = g_new0(ConnectTask, 1);
    task->state = state;
    task->bdaddr = bdaddr;
    task->normalized = normalized;
    g_thread_new("connect-worker", connect_worker_main, task);
}

void mib10_app_shutdown(GApplication *app, gpointer user_data) {
    (void)app;
    AppState *state = user_data;
    if (state->activity_refetch_source_id != 0) {
        g_source_remove(state->activity_refetch_source_id);
        state->activity_refetch_source_id = 0;
    }
    if (state->activity_live_poll_source_id != 0) {
        g_source_remove(state->activity_live_poll_source_id);
        state->activity_live_poll_source_id = 0;
    }
    reset_activity_fetch_state(state);
    if (state->activity_fetch_queue != NULL) {
        g_queue_free(state->activity_fetch_queue);
        state->activity_fetch_queue = NULL;
    }
    if (state->activity_fetch_seen != NULL) {
        g_hash_table_unref(state->activity_fetch_seen);
        state->activity_fetch_seen = NULL;
    }
    disconnect_socket(state);
    if (state->log_flush_id != 0) {
        g_source_remove(state->log_flush_id);
        state->log_flush_id = 0;
    }
    g_mutex_lock(&state->log_lock);
    if (state->log_queue != NULL) {
        while (g_queue_get_length(state->log_queue) > 0) {
            g_free(g_queue_pop_head(state->log_queue));
        }
        g_queue_free(state->log_queue);
        state->log_queue = NULL;
    }
    g_mutex_unlock(&state->log_lock);
    if (state->chart_label_idle_id != 0) {
        g_source_remove(state->chart_label_idle_id);
        state->chart_label_idle_id = 0;
    }
    g_free(state->pending_chart_label);
    state->pending_chart_label = NULL;
    g_mutex_lock(&state->recorded_lock);
    if (state->recorded_days != NULL) {
        g_hash_table_unref(state->recorded_days);
        state->recorded_days = NULL;
    }
    if (state->daily_summary_steps != NULL) {
        g_hash_table_unref(state->daily_summary_steps);
        state->daily_summary_steps = NULL;
    }
    g_mutex_unlock(&state->recorded_lock);
    g_free(state->selected_recorded_day);
    state->selected_recorded_day = NULL;
    gb_db_close();
}

static void on_recorded_day_selected(GObject *object, GParamSpec *pspec, gpointer user_data) {
    (void)pspec;
    GtkDropDown *dd = GTK_DROP_DOWN(object);
    AppState *state = user_data;
    GObject *item = gtk_drop_down_get_selected_item(dd);
    g_free(state->selected_recorded_day);
    if (item != NULL && GTK_IS_STRING_OBJECT(item)) {
        const char *s = gtk_string_object_get_string(GTK_STRING_OBJECT(item));
        state->selected_recorded_day = (s != NULL && s[0] != '\0') ? g_strdup(s) : NULL;
    } else {
        state->selected_recorded_day = NULL;
    }
    state->chart_hover_active = FALSE;
    recorded_refresh_ui(state);
}

static void on_recorded_stat_selected(GObject *object, GParamSpec *pspec, gpointer user_data) {
    (void)pspec;
    GtkDropDown *dd = GTK_DROP_DOWN(object);
    AppState *state = user_data;
    guint pos = gtk_drop_down_get_selected(dd);
    if (pos != GTK_INVALID_LIST_POSITION) {
        state->selected_recorded_stat = (gint)pos;
    } else {
        state->selected_recorded_stat = REC_STAT_HEART_RATE;
    }
    state->chart_hover_active = FALSE;
    recorded_refresh_ui(state);
}

static guint16 chart_x_to_minute(double x, double left, double w) {
    double t = (x - left) / w;
    if (t < 0.0) t = 0.0;
    if (t > 1.0) t = 1.0;
    return (guint16)(t * 1439.0);
}

static gboolean compute_sleep_window_from_array(const GArray *arr, guint16 *out_first, guint16 *out_last) {
    if (arr == NULL || arr->len == 0 || out_first == NULL || out_last == NULL) return FALSE;

    /* Data-driven sleep axis: prefer samples from sleep payloads. */
    guint first_stage = 1440, last_stage = 0;
    for (guint i = 0; i < arr->len; i++) {
        const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
        if (s->sleep_source > 0) {
            if (i < first_stage) first_stage = i;
            if (i > last_stage) last_stage = i;
        }
    }
    if (first_stage < 1440 && last_stage > first_stage) {
        *out_first = (guint16)first_stage;
        *out_last = (guint16)last_stage;
        return TRUE;
    }

    /* Secondary: explicit sleep stage points, even if source flag wasn't preserved. */
    first_stage = 1440; last_stage = 0;
    for (guint i = 0; i < arr->len; i++) {
        const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
        if (s->sleep_stage >= 0) {
            if (i < first_stage) first_stage = i;
            if (i > last_stage) last_stage = i;
        }
    }
    if (first_stage < 1440 && last_stage > first_stage) {
        *out_first = (guint16)first_stage;
        *out_last = (guint16)last_stage;
        return TRUE;
    }

    return FALSE;
}

static gboolean sleep_window_for_selected_day(AppState *state, guint16 *out_first, guint16 *out_last) {
    if (state->selected_recorded_day == NULL || state->recorded_days == NULL) return FALSE;
    gboolean ok = FALSE;
    g_mutex_lock(&state->recorded_lock);
    GArray *arr = g_hash_table_lookup(state->recorded_days, state->selected_recorded_day);
    ok = compute_sleep_window_from_array(arr, out_first, out_last);
    g_mutex_unlock(&state->recorded_lock);
    return ok;
}

static guint16 chart_x_to_minute_for_current_stat(AppState *state, double x, double left, double w) {
    guint16 m = chart_x_to_minute(x, left, w);
    if (state->selected_recorded_stat == REC_STAT_SLEEP) {
        guint16 first_m = 0, last_m = 0;
        if (sleep_window_for_selected_day(state, &first_m, &last_m)) {
            double t = (x - left) / w;
            if (t < 0.0) t = 0.0;
            if (t > 1.0) t = 1.0;
            guint span = (guint)(last_m - first_m);
            m = (guint16)(first_m + (guint)(t * (double)span + 0.5));
        }
    }
    return m;
}

typedef struct {
    double left, top, right, bottom, w, h;
} ChartPlotGeom;

static gboolean chart_plot_geometry(int width, int height, ChartPlotGeom *g) {
    if (width <= 1 || height <= 1 || g == NULL) {
        return FALSE;
    }
    g->left = fmax(34.0, fmin(54.0, width * 0.105));
    g->right = fmax(38.0, fmin(60.0, width * 0.095));
    g->top = fmax(7.0, fmin(16.0, height * 0.065));
    g->bottom = fmax(20.0, fmin(36.0, height * 0.155));
    g->w = (double)width - g->left - g->right;
    g->h = (double)height - g->top - g->bottom;
    return g->w > 1.0 && g->h > 1.0;
}

static void chart_pointer_set_hover_x(AppState *state, double x, gboolean from_motion_controller) {
    if (state->recorded_chart == NULL) {
        return;
    }
    if (from_motion_controller && state->chart_touch_dragging) {
        return;
    }
    int width = gtk_widget_get_width(state->recorded_chart);
    int height = gtk_widget_get_height(state->recorded_chart);
    ChartPlotGeom geo;
    if (!chart_plot_geometry(width, height, &geo)) {
        return;
    }
    guint16 nm = chart_x_to_minute_for_current_stat(state, x, geo.left, geo.w);
    state->chart_hover_active = TRUE;
    state->chart_hover_minute = nm;
    gtk_widget_queue_draw(state->recorded_chart);
}

static void update_responsive_layout(AppState *state) {
    if (state->window == NULL || state->stats_row == NULL || state->recorded_chart == NULL) {
        return;
    }
    int ww = gtk_widget_get_width(state->window);
    if (ww <= 0) {
        return;
    }

    gtk_orientable_set_orientation(GTK_ORIENTABLE(state->stats_row), GTK_ORIENTATION_HORIZONTAL);
    gtk_box_set_spacing(GTK_BOX(state->stats_row), ww < 700 ? 10 : 14);

    GtkWidget *first = gtk_widget_get_first_child(state->stats_row);
    GtkWidget *second = first ? gtk_widget_get_next_sibling(first) : NULL;
    if (first != NULL) {
        gtk_widget_set_hexpand(first, FALSE);
        gtk_widget_set_size_request(first, CLAMP((int)(ww * 0.30), 150, 240), -1);
    }
    if (second != NULL) {
        gtk_widget_set_hexpand(second, TRUE);
        gtk_widget_set_size_request(second, -1, -1);
    }

    gtk_widget_set_hexpand(state->recorded_chart, TRUE);

    int chart_h;
    if (ww < 620) {
        chart_h = 170;
    } else if (ww < 900) {
        chart_h = 190;
    } else if (ww < 1100) {
        chart_h = 205;
    } else {
        chart_h = 220;
    }
    gtk_widget_set_size_request(state->recorded_chart, -1, chart_h);
}

static gboolean on_window_tick(GtkWidget *widget, GdkFrameClock *clock, gpointer user_data) {
    (void)widget;
    (void)clock;
    AppState *state = user_data;
    update_responsive_layout(state);
    return G_SOURCE_CONTINUE;
}

/* Tooltip / labels: only a value actually stored for that minute (no blending or neighbor fill). */
static gboolean recorded_value_exact(const GArray *arr, gint stat, guint16 minute, int *out_value) {
    if (arr == NULL || out_value == NULL || minute >= arr->len) return FALSE;
    int v = recorded_sample_value(&g_array_index(arr, RecordedSample, minute), stat);
    if (v < 0) return FALSE;
    *out_value = v;
    return TRUE;
}

static void chart_draw_y_labels(cairo_t *cr, double left, double top, double h, int min_v, int max_v,
                                double lr, double lg, double lb) {
    cairo_set_source_rgb(cr, lr, lg, lb);
    cairo_set_font_size(cr, 10.0);
    for (int i = 0; i <= 5; i++) {
        double y = top + (h * i / 5.0);
        int v = max_v - (int)((double)(max_v - min_v) * ((double)i / 5.0));
        gchar buf[32];
        g_snprintf(buf, sizeof(buf), "%d", v);
        cairo_move_to(cr, 4.0, y + 3.0);
        cairo_show_text(cr, buf);
    }
}

static void chart_draw_time_axis(cairo_t *cr, double left, double top, double h, double w, guint start_min, guint end_min,
                                 double lr, double lg, double lb) {
    if (end_min <= start_min) return;
    cairo_set_source_rgb(cr, lr, lg, lb);
    cairo_set_font_size(cr, 10.0);
    const guint ticks = 6;
    for (guint i = 0; i <= ticks; i++) {
        double t = (double)i / (double)ticks;
        double x = left + (t * w);
        guint m = start_min + (guint)((double)(end_min - start_min) * t + 0.5);
        guint hh = (m / 60u) % 24u;
        guint mm = m % 60u;
        gchar buf[16];
        g_snprintf(buf, sizeof(buf), "%02u:%02u", hh, mm);
        cairo_move_to(cr, x - 14.0, top + h + 14.0);
        cairo_show_text(cr, buf);
    }
}

static void on_chart_motion(GtkEventControllerMotion *controller, double x, double y, gpointer user_data) {
    (void)controller;
    (void)y;
    chart_pointer_set_hover_x((AppState *)user_data, x, TRUE);
}

static void on_chart_leave(GtkEventControllerMotion *controller, gpointer user_data) {
    (void)controller;
    AppState *state = user_data;
    state->chart_hover_active = FALSE;
    if (state->recorded_chart != NULL) {
        gtk_widget_queue_draw(state->recorded_chart);
    }
}

static void on_chart_touch_pressed(GtkGestureClick *gesture, gint n_press, double x, double y, gpointer user_data) {
    (void)gesture;
    (void)n_press;
    (void)y;
    chart_pointer_set_hover_x((AppState *)user_data, x, FALSE);
}

static void on_chart_drag_begin(GtkGestureDrag *gesture, double start_x, double start_y, gpointer user_data) {
    (void)gesture;
    (void)start_y;
    AppState *state = user_data;
    state->chart_touch_drag_start_x = start_x;
    state->chart_touch_dragging = TRUE;
    chart_pointer_set_hover_x(state, start_x, FALSE);
}

static void on_chart_drag_update(GtkGestureDrag *gesture, double offset_x, double offset_y, gpointer user_data) {
    (void)gesture;
    (void)offset_y;
    AppState *state = user_data;
    chart_pointer_set_hover_x(state, state->chart_touch_drag_start_x + offset_x, FALSE);
}

static void on_chart_drag_end(GtkGestureDrag *gesture, double offset_x, double offset_y, gpointer user_data) {
    (void)gesture;
    (void)offset_x;
    (void)offset_y;
    ((AppState *)user_data)->chart_touch_dragging = FALSE;
}

static void draw_recorded_chart(GtkDrawingArea *area, cairo_t *cr, int width, int height, gpointer user_data) {
    (void)area;
    AppState *state = user_data;

    gboolean dark = adw_style_manager_get_dark(adw_style_manager_get_default());
    double bg0 = dark ? 0.11 : 0.94;
    double bg1 = dark ? 0.14 : 0.98;
    double border = dark ? 0.28 : 0.82;
    double grid_maj = dark ? 0.22 : 0.88;
    const double txt_r = dark ? 0.70 : 0.32;
    const double txt_g = dark ? 0.70 : 0.34;
    const double txt_b = dark ? 0.74 : 0.36;

    cairo_set_source_rgb(cr, bg0, bg0, bg0 + (dark ? 0.02 : -0.02));
    cairo_rectangle(cr, 0, 0, width, height);
    cairo_fill(cr);

    ChartPlotGeom geo;
    if (!chart_plot_geometry(width, height, &geo)) {
        return;
    }
    const double left = geo.left, top = geo.top, w = geo.w, h = geo.h;

    cairo_set_source_rgb(cr, bg1, bg1, bg1 + (dark ? 0.02 : -0.02));
    cairo_rectangle(cr, left, top, w, h);
    cairo_fill(cr);

    cairo_set_source_rgb(cr, border, border, border);
    cairo_set_line_width(cr, 1.0);
    cairo_rectangle(cr, left + 0.5, top + 0.5, w - 1.0, h - 1.0);
    cairo_stroke(cr);

    if (state->selected_recorded_day == NULL || state->recorded_days == NULL) {
        chart_submit_caption_literal(state, "Recorded history chart: no day selected.");
        return;
    }

    g_mutex_lock(&state->recorded_lock);
    GArray *arr = g_hash_table_lookup(state->recorded_days, state->selected_recorded_day);
    if (arr == NULL || arr->len == 0) {
        g_mutex_unlock(&state->recorded_lock);
        chart_submit_caption_literal(state, "Recorded history chart: no samples for selected day.");
        return;
    }

    if (state->selected_recorded_stat == REC_STAT_SLEEP) {
        guint16 first_m = 0, last_m = 0;
        if (!compute_sleep_window_from_array(arr, &first_m, &last_m)) {
            g_mutex_unlock(&state->recorded_lock);
            chart_submit_caption_literal(state, "Sleep: no sleep samples for selected day.");
            return;
        }

        const double sleep_span = (double)(last_m - first_m);
        for (guint i = 0; i < arr->len; i++) {
            if (i < first_m || i > last_m) continue;
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->sleep_stage < 0) continue;
            double x0 = left + (((double)(i - first_m)) / sleep_span) * w;
            double x1 = left + (((double)(i + 1 - first_m)) / sleep_span) * w;
            switch (s->sleep_stage) {
                case 4: cairo_set_source_rgba(cr, 0.90, 0.30, 0.80, 0.45); break; /* REM */
                case 2: cairo_set_source_rgba(cr, 0.25, 0.35, 0.95, 0.45); break; /* DEEP */
                case 3: cairo_set_source_rgba(cr, 0.35, 0.85, 0.55, 0.35); break; /* LIGHT */
                case 5: cairo_set_source_rgba(cr, 0.80, 0.80, 0.80, 0.20); break; /* AWAKE */
                default: cairo_set_source_rgba(cr, 0.80, 0.80, 0.80, 0.20); break; /* awake/other */
            }
            cairo_rectangle(cr, x0, top, x1 - x0 + 1.0, h);
            cairo_fill(cr);
        }
        int hr_min = 40, hr_max = 180;
        int spo2_min = 80, spo2_max = 100;
        chart_draw_y_labels(cr, left, top, h, hr_min, hr_max, txt_r, txt_g, txt_b);
        cairo_set_source_rgb(cr, txt_r, txt_g, txt_b);
        cairo_set_font_size(cr, 10.0);
        for (int i = 0; i <= 5; i++) {
            double y = top + (h * i / 5.0);
            int v = spo2_max - (int)((double)(spo2_max - spo2_min) * ((double)i / 5.0));
            gchar buf[32];
            g_snprintf(buf, sizeof(buf), "%d%%", v);
            cairo_move_to(cr, left + w + 4.0, y + 3.0);
            cairo_show_text(cr, buf);
        }

        cairo_set_source_rgb(cr, 0.95, 0.90, 0.30); /* HR: segments only between consecutive measured minutes */
        gboolean need_hr_move = TRUE;
        guint prev_hr_i = G_MAXUINT;
        guint hr_points = 0, spo2_points = 0;
        int hover_hr = -1, hover_spo2 = -1, hover_stage = -1;
        for (guint i = 0; i < arr->len; i++) {
            if (i < first_m || i > last_m) continue;
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->heart_rate > 0) {
                double x = left + (((double)(i - first_m)) / sleep_span) * w;
                double y = top + h - (((double)(s->heart_rate - hr_min)) / (double)(hr_max - hr_min) * h);
                if (need_hr_move) {
                    cairo_move_to(cr, x, y);
                    need_hr_move = FALSE;
                } else if (prev_hr_i != G_MAXUINT && i - prev_hr_i > 1u) {
                    cairo_stroke(cr);
                    cairo_set_source_rgb(cr, 0.95, 0.90, 0.30);
                    cairo_move_to(cr, x, y);
                } else {
                    cairo_line_to(cr, x, y);
                }
                prev_hr_i = i;
                hr_points++;
            }
        }
        cairo_stroke(cr);

        cairo_set_source_rgb(cr, 0.30, 0.95, 0.95); /* SpO2 */
        gboolean need_sp_move = TRUE;
        guint prev_sp_i = G_MAXUINT;
        for (guint i = 0; i < arr->len; i++) {
            if (i < first_m || i > last_m) continue;
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->spo2 > 0) {
                double x = left + (((double)(i - first_m)) / sleep_span) * w;
                double y = top + h - (((double)(s->spo2 - spo2_min)) / (double)(spo2_max - spo2_min) * h);
                if (need_sp_move) {
                    cairo_move_to(cr, x, y);
                    need_sp_move = FALSE;
                } else if (prev_sp_i != G_MAXUINT && i - prev_sp_i > 1u) {
                    cairo_stroke(cr);
                    cairo_set_source_rgb(cr, 0.30, 0.95, 0.95);
                    cairo_move_to(cr, x, y);
                } else {
                    cairo_line_to(cr, x, y);
                }
                prev_sp_i = i;
                spo2_points++;
            }
        }
        cairo_stroke(cr);

        guint16 hover_m = state->chart_hover_minute;
        if (state->chart_hover_active) {
            if (hover_m < first_m) hover_m = (guint16)first_m;
            if (hover_m > last_m) hover_m = (guint16)last_m;
            (void)recorded_value_exact(arr, REC_STAT_HEART_RATE, hover_m, &hover_hr);
            (void)recorded_value_exact(arr, REC_STAT_SPO2, hover_m, &hover_spo2);
            if (hover_m < arr->len) {
                hover_stage = g_array_index(arr, RecordedSample, hover_m).sleep_stage;
            }
        }

        if (state->chart_hover_active) {
            double xh = left + (((double)(hover_m - first_m)) / sleep_span) * w;
            if (dark) {
                cairo_set_source_rgba(cr, 1.0, 1.0, 1.0, 0.42);
            } else {
                cairo_set_source_rgba(cr, 0.10, 0.42, 0.92, 0.65);
            }
            cairo_set_line_width(cr, 1.0);
            cairo_move_to(cr, xh + 0.5, top);
            cairo_line_to(cr, xh + 0.5, top + h);
            cairo_stroke(cr);
        }
        chart_draw_time_axis(cr, left, top, h, w, first_m, last_m, txt_r, txt_g, txt_b);
        g_mutex_unlock(&state->recorded_lock);

        const char *stage_txt = "n/a";
        if (hover_stage == 4) stage_txt = "REM";
        else if (hover_stage == 2) stage_txt = "Deep";
        else if (hover_stage == 3) stage_txt = "Light";
        else if (hover_stage == 5) stage_txt = "Awake";

        gchar *lbl = NULL;
        if (state->chart_hover_active) {
            gchar hr_buf[32], sp_buf[32];
            if (hover_hr >= 0) {
                g_snprintf(hr_buf, sizeof(hr_buf), "%d bpm", hover_hr);
            } else {
                g_strlcpy(hr_buf, "n/a", sizeof(hr_buf));
            }
            if (hover_spo2 >= 0) {
                g_snprintf(sp_buf, sizeof(sp_buf), "%d%%", hover_spo2);
            } else {
                g_strlcpy(sp_buf, "n/a", sizeof(sp_buf));
            }
            lbl = g_strdup_printf("Sleep: %s (HR: %s, SpO2: %s) @ %02u:%02u",
                                  stage_txt,
                                  hr_buf,
                                  sp_buf,
                                  (unsigned)(hover_m / 60u),
                                  (unsigned)(hover_m % 60u));
        } else {
            lbl = g_strdup_printf("Sleep: %02u:%02u-%02u:%02u (HR: %u, SpO2: %u)",
                                  (unsigned)(first_m / 60u), (unsigned)(first_m % 60u),
                                  (unsigned)(last_m / 60u), (unsigned)(last_m % 60u),
                                  hr_points, spo2_points);
        }
        chart_submit_caption(state, lbl);
        return;
    }

    if (state->selected_recorded_stat == REC_STAT_STEPS) {
        int cum_steps = 0, cum_dist_cm = 0;
        int max_steps = 0, max_dist_cm = 0;
        guint last_idx = 0;
        for (guint i = 0; i < arr->len; i++) {
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->steps > 0) cum_steps += s->steps;
            if (s->distance_cm >= 0) cum_dist_cm += s->distance_cm;
            if (cum_steps > max_steps) max_steps = cum_steps;
            if (cum_dist_cm > max_dist_cm) max_dist_cm = cum_dist_cm;
            if (s->steps >= 0 || s->distance_cm >= 0) last_idx = i;
        }
        if (max_steps <= 0 && max_dist_cm <= 0) {
            guint summary_steps = 0;
            gboolean has_summary_steps = FALSE;
            if (state->daily_summary_steps != NULL && state->selected_recorded_day != NULL) {
                guint *p = g_hash_table_lookup(state->daily_summary_steps, state->selected_recorded_day);
                if (p != NULL && *p > 0) {
                    summary_steps = *p;
                    has_summary_steps = TRUE;
                }
            }

            if (!has_summary_steps) {
                g_mutex_unlock(&state->recorded_lock);
                chart_submit_caption_literal(state, "Steps + Distance: no values");
                return;
            }

            g_mutex_unlock(&state->recorded_lock);
            gchar *lbl = g_strdup_printf(
                "Steps summary only: %u (minute step history not decoded/received, flat fallback removed)",
                summary_steps
            );
            chart_submit_caption(state, lbl);
            return;
        }
        if (max_steps <= 0) max_steps = 1;
        int max_dist_m = (max_dist_cm + 99) / 100;
        if (max_dist_m < 1) max_dist_m = 1;
        int y_steps_max = (max_steps * 110 + 99) / 100;
        int y_dist_max_m = (max_dist_m * 110 + 99) / 100;
        if (y_steps_max < 1) y_steps_max = 1;
        if (y_dist_max_m < 1) y_dist_max_m = 1;
        chart_draw_y_labels(cr, left, top, h, 0, y_steps_max, txt_r, txt_g, txt_b);
        cairo_set_source_rgb(cr, txt_r, txt_g, txt_b);
        cairo_set_font_size(cr, 10.0);
        for (int i = 0; i <= 5; i++) {
            double y = top + (h * i / 5.0);
            int v = y_dist_max_m - (int)((double)y_dist_max_m * ((double)i / 5.0));
            gchar buf[32];
            g_snprintf(buf, sizeof(buf), "%d", v);
            cairo_move_to(cr, left + w + 4.0, y + 3.0);
            cairo_show_text(cr, buf);
        }

        int hover_steps = 0, hover_dist_m = 0;
        cairo_set_source_rgb(cr, 0.25, 0.70, 0.95); /* steps */
        gboolean first = TRUE;
        cum_steps = 0; cum_dist_cm = 0;
        for (guint i = 0; i <= last_idx && i < arr->len; i++) {
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->steps > 0) cum_steps += s->steps;
            if (s->distance_cm >= 0) cum_dist_cm += s->distance_cm;
            if (state->chart_hover_active && i == state->chart_hover_minute) {
                hover_steps = cum_steps;
                hover_dist_m = (cum_dist_cm + 50) / 100;
            }
            double x = left + ((double)i / 1439.0) * w;
            double y = top + h - (((double)cum_steps / (double)y_steps_max) * h);
            if (first) { cairo_move_to(cr, x, y); first = FALSE; } else { cairo_line_to(cr, x, y); }
        }
        cairo_stroke(cr);

        cairo_set_source_rgb(cr, 0.95, 0.85, 0.30); /* cumulative distance (m), from GB distanceCm */
        first = TRUE;
        cum_steps = 0; cum_dist_cm = 0;
        for (guint i = 0; i <= last_idx && i < arr->len; i++) {
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            if (s->steps > 0) cum_steps += s->steps;
            if (s->distance_cm >= 0) cum_dist_cm += s->distance_cm;
            double x = left + ((double)i / 1439.0) * w;
            const double cum_m = (double)cum_dist_cm / 100.0;
            double y = top + h - ((cum_m / (double)y_dist_max_m) * h);
            if (first) { cairo_move_to(cr, x, y); first = FALSE; } else { cairo_line_to(cr, x, y); }
        }
        cairo_stroke(cr);

        if (state->chart_hover_active) {
            double xh = left + ((double)state->chart_hover_minute / 1439.0) * w;
            if (dark) {
                cairo_set_source_rgba(cr, 1.0, 1.0, 1.0, 0.42);
            } else {
                cairo_set_source_rgba(cr, 0.10, 0.42, 0.92, 0.65);
            }
            cairo_set_line_width(cr, 1.0);
            cairo_move_to(cr, xh + 0.5, top);
            cairo_line_to(cr, xh + 0.5, top + h);
            cairo_stroke(cr);
        }
        chart_draw_time_axis(cr, left, top, h, w, 0, 1440, txt_r, txt_g, txt_b);
        g_mutex_unlock(&state->recorded_lock);
        gchar *lbl = NULL;
        if (state->chart_hover_active) {
            lbl = g_strdup_printf("Steps: %d (Distance: %d m) @ %02u:%02u",
                                  hover_steps, hover_dist_m,
                                  (unsigned)(state->chart_hover_minute / 60u),
                                  (unsigned)(state->chart_hover_minute % 60u));
        } else {
            lbl = g_strdup_printf("Steps: %d (Distance: %d m)", max_steps, (max_dist_cm + 50) / 100);
        }
        chart_submit_caption(state, lbl);
        return;
    }

    int min_v = G_MAXINT;
    int max_v = G_MININT;
    guint points = 0;
    int hover_v = -1;
    guint last_idx = 0;
    for (guint i = 0; i < arr->len; i++) {
        const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
        const int v = recorded_sample_value(s, state->selected_recorded_stat);
        if (v >= 0) {
            if (v < min_v) min_v = v;
            if (v > max_v) max_v = v;
            points++;
            last_idx = i;
        }
    }

    if (points == 0) {
        g_mutex_unlock(&state->recorded_lock);
        gchar *lbl = g_strdup_printf("%s %s: no values",
                                     state->selected_recorded_day,
                                     recorded_stat_name(state->selected_recorded_stat));
        chart_submit_caption(state, lbl);
        return;
    }
    if (min_v == max_v) max_v = min_v + 1;
    int y_max = (max_v * 110 + 99) / 100;
    if (y_max <= min_v) y_max = max_v + 1;

    cairo_set_source_rgb(cr, grid_maj, grid_maj, grid_maj);
    for (int i = 1; i < 6; i++) {
        double y = top + (h * i / 6.0);
        cairo_move_to(cr, left, y);
        cairo_line_to(cr, left + w, y);
    }
    cairo_stroke(cr);
    chart_draw_y_labels(cr, left, top, h, min_v, y_max, txt_r, txt_g, txt_b);

    cairo_set_source_rgb(cr, 0.25, 0.70, 0.95);
    gboolean smooth = (state->selected_recorded_stat == REC_STAT_SPO2 || state->selected_recorded_stat == REC_STAT_STRESS);
    if (smooth) {
        const double bar_w = 2.0;
        for (guint i = 0; i <= last_idx && i < arr->len; i++) {
            int raw = recorded_sample_value(&g_array_index(arr, RecordedSample, i), state->selected_recorded_stat);
            if (raw < 0) continue;
            int v = raw;
            double x = left + ((double)i / 1439.0) * w;
            double y = top + h - ((((double)(v - min_v)) / (double)(y_max - min_v)) * h);
            cairo_rectangle(cr, x - (bar_w / 2.0), y, bar_w, (top + h) - y);
            cairo_fill(cr);
        }
    } else {
        /* Line charts: break path across gaps so we do not imply values between sparse samples. */
        guint line_pts = 0;
        double lone_x = 0.0, lone_y = 0.0;
        gboolean need_move = TRUE;
        guint prev_plot_i = G_MAXUINT;
        for (guint i = 0; i <= last_idx && i < arr->len; i++) {
            const RecordedSample *s = &g_array_index(arr, RecordedSample, i);
            const int v = recorded_sample_value(s, state->selected_recorded_stat);
            if (v < 0) continue;
            double x = left + ((double)i / 1439.0) * w;
            double y = top + h - ((((double)(v - min_v)) / (double)(y_max - min_v)) * h);
            line_pts++;
            lone_x = x;
            lone_y = y;
            if (need_move) {
                cairo_move_to(cr, x, y);
                need_move = FALSE;
            } else if (prev_plot_i != G_MAXUINT && i - prev_plot_i > 1u) {
                cairo_stroke(cr);
                cairo_set_source_rgb(cr, 0.25, 0.70, 0.95);
                cairo_move_to(cr, x, y);
            } else {
                cairo_line_to(cr, x, y);
            }
            prev_plot_i = i;
        }
        cairo_stroke(cr);
        if (line_pts == 1) {
            cairo_set_source_rgb(cr, 0.25, 0.70, 0.95);
            cairo_arc(cr, lone_x, lone_y, 4.0, 0.0, 2.0 * M_PI);
            cairo_fill(cr);
        }
    }

    if (state->chart_hover_active) {
        double xh = left + ((double)state->chart_hover_minute / 1439.0) * w;
        if (dark) {
            cairo_set_source_rgba(cr, 1.0, 1.0, 1.0, 0.42);
        } else {
            cairo_set_source_rgba(cr, 0.10, 0.42, 0.92, 0.65);
        }
        cairo_set_line_width(cr, 1.0);
        cairo_move_to(cr, xh + 0.5, top);
        cairo_line_to(cr, xh + 0.5, top + h);
        cairo_stroke(cr);
        (void)recorded_value_exact(arr, state->selected_recorded_stat, state->chart_hover_minute, &hover_v);
    }
    chart_draw_time_axis(cr, left, top, h, w, 0, 1440, txt_r, txt_g, txt_b);
    g_mutex_unlock(&state->recorded_lock);

    gchar *lbl = NULL;
    if (state->chart_hover_active) {
        if (hover_v >= 0) {
            lbl = g_strdup_printf("%s: %d @ %02u:%02u",
                                  recorded_stat_name(state->selected_recorded_stat),
                                  hover_v,
                                  (unsigned)(state->chart_hover_minute / 60u),
                                  (unsigned)(state->chart_hover_minute % 60u));
        } else {
            lbl = g_strdup_printf("%s: n/a @ %02u:%02u",
                                  recorded_stat_name(state->selected_recorded_stat),
                                  (unsigned)(state->chart_hover_minute / 60u),
                                  (unsigned)(state->chart_hover_minute % 60u));
        }
    } else {
        lbl = g_strdup_printf("%s: %u points (min: %d, max: %d)",
                              recorded_stat_name(state->selected_recorded_stat),
                              points, min_v, y_max);
    }
    chart_submit_caption(state, lbl);
}
/* Min-width 0 for flex children; chart frame + caption tuned for light/dark. */
static void mib10_install_tight_layout_css(GdkDisplay *display) {
    static gboolean provider_installed;
    if (provider_installed) {
        return;
    }
    GtkCssProvider *css = gtk_css_provider_new();
    gtk_css_provider_load_from_string(css,
                                      ".mib10-shrink { min-width: 0; }\n"
                                      ".mib10-chart-day-combo { min-width: 0; }\n"
                                      ".mib10-chart-stat-combo { min-width: 0; }\n"
                                      ".mib10-chart-frame { padding: 0; }\n"
                                      ".mib10-chart-frame > border { border-radius: 10px; }\n"
                                      ".mib10-caption { font-size: 0.9rem; opacity: 0.9; "
                                      "min-height: 2.75em; margin-top: 6px; }\n"
                                      ".mib10-dashboard > * { margin-left: 2px; margin-right: 2px; }\n"
                                      ".mib10-status { font-weight: 500; font-size: 0.95rem; "
                                      "padding: 6px 4px; }\n");
    gtk_style_context_add_provider_for_display(display, GTK_STYLE_PROVIDER(css),
                                               GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);
    g_object_unref(css);
    provider_installed = TRUE;
}

static void mib10_allow_width_shrink(GtkWidget *w) {
    if (w == NULL) {
        return;
    }
    gtk_widget_add_css_class(w, "mib10-shrink");
}

static void mib10_apply_chart_combo_classes(GtkWidget *day_combo, GtkWidget *stat_combo) {
    gtk_widget_add_css_class(day_combo, "mib10-chart-day-combo");
    gtk_widget_add_css_class(stat_combo, "mib10-chart-stat-combo");
}

static GtkWidget *mib10_create_compact_gauge(const char *title_text, const char *value_initial,
                                             GtkWidget **value_label_out, GtkWidget **bar_out) {
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);
    gtk_widget_set_hexpand(box, TRUE);

    GtkWidget *head = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    GtkWidget *title = gtk_label_new(title_text);
    gtk_label_set_xalign(GTK_LABEL(title), 0.0f);
    GtkWidget *value = gtk_label_new(value_initial);
    gtk_label_set_xalign(GTK_LABEL(value), 1.0f);
    gtk_widget_set_hexpand(value, TRUE);
    gtk_box_append(GTK_BOX(head), title);
    gtk_box_append(GTK_BOX(head), value);

    GtkWidget *bar = gtk_progress_bar_new();
    gtk_progress_bar_set_show_text(GTK_PROGRESS_BAR(bar), FALSE);
    gtk_widget_set_hexpand(bar, TRUE);

    gtk_box_append(GTK_BOX(box), head);
    gtk_box_append(GTK_BOX(box), bar);

    *value_label_out = value;
    *bar_out = bar;
    return box;
}

void mib10_activate(GApplication *application, gpointer user_data) {
    AppState *state = user_data;
    if (!gb_db_open()) {
        g_warning("Gadgetbridge-compatible database could not be opened; samples will not be persisted.");
    }
    state->window = adw_application_window_new(GTK_APPLICATION(application));
    gtk_window_set_title(GTK_WINDOW(state->window), "Mi Band 10 SPP Viewer");
    gtk_window_set_default_size(GTK_WINDOW(state->window), 720, 560);
    gtk_widget_add_css_class(state->window, "mib10-app");
    mib10_install_tight_layout_css(gtk_widget_get_display(state->window));

    GtkWidget *toolbar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    gtk_widget_set_margin_top(toolbar, 12);
    gtk_widget_set_margin_bottom(toolbar, 12);
    gtk_widget_set_margin_start(toolbar, 12);
    gtk_widget_set_margin_end(toolbar, 12);

    state->btn_refresh = gtk_button_new_with_label("Refresh");
    g_signal_connect(state->btn_refresh, "clicked", G_CALLBACK(on_refresh_clicked), state);

    gtk_box_append(GTK_BOX(toolbar), state->btn_refresh);

    state->label_status = gtk_label_new("Idle");
    gtk_label_set_xalign(GTK_LABEL(state->label_status), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(state->label_status), TRUE);
    gtk_label_set_wrap_mode(GTK_LABEL(state->label_status), PANGO_WRAP_WORD_CHAR);
    gtk_widget_set_margin_start(state->label_status, 14);
    gtk_widget_set_margin_end(state->label_status, 14);
    gtk_widget_set_margin_top(state->label_status, 4);
    gtk_widget_set_margin_bottom(state->label_status, 6);
    gtk_widget_add_css_class(state->label_status, "mib10-status");
    mib10_allow_width_shrink(state->label_status);

    GtkWidget *top_row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 14);
    state->stats_row = top_row;
    gtk_widget_set_margin_start(top_row, 10);
    gtk_widget_set_margin_end(top_row, 10);
    gtk_widget_set_margin_bottom(top_row, 8);
    mib10_allow_width_shrink(top_row);

    GtkWidget *heart_card = gtk_frame_new(NULL);
    mib10_allow_width_shrink(heart_card);
    GtkWidget *heart_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_top(heart_box, 10);
    gtk_widget_set_margin_bottom(heart_box, 10);
    gtk_widget_set_margin_start(heart_box, 10);
    gtk_widget_set_margin_end(heart_box, 10);
    GtkWidget *heart_gauge = mib10_create_compact_gauge("Heart rate", "-- bpm", &state->hr_label, &state->hr_bar);
    gtk_box_append(GTK_BOX(heart_box), heart_gauge);
    gtk_frame_set_child(GTK_FRAME(heart_card), heart_box);

    GtkWidget *gauge_card = gtk_frame_new(NULL);
    mib10_allow_width_shrink(gauge_card);
    GtkWidget *gauge_grid = gtk_grid_new();
    gtk_grid_set_row_spacing(GTK_GRID(gauge_grid), 6);
    gtk_grid_set_column_spacing(GTK_GRID(gauge_grid), 10);
    gtk_widget_set_margin_top(gauge_grid, 10);
    gtk_widget_set_margin_bottom(gauge_grid, 10);
    gtk_widget_set_margin_start(gauge_grid, 10);
    gtk_widget_set_margin_end(gauge_grid, 10);
    GtkWidget *steps_gauge = mib10_create_compact_gauge("Steps", "--", &state->steps_label, &state->steps_bar);
    GtkWidget *calories_gauge = mib10_create_compact_gauge("Calories", "-- kcal", &state->calories_label, &state->calories_bar);
    GtkWidget *battery_gauge = mib10_create_compact_gauge("Battery", "--%", &state->battery_label, &state->battery_bar);
    GtkWidget *moving_gauge = mib10_create_compact_gauge("Moving", "--", &state->moving_label, &state->moving_bar);
    gtk_grid_attach(GTK_GRID(gauge_grid), steps_gauge, 0, 0, 1, 1);
    gtk_grid_attach(GTK_GRID(gauge_grid), calories_gauge, 0, 1, 1, 1);
    gtk_grid_attach(GTK_GRID(gauge_grid), battery_gauge, 0, 2, 1, 1);
    gtk_grid_attach(GTK_GRID(gauge_grid), moving_gauge, 0, 3, 1, 1);
    gtk_frame_set_child(GTK_FRAME(gauge_card), gauge_grid);

    state->standing_label = NULL;
    state->standing_bar = NULL;
    state->spo2_label = NULL;
    state->stress_label = NULL;
    state->charging_label = NULL;
    state->wearing_label = NULL;
    state->sleep_label = NULL;

    gtk_widget_set_hexpand(heart_card, FALSE);
    gtk_widget_set_hexpand(gauge_card, TRUE);
    gtk_box_append(GTK_BOX(top_row), heart_card);
    gtk_box_append(GTK_BOX(top_row), gauge_card);

    GtkWidget *recorded_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_start(recorded_box, 8);
    gtk_widget_set_margin_end(recorded_box, 8);
    gtk_widget_set_margin_bottom(recorded_box, 4);
    mib10_allow_width_shrink(recorded_box);

    GtkWidget *recorded_controls = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);
    gtk_widget_set_hexpand(recorded_controls, TRUE);
    mib10_allow_width_shrink(recorded_controls);
    GtkStringList *day_strings = gtk_string_list_new(NULL);
    state->recorded_day_dropdown = gtk_drop_down_new(G_LIST_MODEL(day_strings), NULL);
    g_object_unref(day_strings);

    const char *stat_labels[] = {
        "Heart rate", "Steps + Distance", "Active calories", "SpO2", "Stress", "Energy", "Sleep", NULL,
    };
    GtkStringList *stat_strings = gtk_string_list_new(NULL);
    for (const char **p = stat_labels; *p; p++) {
        gtk_string_list_append(stat_strings, *p);
    }
    state->recorded_stat_dropdown = gtk_drop_down_new(G_LIST_MODEL(stat_strings), NULL);
    g_object_unref(stat_strings);

    gtk_widget_set_hexpand(state->recorded_day_dropdown, TRUE);
    gtk_widget_set_hexpand(state->recorded_stat_dropdown, TRUE);
    mib10_apply_chart_combo_classes(state->recorded_day_dropdown, state->recorded_stat_dropdown);
    gtk_drop_down_set_selected(GTK_DROP_DOWN(state->recorded_stat_dropdown), (guint)REC_STAT_HEART_RATE);
    g_signal_connect(state->recorded_day_dropdown, "notify::selected-item", G_CALLBACK(on_recorded_day_selected), state);
    g_signal_connect(state->recorded_stat_dropdown, "notify::selected-item", G_CALLBACK(on_recorded_stat_selected), state);

    gtk_box_append(GTK_BOX(recorded_controls), state->recorded_day_dropdown);
    gtk_box_append(GTK_BOX(recorded_controls), state->recorded_stat_dropdown);

    state->recorded_chart = gtk_drawing_area_new();
    gtk_widget_set_size_request(state->recorded_chart, -1, 180);
    mib10_allow_width_shrink(state->recorded_chart);
    gtk_widget_set_can_focus(state->recorded_chart, FALSE);
    gtk_drawing_area_set_draw_func(GTK_DRAWING_AREA(state->recorded_chart), draw_recorded_chart, state, NULL);
    GtkEventController *motion = gtk_event_controller_motion_new();
    g_signal_connect(motion, "motion", G_CALLBACK(on_chart_motion), state);
    g_signal_connect(motion, "leave", G_CALLBACK(on_chart_leave), state);
    gtk_widget_add_controller(state->recorded_chart, motion);
    GtkGesture *tap = gtk_gesture_click_new();
    gtk_gesture_single_set_touch_only(GTK_GESTURE_SINGLE(tap), TRUE);
    g_signal_connect(tap, "pressed", G_CALLBACK(on_chart_touch_pressed), state);
    gtk_widget_add_controller(state->recorded_chart, GTK_EVENT_CONTROLLER(tap));
    GtkGesture *drag = gtk_gesture_drag_new();
    gtk_gesture_single_set_touch_only(GTK_GESTURE_SINGLE(drag), TRUE);
    g_signal_connect(drag, "drag-begin", G_CALLBACK(on_chart_drag_begin), state);
    g_signal_connect(drag, "drag-update", G_CALLBACK(on_chart_drag_update), state);
    g_signal_connect(drag, "drag-end", G_CALLBACK(on_chart_drag_end), state);
    gtk_widget_add_controller(state->recorded_chart, GTK_EVENT_CONTROLLER(drag));

    GtkWidget *chart_frame = gtk_frame_new(NULL);
    gtk_widget_add_css_class(chart_frame, "mib10-chart-frame");
    mib10_allow_width_shrink(chart_frame);
    gtk_frame_set_child(GTK_FRAME(chart_frame), state->recorded_chart);

    state->recorded_chart_label = gtk_label_new("Recorded history chart: no data yet.");
    gtk_label_set_xalign(GTK_LABEL(state->recorded_chart_label), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(state->recorded_chart_label), TRUE);
    gtk_label_set_wrap_mode(GTK_LABEL(state->recorded_chart_label), PANGO_WRAP_WORD_CHAR);
    gtk_label_set_lines(GTK_LABEL(state->recorded_chart_label), 3);
    gtk_widget_set_hexpand(state->recorded_chart_label, TRUE);
    gtk_widget_add_css_class(state->recorded_chart_label, "mib10-caption");
    mib10_allow_width_shrink(state->recorded_chart_label);

    gtk_box_append(GTK_BOX(recorded_box), recorded_controls);
    gtk_box_append(GTK_BOX(recorded_box), chart_frame);
    gtk_box_append(GTK_BOX(recorded_box), state->recorded_chart_label);

    GtkWidget *text_view = gtk_text_view_new();
    gtk_text_view_set_editable(GTK_TEXT_VIEW(text_view), FALSE);
    gtk_text_view_set_monospace(GTK_TEXT_VIEW(text_view), TRUE);
    state->log_buffer = gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view));

    GtkWidget *scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(scroll), text_view);
    gtk_widget_set_margin_start(scroll, 12);
    gtk_widget_set_margin_end(scroll, 12);
    gtk_widget_set_margin_bottom(scroll, 12);
    gtk_widget_set_vexpand(scroll, TRUE);
    mib10_allow_width_shrink(scroll);
    gtk_scrolled_window_set_min_content_width(GTK_SCROLLED_WINDOW(scroll), 0);

    state->listbox_devices = gtk_list_box_new();
    g_signal_connect(state->listbox_devices, "row-selected", G_CALLBACK(on_device_row_selected), state);
    g_signal_connect(state->listbox_devices, "row-activated", G_CALLBACK(on_device_row_activated), state);
    gtk_list_box_set_activate_on_single_click(GTK_LIST_BOX(state->listbox_devices), TRUE);

    GtkWidget *device_scroll = gtk_scrolled_window_new();
    gtk_scrolled_window_set_child(GTK_SCROLLED_WINDOW(device_scroll), state->listbox_devices);
    gtk_widget_set_margin_start(device_scroll, 12);
    gtk_widget_set_margin_end(device_scroll, 12);
    gtk_widget_set_margin_bottom(device_scroll, 12);
    gtk_widget_set_vexpand(device_scroll, TRUE);
    mib10_allow_width_shrink(device_scroll);
    gtk_scrolled_window_set_min_content_width(GTK_SCROLLED_WINDOW(device_scroll), 0);

    GtkWidget *pairing_page = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_top(pairing_page, 12);
    gtk_widget_set_margin_bottom(pairing_page, 12);
    gtk_widget_set_margin_start(pairing_page, 12);
    gtk_widget_set_margin_end(pairing_page, 12);

    GtkWidget *auth_row = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    GtkWidget *auth_label = gtk_label_new("Auth key:");
    gtk_label_set_xalign(GTK_LABEL(auth_label), 0.0f);
    state->entry_authkey = gtk_entry_new();
    gtk_widget_set_hexpand(state->entry_authkey, TRUE);
    gtk_editable_set_text(GTK_EDITABLE(state->entry_authkey), state->auth_key_hex);
    gtk_entry_set_placeholder_text(GTK_ENTRY(state->entry_authkey), "32 hex chars");
    gtk_box_append(GTK_BOX(auth_row), auth_label);
    gtk_box_append(GTK_BOX(auth_row), state->entry_authkey);

    GtkWidget *pairing_hint = gtk_label_new("Click a device to pair/trust if needed, then connect.");
    gtk_label_set_xalign(GTK_LABEL(pairing_hint), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(pairing_hint), TRUE);

    gtk_box_append(GTK_BOX(pairing_page), auth_row);
    gtk_box_append(GTK_BOX(pairing_page), toolbar);
    gtk_box_append(GTK_BOX(pairing_page), pairing_hint);
    gtk_box_append(GTK_BOX(pairing_page), device_scroll);

    GtkWidget *dashboard_page = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_widget_add_css_class(dashboard_page, "mib10-dashboard");
    mib10_allow_width_shrink(dashboard_page);
    gtk_widget_set_hexpand(top_row, TRUE);
    gtk_widget_set_vexpand(top_row, FALSE);
    gtk_widget_set_hexpand(recorded_box, TRUE);
    gtk_widget_set_vexpand(recorded_box, FALSE);

    gtk_box_append(GTK_BOX(dashboard_page), state->label_status);
    gtk_box_append(GTK_BOX(dashboard_page), top_row);
    gtk_box_append(GTK_BOX(dashboard_page), recorded_box);
    gtk_box_append(GTK_BOX(dashboard_page), scroll);

    state->stack = gtk_stack_new();
    mib10_allow_width_shrink(state->stack);
    state->page_pairing = pairing_page;
    state->page_dashboard = dashboard_page;
    mib10_allow_width_shrink(pairing_page);
    gtk_stack_add_named(GTK_STACK(state->stack), pairing_page, "pairing");
    gtk_stack_add_named(GTK_STACK(state->stack), dashboard_page, "dashboard");
    gtk_stack_set_visible_child(GTK_STACK(state->stack), pairing_page);
    gtk_widget_add_tick_callback(state->window, on_window_tick, state, NULL);
    update_responsive_layout(state);

    adw_application_window_set_content(ADW_APPLICATION_WINDOW(state->window), state->stack);
    gtk_window_present(GTK_WINDOW(state->window));

    populate_device_list(state);
}
