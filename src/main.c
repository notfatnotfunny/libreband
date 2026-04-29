#include "mib10_app.h"

#include <stdlib.h>

#include <glib.h>
#include <gio/gio.h>
#include <gtk/gtk.h>
#include <adwaita.h>

int main(int argc, char **argv) {
    AppState state = {0};
    state.sock_fd = -1;
    state.selected_recorded_stat = REC_STAT_HEART_RATE;
    g_strlcpy(state.auth_key_hex, DEFAULT_AUTH_KEY_HEX, sizeof(state.auth_key_hex));
    state.verbose_wire_log = (g_getenv("MIBAND10_VERBOSE_WIRE_LOG") != NULL);
    state.verbose_fetch_log = (g_getenv("MIBAND10_FETCH_DEBUG") != NULL);
    g_mutex_init(&state.recorded_lock);
    g_mutex_init(&state.log_lock);

    state.app = adw_application_new("me.nf2x.libreband", G_APPLICATION_DEFAULT_FLAGS);
    g_signal_connect(state.app, "activate", G_CALLBACK(mib10_activate), &state);
    g_signal_connect(state.app, "shutdown", G_CALLBACK(mib10_app_shutdown), &state);

    const int status = g_application_run(G_APPLICATION(state.app), argc, argv);
    g_object_unref(state.app);
    g_mutex_clear(&state.recorded_lock);
    g_mutex_clear(&state.log_lock);
    return status;
}
