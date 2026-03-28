#include "mib10_app.h"

#include <gio/gio.h>
static gchar *find_bluez_device_path_by_address(GDBusConnection *system_bus, const gchar *address) {
    enum { IFACE_OBJECT_MANAGER = 1 };
    (void)IFACE_OBJECT_MANAGER;

    GError *error = NULL;
    GVariant *reply = g_dbus_connection_call_sync(
        system_bus,
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
        return NULL;
    }

    gchar *found_path = NULL;
    GVariant *objects = g_variant_get_child_value(reply, 0);
    GVariantIter iter;
    g_variant_iter_init(&iter, objects);
    GVariant *entry = NULL;

    while ((entry = g_variant_iter_next_value(&iter)) != NULL && found_path == NULL) {
        gchar *path = NULL;
        GVariant *ifaces = NULL;
        g_variant_get(entry, "{o@a{sa{sv}}}", &path, &ifaces);

        if (g_str_has_prefix(path, "/")) {
            GVariant *device_props = g_variant_lookup_value(ifaces, "org.bluez.Device1", G_VARIANT_TYPE("a{sv}"));
            if (device_props != NULL) {
                GVariant *address_var = g_variant_lookup_value(device_props, "Address", G_VARIANT_TYPE_STRING);
                if (address_var != NULL) {
                    const gchar *current = g_variant_get_string(address_var, NULL);
                    if (g_strcmp0(current, address) == 0) {
                        found_path = g_strdup(path);
                    }
                    g_variant_unref(address_var);
                }
                g_variant_unref(device_props);
            }
        }

        g_variant_unref(ifaces);
        g_free(path);
        g_variant_unref(entry);
    }

    g_variant_unref(objects);
    g_variant_unref(reply);
    return found_path;
}

static gboolean bluez_set_trusted(AppState *state, GDBusConnection *bus, const gchar *device_path, gboolean trusted) {
    GError *error = NULL;
    GVariant *reply = g_dbus_connection_call_sync(
        bus,
        "org.bluez",
        device_path,
        "org.freedesktop.DBus.Properties",
        "Set",
        g_variant_new("(ssv)", "org.bluez.Device1", "Trusted", g_variant_new_boolean(trusted)),
        NULL,
        G_DBUS_CALL_FLAGS_NONE,
        -1,
        NULL,
        &error
    );

    if (reply == NULL) {
        gchar *line = g_strdup_printf("BlueZ set Trusted=%s failed: %s", trusted ? "true" : "false", error ? error->message : "unknown");
        append_log(state, line);
        g_free(line);
        if (error) {
            g_error_free(error);
        }
        return FALSE;
    }

    g_variant_unref(reply);
    return TRUE;
}

static gboolean bluez_call_device_method(AppState *state,
                                         GDBusConnection *bus,
                                         const gchar *device_path,
                                         const gchar *method,
                                         gboolean treat_already_done_as_success) {
    GError *error = NULL;
    GVariant *reply = g_dbus_connection_call_sync(
        bus,
        "org.bluez",
        device_path,
        "org.bluez.Device1",
        method,
        NULL,
        NULL,
        G_DBUS_CALL_FLAGS_NONE,
        -1,
        NULL,
        &error
    );

    if (reply == NULL) {
        const gchar *msg = (error && error->message) ? error->message : "unknown";
        if (treat_already_done_as_success &&
            (g_strrstr(msg, "AlreadyExists") || g_strrstr(msg, "Already Paired") || g_strrstr(msg, "Already connected"))) {
            if (error) {
                g_error_free(error);
            }
            return TRUE;
        }

        gchar *line = g_strdup_printf("BlueZ %s failed: %s", method, msg);
        append_log(state, line);
        g_free(line);
        if (error) {
            g_error_free(error);
        }
        return FALSE;
    }

    g_variant_unref(reply);
    return TRUE;
}

static gboolean bluez_get_device_state(GDBusConnection *bus,
                                       const gchar *device_path,
                                       gboolean *paired,
                                       gboolean *trusted,
                                       gboolean *connected) {
    if (paired) *paired = FALSE;
    if (trusted) *trusted = FALSE;
    if (connected) *connected = FALSE;

    GError *error = NULL;
    GVariant *reply = g_dbus_connection_call_sync(
        bus,
        "org.bluez",
        device_path,
        "org.freedesktop.DBus.Properties",
        "GetAll",
        g_variant_new("(s)", "org.bluez.Device1"),
        G_VARIANT_TYPE("(a{sv})"),
        G_DBUS_CALL_FLAGS_NONE,
        -1,
        NULL,
        &error
    );
    if (reply == NULL) {
        if (error) g_error_free(error);
        return FALSE;
    }

    GVariant *props = g_variant_get_child_value(reply, 0);
    GVariantDict dict;
    g_variant_dict_init(&dict, props);
    if (paired) (void)g_variant_dict_lookup(&dict, "Paired", "b", paired);
    if (trusted) (void)g_variant_dict_lookup(&dict, "Trusted", "b", trusted);
    if (connected) (void)g_variant_dict_lookup(&dict, "Connected", "b", connected);
    g_variant_unref(props);
    g_variant_unref(reply);
    return TRUE;
}

gboolean bluez_pair_trust_connect(AppState *state, const gchar *address) {
    GError *error = NULL;
    GDBusConnection *bus = g_bus_get_sync(G_BUS_TYPE_SYSTEM, NULL, &error);
    if (bus == NULL) {
        set_status(state, "BlueZ D-Bus error.");
        if (error) g_error_free(error);
        return FALSE;
    }

    gchar *device_path = find_bluez_device_path_by_address(bus, address);
    if (device_path == NULL) {
        append_log(state, "BlueZ device path not found for selected address.");
        g_object_unref(bus);
        return FALSE;
    }

    gboolean paired = FALSE, trusted = FALSE, connected = FALSE;
    (void)bluez_get_device_state(bus, device_path, &paired, &trusted, &connected);

    gboolean pair_ok = TRUE;
    gboolean trust_ok = TRUE;
    if (!paired) {
        append_log(state, "BlueZ: first-time setup -> Pair + Trust.");
        pair_ok = bluez_call_device_method(state, bus, device_path, "Pair", TRUE);
        trust_ok = bluez_set_trusted(state, bus, device_path, TRUE);
    } else if (!trusted) {
        append_log(state, "BlueZ: already paired, setting Trusted=true.");
        trust_ok = bluez_set_trusted(state, bus, device_path, TRUE);
    } else {
        append_log(state, "BlueZ: already paired/trusted, skipping Pair/Trust.");
    }

    const gboolean connect_ok = connected ? TRUE : bluez_call_device_method(state, bus, device_path, "Connect", TRUE);

    g_free(device_path);
    g_object_unref(bus);
    return pair_ok && trust_ok && connect_ok;
}
