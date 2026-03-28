#include "mib10_app.h"

#include <bluetooth/bluetooth.h>
#include <bluetooth/rfcomm.h>
#include <bluetooth/sdp.h>
#include <bluetooth/sdp_lib.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>

#include <openssl/rand.h>

#include "xiaomi.pb-c.h"

static void log_activity_file_ids(const uint8_t *ids, size_t len, const char *label);
static void enqueue_activity_file_ids(AppState *state, const uint8_t *ids, size_t len);
static gboolean send_encrypted_simple_command(AppState *state, uint32_t type, uint32_t subtype, const char *label);
static void send_activity_fetch_past_channels(AppState *state, const char *label_prefix);
static void stop_activity_live_poll(AppState *state);
static void start_activity_live_poll(AppState *state);
static const uint32_t ACTIVITY_TODAY_CHANNEL_MAX = 7u;
static const guint ACTIVITY_AUTO_REFETCH_ROUNDS = 4u;
static const guint ACTIVITY_LIVE_POLL_INTERVAL_SECONDS = 60u;
static const guint ACTIVITY_LIVE_PAST_PROBE_EVERY = 5u;
static const uint8_t ACTIVITY_PROTO_CHANNEL = 1u;
static const uint8_t ACTIVITY_ALT_PROTO_CHANNEL = 5u;
static int discover_spp_channel(const char *mac_addr) {
    uuid_t svc_uuid;
    uint32_t range = 0x0000ffff;
    sdp_list_t *response_list = NULL;
    sdp_list_t *search_list = NULL;
    sdp_list_t *attrid_list = NULL;
    int channel = -1;

    bdaddr_t target = {0};
    str2ba(mac_addr, &target);

    sdp_session_t *session = sdp_connect(BDADDR_ANY, &target, SDP_RETRY_IF_BUSY);
    if (!session) {
        return -1;
    }

    sdp_uuid16_create(&svc_uuid, SERIAL_PORT_SVCLASS_ID);
    search_list = sdp_list_append(NULL, &svc_uuid);
    attrid_list = sdp_list_append(NULL, &range);

    if (sdp_service_search_attr_req(session, search_list, SDP_ATTR_REQ_RANGE, attrid_list, &response_list) == 0) {
        for (sdp_list_t *r = response_list; r; r = r->next) {
            sdp_record_t *rec = r->data;
            sdp_list_t *proto_list = NULL;

            if (sdp_get_access_protos(rec, &proto_list) == 0) {
                for (sdp_list_t *p = proto_list; p; p = p->next) {
                    sdp_list_t *pds = p->data;
                    for (; pds; pds = pds->next) {
                        sdp_data_t *d = pds->data;
                        int proto = 0;
                        for (; d; d = d->next) {
                            switch (d->dtd) {
                                case SDP_UUID16:
                                case SDP_UUID32:
                                case SDP_UUID128:
                                    proto = sdp_uuid_to_proto(&d->val.uuid);
                                    break;
                                case SDP_UINT8:
                                    if (proto == RFCOMM_UUID) {
                                        channel = d->val.int8;
                                    }
                                    break;
                            }
                        }
                    }
                }
                sdp_list_free(proto_list, 0);
            }

            sdp_record_free(rec);
            if (channel > 0) {
                break;
            }
        }
    }

    sdp_list_free(response_list, 0);
    sdp_list_free(search_list, 0);
    sdp_list_free(attrid_list, 0);
    sdp_close(session);

    return channel;
}

static ssize_t write_all(int fd, const uint8_t *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, data + sent, len - sent);
        if (n <= 0) {
            return n;
        }
        sent += (size_t)n;
    }
    return (ssize_t)sent;
}
typedef struct {
    uint32_t type;
    uint32_t subtype;
    uint32_t auth_status;
    gboolean has_type;
    gboolean has_subtype;
    gboolean has_auth_status;
    uint8_t watch_nonce[16];
    uint8_t watch_hmac[32];
    gboolean has_watch_nonce;
} ParsedAuthCommand;
static void append_friendly_command_log(AppState *state, const Xiaomi__Command *cmd) {
    GString *s = g_string_new("Friendly ");
    g_string_append_printf(s, "type=%u", cmd->type);
    if (cmd->has_subtype) {
        g_string_append_printf(s, " subtype=%u", cmd->subtype);
    }
    g_string_append(s, " -> ");

    if (cmd->type == 18 && cmd->has_subtype && cmd->subtype == 0) {
        if (cmd->music && cmd->music->musicinfo) {
            const Xiaomi__MusicInfo *mi = cmd->music->musicinfo;
            g_string_append_printf(s, "Music info (state=%u", mi->state);
            if (mi->has_volume) g_string_append_printf(s, ", volume=%u", mi->volume);
            if (mi->track) g_string_append_printf(s, ", track=\"%s\"", mi->track);
            if (mi->artist) g_string_append_printf(s, ", artist=\"%s\"", mi->artist);
            g_string_append(s, ")");
        } else {
            g_string_append(s, "Music keepalive/status ping");
        }
    } else if (cmd->type == 17 && cmd->has_subtype && cmd->subtype == 7) {
        if (cmd->schedule && cmd->schedule->sleepmode) {
            g_string_append_printf(s, "Sleep mode status: %s",
                                   cmd->schedule->sleepmode->enabled ? "enabled" : "disabled");
        } else {
            g_string_append(s, "Schedule/sleep-mode related event (schema may be partial)");
        }
    } else if (cmd->type == 10 && cmd->has_subtype && cmd->subtype == 3) {
        if (cmd->weather && cmd->weather->location) {
            const Xiaomi__WeatherLocation *loc = cmd->weather->location;
            g_string_append_printf(s, "Weather location update request (code=%s",
                                   loc->code ? loc->code : "");
            if (loc->name) g_string_append_printf(s, ", name=%s", loc->name);
            g_string_append(s, ")");
        } else {
            g_string_append(s, "Weather update indication");
        }
    } else if (cmd->type == 2 && cmd->has_subtype && cmd->subtype == 42) {
        if (cmd->system && cmd->system->miscsettingset) {
            const Xiaomi__MiscSettingSet *m = cmd->system->miscsettingset;
            if (m->dndsync && m->dndsync->has_enabled) {
                g_string_append_printf(s, "System misc setting: DND sync %s", m->dndsync->enabled ? "enabled" : "disabled");
            } else if (m->wearingmode && m->wearingmode->has_mode) {
                g_string_append_printf(s, "System misc setting: wearing mode=%u", m->wearingmode->mode);
            } else if (m->miscnotificationsettings) {
                g_string_append(s, "System misc setting: notification settings changed");
            } else {
                g_string_append(s, "System misc setting changed");
            }
        } else {
            g_string_append(s, "System setting event");
        }
    } else if (cmd->type == 8 && cmd->health && cmd->health->realtimestats) {
        const Xiaomi__RealTimeStats *rt = cmd->health->realtimestats;
        g_string_append_printf(s, "Realtime stats");
        if (rt->has_steps) g_string_append_printf(s, " steps=%u", rt->steps);
        if (rt->has_heartrate) g_string_append_printf(s, " hr=%u", rt->heartrate);
        if (rt->has_calories) g_string_append_printf(s, " calories=%u", rt->calories);
        if (rt->has_unknown5) g_string_append_printf(s, " moving=%u", rt->unknown5);
        if (rt->has_standinghours) g_string_append_printf(s, " standing=%u", rt->standinghours);
    } else if (cmd->type == 8 && cmd->has_subtype &&
               (cmd->subtype == 1 || cmd->subtype == 2) &&
               cmd->health && cmd->health->has_activityrequestfileids) {
        const ProtobufCBinaryData ids = cmd->health->activityrequestfileids;
        const char *scope = (cmd->subtype == 1) ? "today" : "past";
        if (ids.len == 0) {
            g_string_append_printf(s, "Activity file IDs (%s): empty", scope);
        } else if (ids.len % 7 == 0) {
            g_string_append_printf(s, "Activity file IDs (%s): %zu entries", scope, ids.len / 7);
        } else {
            g_string_append_printf(s, "Activity file IDs (%s): %zu bytes (non-7-byte-aligned)", scope, ids.len);
        }
    } else {
        g_string_append(s, "Unmapped command (kept as PB line)");
    }

    append_log(state, s->str);
    g_string_free(s, TRUE);
}

static void log_decoded_command_proto(AppState *state, const uint8_t *plain, size_t plain_len) {
    Xiaomi__Command *cmd = xiaomi__command__unpack(NULL, plain_len, plain);
    if (cmd == NULL) {
        append_log(state, "protobuf-c failed to decode Xiaomi Command.");
        return;
    }

    if (cmd->health && cmd->health->realtimestats) {
        const Xiaomi__RealTimeStats *rt = cmd->health->realtimestats;
        schedule_metrics_update(
            state,
            rt->has_heartrate, rt->heartrate,
            rt->has_steps, rt->steps,
            rt->has_calories, rt->calories,
            rt->has_standinghours, rt->standinghours,
            FALSE, 0,
            rt->has_unknown5, rt->unknown5,
            NULL, NULL
        );
    }

    if (cmd->system && cmd->system->power && cmd->system->power->battery) {
        const Xiaomi__Battery *bat = cmd->system->power->battery;
        if (bat->has_level) {
            schedule_metrics_update(
                state,
                FALSE, 0,
                FALSE, 0,
                FALSE, 0,
                FALSE, 0,
                TRUE, bat->level,
                FALSE, 0,
                NULL, NULL
            );
        }
    }

    if (cmd->system && cmd->system->basicdevicestate && cmd->system->basicdevicestate->has_batterylevel) {
        schedule_metrics_update(
            state,
            FALSE, 0,
            FALSE, 0,
            FALSE, 0,
            FALSE, 0,
            TRUE, cmd->system->basicdevicestate->batterylevel,
            FALSE, 0,
            NULL, NULL
        );
    }

    if (cmd->system && cmd->system->basicdevicestate) {
        const Xiaomi__BasicDeviceState *bs = cmd->system->basicdevicestate;
        schedule_state_update(
            state,
            bs->ischarging ? "yes" : "no",
            bs->isworn ? "worn" : "not worn",
            bs->isuserasleep ? "asleep" : "awake"
        );
    }

    if (cmd->system && cmd->system->devicestate) {
        const Xiaomi__DeviceState *ds = cmd->system->devicestate;
        const gchar *charging = NULL;
        const gchar *wearing = NULL;
        const gchar *sleep = NULL;
        if (ds->has_chargingstate) charging = (ds->chargingstate == 1) ? "yes" : "no";
        if (ds->has_wearingstate) wearing = (ds->wearingstate == 1) ? "worn" : "not worn";
        if (ds->has_sleepstate) sleep = (ds->sleepstate == 1) ? "asleep" : "awake";
        schedule_state_update(state, charging, wearing, sleep);
    }

    if (cmd->health && cmd->health->spo2) {
        const Xiaomi__SpO2 *sp = cmd->health->spo2;
        gchar *txt = NULL;
        if (sp->alarmlow && sp->alarmlow->has_alarmlowenabled && sp->alarmlow->alarmlowenabled
            && sp->alarmlow->has_alarmlowthreshold && sp->alarmlow->alarmlowthreshold > 0) {
            txt = g_strdup_printf("alarm <%u%% (%s)",
                                  sp->alarmlow->alarmlowthreshold,
                                  sp->alarmlow->alarmlowenabled ? "on" : "off");
        } else if (sp->alarmlow && sp->alarmlow->has_alarmlowenabled && !sp->alarmlow->alarmlowenabled) {
            txt = g_strdup("alarm off");
        } else {
            txt = g_strdup_printf("all-day: %s", (sp->has_alldaytracking && sp->alldaytracking) ? "on" : "off");
        }
        schedule_metrics_update(state, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, txt, NULL);
        g_free(txt);
    }

    if (cmd->health && cmd->health->stress) {
        const Xiaomi__Stress *st = cmd->health->stress;
        gchar *txt = g_strdup_printf("all-day: %s", st->has_alldaytracking && st->alldaytracking ? "on" : "off");
        schedule_metrics_update(state, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, FALSE, 0, NULL, txt);
        g_free(txt);
    }

    if (cmd->type == 8 && cmd->has_subtype &&
        (cmd->subtype == 1 || cmd->subtype == 2) &&
        cmd->health && cmd->health->has_activityrequestfileids) {
        const uint8_t *ids = cmd->health->activityrequestfileids.data;
        const size_t len = cmd->health->activityrequestfileids.len;
        const gboolean ids_valid = ((len % 7u) == 0u);
        gchar *line = g_strdup_printf("History file IDs received (%s): %zu bytes%s",
                                      cmd->subtype == 1 ? "today" : "past",
                                      len,
                                      ids_valid ? "" : " (invalid length)");
        append_log(state, line);
        g_free(line);

        if (!ids_valid) {
            append_log(state, "History file IDs payload length is not divisible by 7; ignoring this list.");
        } else if (len >= 7) {
            guint n_daily_details = 0;
            guint n_daily_summary = 0;
            guint n_sleep = 0;
            guint n_sleep_stages = 0;
            guint n_other = 0;
            for (size_t off = 0; off + 7 <= len; off += 7) {
                const uint8_t *p = ids + off;
                uint32_t ts = (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
                int8_t tz = (int8_t)p[4];
                uint8_t ver = p[5];
                uint8_t flags = p[6];
                int type = (flags >> 7) & 1;
                int subtype = (flags & 127) >> 2;
                int detail = flags & 3;
                if (type == 0 && subtype == 0 && detail == 0) {
                    n_daily_details++;
                } else if (type == 0 && subtype == 0 && detail == 1) {
                    n_daily_summary++;
                } else if (type == 0 && subtype == 8) {
                    n_sleep++;
                } else if (type == 0 && subtype == 3) {
                    n_sleep_stages++;
                } else {
                    n_other++;
                }
                const char *name = activity_subtype_name(type, subtype);
                gchar *item = g_strdup_printf("  id ts=%u tz=%d type=%d subtype=%d(%s) detail=%d ver=%u",
                                              ts, tz, type, subtype, name, detail, ver);
                append_log(state, item);
                g_free(item);
            }

            gchar *summary = g_strdup_printf(
                "ID mix: dailyDetails=%u dailySummary=%u sleep=%u sleepStages=%u other=%u",
                n_daily_details, n_daily_summary, n_sleep, n_sleep_stages, n_other
            );
            append_log(state, summary);
            g_free(summary);

            log_activity_file_ids(ids, len, "Activity file ids");
            enqueue_activity_file_ids(state, ids, len);
            trigger_next_activity_fetch(state);
        }

        if (cmd->subtype == 1) {
            /* Request past once after the first valid today IDs response. */
            if (ids_valid) {
                if (!state->activity_past_requested) {
                    state->activity_past_requested = TRUE;
                    send_activity_fetch_past_channels(state, "Requested activity file IDs (past probe)");
                }
            } else {
                append_log(state, "Skipping past IDs request due to invalid today-IDs payload.");
            }
        } else if (cmd->subtype == 2 && len == 0) {
            append_log(state, "Activity file IDs (past): empty");
        }
    }

    if (state->verbose_wire_log) {
        GString *s = g_string_new("PB ");
        g_string_append_printf(s, "type=%u", cmd->type);
        if (cmd->has_subtype) {
            g_string_append_printf(s, " subtype=%u", cmd->subtype);
        }
        if (cmd->has_status) {
            g_string_append_printf(s, " status=%u", cmd->status);
        }
        g_string_append(s, " ");
        summarize_pb_message((const ProtobufCMessage *)cmd, s, 0);
        append_log(state, s->str);
        g_string_free(s, TRUE);
        append_friendly_command_log(state, cmd);
    }

    xiaomi__command__free_unpacked(cmd, NULL);
}

static gboolean parse_auth_message(const uint8_t *buf, size_t len, ParsedAuthCommand *cmd) {
    memset(cmd, 0, sizeof(*cmd));
    size_t idx = 0;
    while (idx < len) {
        uint64_t tag = 0;
        if (!pb_read_varint(buf, len, &idx, &tag)) return FALSE;
        uint32_t field = (uint32_t)(tag >> 3);
        uint32_t wt = (uint32_t)(tag & 0x07);

        if (field == 1 && wt == 0) {
            uint64_t v = 0;
            if (!pb_read_varint(buf, len, &idx, &v)) return FALSE;
            cmd->type = (uint32_t)v;
            cmd->has_type = TRUE;
        } else if (field == 2 && wt == 0) {
            uint64_t v = 0;
            if (!pb_read_varint(buf, len, &idx, &v)) return FALSE;
            cmd->subtype = (uint32_t)v;
            cmd->has_subtype = TRUE;
        } else if (field == 3 && wt == 2) { /* Auth */
            uint64_t auth_len = 0;
            if (!pb_read_varint(buf, len, &idx, &auth_len)) return FALSE;
            if (idx + auth_len > len) return FALSE;
            size_t aidx = idx;
            const size_t aend = idx + (size_t)auth_len;
            while (aidx < aend) {
                uint64_t atag = 0;
                if (!pb_read_varint(buf, aend, &aidx, &atag)) return FALSE;
                uint32_t af = (uint32_t)(atag >> 3);
                uint32_t awt = (uint32_t)(atag & 0x07);

                if (af == 8 && awt == 0) {
                    uint64_t v = 0;
                    if (!pb_read_varint(buf, aend, &aidx, &v)) return FALSE;
                    cmd->auth_status = (uint32_t)v;
                    cmd->has_auth_status = TRUE;
                } else if (af == 31 && awt == 2) { /* WatchNonce */
                    uint64_t wn_len = 0;
                    if (!pb_read_varint(buf, aend, &aidx, &wn_len)) return FALSE;
                    if (aidx + wn_len > aend) return FALSE;
                    size_t widx = aidx;
                    const size_t wend = aidx + (size_t)wn_len;
                    while (widx < wend) {
                        uint64_t wtag = 0;
                        if (!pb_read_varint(buf, wend, &widx, &wtag)) return FALSE;
                        uint32_t wf = (uint32_t)(wtag >> 3);
                        uint32_t wwt = (uint32_t)(wtag & 0x07);
                        if (wwt != 2) {
                            if (!pb_skip_field(buf, wend, &widx, wwt)) return FALSE;
                            continue;
                        }
                        uint64_t blen = 0;
                        if (!pb_read_varint(buf, wend, &widx, &blen)) return FALSE;
                        if (widx + blen > wend) return FALSE;
                        if (wf == 1 && blen == 16) {
                            memcpy(cmd->watch_nonce, buf + widx, 16);
                        } else if (wf == 2 && blen == 32) {
                            memcpy(cmd->watch_hmac, buf + widx, 32);
                            cmd->has_watch_nonce = TRUE;
                        }
                        widx += (size_t)blen;
                    }
                    aidx = wend;
                } else {
                    if (!pb_skip_field(buf, aend, &aidx, awt)) return FALSE;
                }
            }
            idx += (size_t)auth_len;
        } else {
            if (!pb_skip_field(buf, len, &idx, wt)) return FALSE;
        }
    }
    return TRUE;
}

static gboolean send_spp_v2_packet(AppState *state, uint8_t packet_type, uint8_t seq, const uint8_t *payload, size_t payload_len) {
    uint8_t *pkt = g_malloc(8 + payload_len);
    pkt[0] = 0xA5;
    pkt[1] = 0xA5;
    pkt[2] = packet_type & 0x0F;
    pkt[3] = seq;
    pkt[4] = (uint8_t)(payload_len & 0xFF);
    pkt[5] = (uint8_t)((payload_len >> 8) & 0xFF);
    uint16_t crc = crc16_arc(payload, payload_len);
    pkt[6] = (uint8_t)(crc & 0xFF);
    pkt[7] = (uint8_t)((crc >> 8) & 0xFF);
    if (payload_len > 0) {
        memcpy(pkt + 8, payload, payload_len);
    }

    gboolean ok = write_all(state->sock_fd, pkt, 8 + payload_len) > 0;
    g_free(pkt);
    return ok;
}

static gboolean send_spp_v2_ack(AppState *state, uint8_t seq) {
    if (!send_spp_v2_packet(state, 1, seq, NULL, 0)) {
        append_log(state, "Failed to send SPPv2 ACK.");
        return FALSE;
    }
    return TRUE;
}

static gboolean send_spp_v2_session_config(AppState *state) {
    const uint8_t payload[] = {
        0x01,
        0x01, 0x03, 0x00, 0x01, 0x00, 0x00,
        0x02, 0x02, 0x00, 0x00, 0xFC,
        0x03, 0x02, 0x00, 0x20, 0x00,
        0x04, 0x02, 0x00, 0x10, 0x27
    };
    if (!send_spp_v2_packet(state, 2, 0, payload, sizeof(payload))) {
        append_log(state, "Failed to send SPPv2 session config.");
        return FALSE;
    }
    append_log(state, "Sent SPPv2 session config.");
    return TRUE;
}

static gboolean send_auth_step1_nonce_command(AppState *state) {
    uint8_t secret_key[16];
    if (!hex_to_bytes16(state->auth_key_hex, secret_key)) {
        append_log(state, "Invalid auth key format.");
        return FALSE;
    }

    if (RAND_bytes(state->phone_nonce, sizeof(state->phone_nonce)) != 1) {
        append_log(state, "Failed to generate phone nonce.");
        return FALSE;
    }

    GByteArray *phone_nonce = g_byte_array_new();
    pb_append_len_bytes(phone_nonce, 1, state->phone_nonce, 16);

    GByteArray *auth = g_byte_array_new();
    pb_append_len_bytes(auth, 30, phone_nonce->data, phone_nonce->len);

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 1);   /* type = auth */
    pb_append_u32(cmd, 2, 26);  /* subtype = nonce */
    pb_append_len_bytes(cmd, 3, auth->data, auth->len);

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1; /* protobuf/auth channel */
    uint8_t op = 1;   /* plaintext */
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, cmd->data, cmd->len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    if (ok) {
        append_log(state, "Sent auth step 1 (phone nonce).");
    } else {
        append_log(state, "Failed to send auth step 1.");
    }

    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    g_byte_array_unref(auth);
    g_byte_array_unref(phone_nonce);
    return ok;
}

static gboolean send_auth_step2(AppState *state, const ParsedAuthCommand *parsed) {
    uint8_t secret_key[16];
    uint8_t step_hmac[64];
    uint8_t expected_watch_hmac[32];
    uint8_t input_watch_phone[32];
    uint8_t input_phone_watch[32];

    if (!hex_to_bytes16(state->auth_key_hex, secret_key)) {
        append_log(state, "Invalid auth key format.");
        return FALSE;
    }

    memcpy(state->watch_nonce, parsed->watch_nonce, 16);
    if (!compute_auth_step3_hmac(secret_key, state->phone_nonce, state->watch_nonce, step_hmac)) {
        append_log(state, "Failed to compute auth step HMAC.");
        return FALSE;
    }
    memcpy(state->decryption_key, step_hmac, 16);
    memcpy(state->encryption_key, step_hmac + 16, 16);
    memcpy(state->decryption_nonce, step_hmac + 32, 4);
    memcpy(state->encryption_nonce, step_hmac + 36, 4);

    memcpy(input_watch_phone, state->watch_nonce, 16);
    memcpy(input_watch_phone + 16, state->phone_nonce, 16);
    if (!hmac_sha256(state->decryption_key, 16, input_watch_phone, sizeof(input_watch_phone), expected_watch_hmac)) {
        append_log(state, "Failed to verify watch HMAC.");
        return FALSE;
    }

    if (memcmp(expected_watch_hmac, parsed->watch_hmac, 32) != 0) {
        append_log(state, "Watch HMAC mismatch: auth key likely wrong.");
        return FALSE;
    }

    memcpy(input_phone_watch, state->phone_nonce, 16);
    memcpy(input_phone_watch + 16, state->watch_nonce, 16);
    uint8_t encrypted_nonces[32];
    if (!hmac_sha256(state->encryption_key, 16, input_phone_watch, sizeof(input_phone_watch), encrypted_nonces)) {
        append_log(state, "Failed to compute encrypted nonces.");
        return FALSE;
    }

    GByteArray *device_info = g_byte_array_new();
    pb_append_u32(device_info, 1, 0);
    pb_append_float(device_info, 2, 34.0f);
    pb_append_len_bytes(device_info, 3, (const uint8_t *)"Linux", 5);
    pb_append_u32(device_info, 4, 224);
    pb_append_len_bytes(device_info, 5, (const uint8_t *)"US", 2);

    uint8_t nonce12[12] = {0};
    memcpy(nonce12, state->encryption_nonce, 4);
    uint8_t *encrypted_device_info = NULL;
    size_t encrypted_device_info_len = 0;
    if (!aes_ccm_encrypt_4byte_tag(state->encryption_key, nonce12,
                                   device_info->data, device_info->len,
                                   &encrypted_device_info, &encrypted_device_info_len)) {
        g_byte_array_unref(device_info);
        append_log(state, "Failed to AES-CCM encrypt device info.");
        return FALSE;
    }
    g_byte_array_unref(device_info);

    GByteArray *auth_step3 = g_byte_array_new();
    pb_append_len_bytes(auth_step3, 1, encrypted_nonces, sizeof(encrypted_nonces));
    pb_append_len_bytes(auth_step3, 2, encrypted_device_info, encrypted_device_info_len);
    g_free(encrypted_device_info);

    GByteArray *auth = g_byte_array_new();
    pb_append_len_bytes(auth, 32, auth_step3->data, auth_step3->len);

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 1);   /* type auth */
    pb_append_u32(cmd, 2, 27);  /* subtype auth */
    pb_append_len_bytes(cmd, 3, auth->data, auth->len);

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1;
    uint8_t op = 1; /* plaintext for authentication channel */
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, cmd->data, cmd->len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Sent auth step 2 (AuthStep3)." : "Failed to send auth step 2.");

    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    g_byte_array_unref(auth);
    g_byte_array_unref(auth_step3);
    return ok;
}

static gboolean send_realtime_stats_start(AppState *state) {
    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 8);   /* Health command type */
    pb_append_u32(cmd, 2, 45);  /* CMD_REALTIME_STATS_START */

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        append_log(state, "Failed to encrypt realtime stats start command.");
        return FALSE;
    }

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1; /* protobuf command channel */
    uint8_t op = 2;   /* encrypted */
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Requested realtime stats stream." : "Failed to request realtime stats stream.");

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    return ok;
}

static void log_activity_file_ids(const uint8_t *ids, size_t len, const char *label) {
    if (len == 0) {
        return;
    }
    GString *s = g_string_new(NULL);
    g_string_append_printf(s, "%s (%zu bytes, %zu ids)", label, len, len / 7);
    g_message("%s", s->str);
    g_string_free(s, TRUE);
}

static gboolean send_activity_fetch_request(AppState *state, const uint8_t *ids, size_t ids_len) {
    if (ids == NULL || ids_len == 0) return FALSE;

    GByteArray *health = g_byte_array_new();
    /* Match XiaomiHealthService.requestRecordedData(): field 2 = activityRequestFileIds */
    pb_append_len_bytes(health, 2, ids, ids_len);

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 8);  /* health */
    pb_append_u32(cmd, 2, 3);  /* CMD_ACTIVITY_FETCH_REQUEST (GB constant = 3, not 4) */
    pb_append_len_bytes(cmd, 10, health->data, health->len); /* health field in Command */

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        g_byte_array_unref(health);
        return FALSE;
    }

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1, op = 2;
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);
    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Requested activity history file chunks." : "Failed to request activity history chunks.");

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    g_byte_array_unref(health);
    return ok;
}

gboolean send_activity_fetch_ack(AppState *state, const uint8_t *ids, size_t ids_len) {
    if (ids == NULL || ids_len == 0) return FALSE;

    GByteArray *health = g_byte_array_new();
    /* Match XiaomiHealthService.ackRecordedData(): field 3 = activitySyncAckFileIds */
    pb_append_len_bytes(health, 3, ids, ids_len);

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 8);  /* health */
    pb_append_u32(cmd, 2, 5);  /* CMD_ACTIVITY_FETCH_ACK */
    pb_append_len_bytes(cmd, 10, health->data, health->len);

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        g_byte_array_unref(health);
        return FALSE;
    }

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1, op = 2;
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);
    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Acked fetched activity file ID(s)." : "Failed to ack fetched activity file ID(s).");

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    g_byte_array_unref(health);
    return ok;
}

static gchar *activity_file_id_key(const uint8_t *id7) {
    static const char hex[] = "0123456789ABCDEF";
    gchar *out = g_malloc(15);
    for (int i = 0; i < 7; i++) {
        out[i * 2] = hex[(id7[i] >> 4) & 0x0F];
        out[i * 2 + 1] = hex[id7[i] & 0x0F];
    }
    out[14] = '\0';
    return out;
}

void mib10_log_fetch_transition(AppState *state, const char *event, const uint8_t *file_id7) {
    if (state == NULL || !state->verbose_fetch_log) {
        return;
    }

    const guint qlen = (state->activity_fetch_queue != NULL) ? g_queue_get_length(state->activity_fetch_queue) : 0;
    gchar *id_txt = (file_id7 != NULL) ? activity_file_id_key(file_id7) : g_strdup("-");
    gchar *cur_txt = state->activity_fetch_inflight ? activity_file_id_key(state->activity_fetch_current_id) : g_strdup("-");
    gchar *line = g_strdup_printf("FETCH %s q=%u inflight=%s current=%s id=%s",
                                  event ? event : "?",
                                  qlen,
                                  state->activity_fetch_inflight ? "yes" : "no",
                                  cur_txt,
                                  id_txt);
    append_log(state, line);
    g_free(line);
    g_free(cur_txt);
    g_free(id_txt);
}

static void cancel_activity_fetch_timeout(AppState *state) {
    if (state->activity_fetch_timeout_source_id != 0) {
        g_source_remove(state->activity_fetch_timeout_source_id);
        state->activity_fetch_timeout_source_id = 0;
    }
}

static gboolean activity_fetch_timeout_cb(gpointer user_data) {
    AppState *state = user_data;
    state->activity_fetch_timeout_source_id = 0;
    if (!state->activity_fetch_inflight) {
        return G_SOURCE_REMOVE;
    }

    gchar *id_txt = activity_file_id_key(state->activity_fetch_current_id);
    gchar *line = g_strdup_printf("Timed out waiting for activity file %s; requesting next.", id_txt);
    append_log(state, line);
    g_free(line);
    g_free(id_txt);
    mib10_log_fetch_transition(state, "timeout", state->activity_fetch_current_id);

    state->activity_fetch_inflight = FALSE;
    memset(state->activity_fetch_current_id, 0, sizeof(state->activity_fetch_current_id));
    trigger_next_activity_fetch(state);
    return G_SOURCE_REMOVE;
}

static void schedule_activity_fetch_timeout(AppState *state) {
    cancel_activity_fetch_timeout(state);
    state->activity_fetch_timeout_source_id = g_timeout_add(5000, activity_fetch_timeout_cb, state);
}

static int activity_detail_fetch_order(uint8_t detail) {
    /* Match XiaomiActivityFileId.DetailType#getFetchOrder in GB. */
    switch (detail) {
        case 1: return 0; /* summary first */
        case 0: return 1; /* details */
        case 2: return 2; /* gps track */
        default: return 3;
    }
}

static gint activity_file_id_compare(gconstpointer a, gconstpointer b, gpointer user_data) {
    (void)user_data;
    const uint8_t *aa = a;
    const uint8_t *bb = b;

    const uint32_t ts_a = mib10_read_u32_le(aa);
    const uint32_t ts_b = mib10_read_u32_le(bb);
    if (ts_a != ts_b) return (ts_a < ts_b) ? -1 : 1;

    const int tz_a = (int8_t)aa[4];
    const int tz_b = (int8_t)bb[4];
    if (tz_a != tz_b) return (tz_a < tz_b) ? -1 : 1;

    const uint8_t flags_a = aa[6];
    const uint8_t flags_b = bb[6];
    const int type_a = (flags_a >> 7) & 1;
    const int type_b = (flags_b >> 7) & 1;
    if (type_a != type_b) return (type_a < type_b) ? -1 : 1;

    const int subtype_a = (flags_a & 127) >> 2;
    const int subtype_b = (flags_b & 127) >> 2;
    if (subtype_a != subtype_b) return (subtype_a < subtype_b) ? -1 : 1;

    const int detail_order_a = activity_detail_fetch_order(flags_a & 0x03);
    const int detail_order_b = activity_detail_fetch_order(flags_b & 0x03);
    if (detail_order_a != detail_order_b) return (detail_order_a < detail_order_b) ? -1 : 1;

    const uint8_t ver_a = aa[5];
    const uint8_t ver_b = bb[5];
    if (ver_a != ver_b) return (ver_a < ver_b) ? -1 : 1;

    return 0;
}

void reset_activity_fetch_state(AppState *state) {
    cancel_activity_fetch_timeout(state);
    state->activity_fetch_inflight = FALSE;
    state->activity_refetch_round = 0;
    state->activity_past_requested = FALSE;
    memset(state->activity_fetch_current_id, 0, sizeof(state->activity_fetch_current_id));
    if (state->activity_fetch_queue != NULL) {
        g_queue_clear_full(state->activity_fetch_queue, g_free);
    }
    if (state->activity_fetch_seen != NULL) {
        g_hash_table_remove_all(state->activity_fetch_seen);
    }
    mib10_log_fetch_transition(state, "reset", NULL);
}

static void enqueue_activity_file_ids(AppState *state, const uint8_t *ids, size_t len) {
    if (ids == NULL || len < 7) return;
    if (state->activity_fetch_queue == NULL) {
        state->activity_fetch_queue = g_queue_new();
    }
    if (state->activity_fetch_seen == NULL) {
        state->activity_fetch_seen = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    }

    guint added = 0;
    for (size_t off = 0; off + 7 <= len; off += 7) {
        const uint8_t *id = ids + off;
        const uint32_t ts = mib10_read_u32_le(id);
        const uint8_t ver = id[5];
        if (ts == 0 && ver == 0) {
            continue;
        }
        gchar *key = activity_file_id_key(id);
        if (g_hash_table_contains(state->activity_fetch_seen, key)) {
            g_free(key);
            continue;
        }
        g_hash_table_add(state->activity_fetch_seen, key);
        g_queue_insert_sorted(state->activity_fetch_queue,
                              g_memdup2(id, 7),
                              activity_file_id_compare,
                              NULL);
        added++;
    }
    if (added > 0) {
        gchar *event = g_strdup_printf("enqueue+%u", added);
        mib10_log_fetch_transition(state, event, NULL);
        g_free(event);
    }
}

void trigger_next_activity_fetch(AppState *state) {
    if (!state->auth_initialized || state->activity_fetch_inflight) {
        return;
    }
    if (state->activity_fetch_queue == NULL || g_queue_is_empty(state->activity_fetch_queue)) {
        return;
    }

    uint8_t *next_id = g_queue_pop_head(state->activity_fetch_queue);
    if (next_id == NULL) {
        return;
    }

    if (send_activity_fetch_request(state, next_id, 7)) {
        state->activity_fetch_inflight = TRUE;
        memcpy(state->activity_fetch_current_id, next_id, 7);
        schedule_activity_fetch_timeout(state);
        mib10_log_fetch_transition(state, "request-sent", next_id);
    } else {
        gchar *key = activity_file_id_key(next_id);
        if (state->activity_fetch_seen != NULL) {
            g_hash_table_remove(state->activity_fetch_seen, key);
        }
        g_free(key);
        mib10_log_fetch_transition(state, "request-failed", next_id);
    }

    g_free(next_id);
}

static gboolean send_activity_fetch_catalog_request(AppState *state,
                                                    uint32_t subtype,
                                                    gboolean include_activity_sync_today,
                                                    gboolean include_unknown1,
                                                    uint32_t unknown1,
                                                    uint8_t tx_channel,
                                                    const char *label) {
    GByteArray *health = NULL;
    if (include_activity_sync_today) {
        GByteArray *activity_sync_today = g_byte_array_new();
        if (include_unknown1) {
            pb_append_u32(activity_sync_today, 1, unknown1);
        }
        health = g_byte_array_new();
        pb_append_len_bytes(health, 5, activity_sync_today->data, activity_sync_today->len);
        g_byte_array_unref(activity_sync_today);
    }

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 8);
    pb_append_u32(cmd, 2, subtype);
    if (health != NULL) {
        pb_append_len_bytes(cmd, 10, health->data, health->len);
        g_byte_array_unref(health);
    }

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        return FALSE;
    }
    g_byte_array_unref(cmd);

    GByteArray *v2payload = g_byte_array_new();
    const uint8_t op = 2;
    g_byte_array_append(v2payload, &tx_channel, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    if (label) {
        gchar *line = NULL;
        if (include_activity_sync_today && include_unknown1) {
            line = g_strdup_printf("%s (unknown1=%u, txChan=%u): %s", label, unknown1, tx_channel, ok ? "sent" : "failed");
        } else if (include_activity_sync_today) {
            line = g_strdup_printf("%s (unknown1=<unset>, txChan=%u): %s", label, tx_channel, ok ? "sent" : "failed");
        } else {
            line = g_strdup_printf("%s (txChan=%u): %s", label, tx_channel, ok ? "sent" : "failed");
        }
        append_log(state, line);
        g_free(line);
    }

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    return ok;
}

/* Gadgetbridge fetchRecordedDataToday(): CMD 8,1 with Health.activitySyncRequestToday.
 * We probe multiple unknown1 values because some firmware variants expose different ID sets.
 */
static gboolean send_activity_fetch_today_request(AppState *state, uint32_t unknown1, const char *label) {
    return send_activity_fetch_catalog_request(state,
                                               1,
                                               TRUE,
                                               TRUE,
                                               unknown1,
                                               ACTIVITY_PROTO_CHANNEL,
                                               label);
}

static gboolean send_activity_fetch_today_request_unset_unknown(AppState *state, const char *label) {
    return send_activity_fetch_catalog_request(state,
                                               1,
                                               TRUE,
                                               FALSE,
                                               0,
                                               ACTIVITY_PROTO_CHANNEL,
                                               label);
}

static gboolean send_activity_fetch_today_request_on_channel(AppState *state, uint32_t unknown1, uint8_t tx_channel, const char *label) {
    return send_activity_fetch_catalog_request(state,
                                               1,
                                               TRUE,
                                               TRUE,
                                               unknown1,
                                               tx_channel,
                                               label);
}

static gboolean send_activity_fetch_past_request(AppState *state, const char *label) {
    return send_activity_fetch_catalog_request(state,
                                               2,
                                               FALSE,
                                               FALSE,
                                               0,
                                               ACTIVITY_PROTO_CHANNEL,
                                               label);
}

static gboolean send_activity_fetch_past_request_with_unknown(AppState *state, uint32_t unknown1, const char *label) {
    return send_activity_fetch_catalog_request(state,
                                               2,
                                               TRUE,
                                               TRUE,
                                               unknown1,
                                               ACTIVITY_PROTO_CHANNEL,
                                               label);
}

static void send_activity_fetch_today_channels(AppState *state, const char *label_prefix) {
    for (uint32_t channel = 0; channel <= ACTIVITY_TODAY_CHANNEL_MAX; channel++) {
        gchar *label = g_strdup_printf("%s [channel=%u]", label_prefix, channel);
        (void)send_activity_fetch_today_request(state, channel, label);
        g_free(label);
    }
    gchar *unset_label = g_strdup_printf("%s [unknown1=unset]", label_prefix);
    (void)send_activity_fetch_today_request_unset_unknown(state, unset_label);
    g_free(unset_label);
    gchar *alt_label = g_strdup_printf("%s [alt tx channel]", label_prefix);
    (void)send_activity_fetch_today_request_on_channel(state, 0, ACTIVITY_ALT_PROTO_CHANNEL, alt_label);
    g_free(alt_label);
}

static void send_activity_fetch_past_channels(AppState *state, const char *label_prefix) {
    (void)send_activity_fetch_past_request(state, "Requested activity file IDs (past)");
    for (uint32_t channel = 0; channel <= ACTIVITY_TODAY_CHANNEL_MAX; channel++) {
        gchar *label = g_strdup_printf("%s [channel=%u]", label_prefix, channel);
        (void)send_activity_fetch_past_request_with_unknown(state, channel, label);
        g_free(label);
    }
}

static void stop_activity_live_poll(AppState *state) {
    if (state->activity_live_poll_source_id != 0) {
        g_source_remove(state->activity_live_poll_source_id);
        state->activity_live_poll_source_id = 0;
    }
    state->activity_live_poll_round = 0;
}

static gboolean activity_live_poll_cb(gpointer user_data) {
    AppState *state = user_data;
    if (!state->auth_initialized || state->sock_fd < 0) {
        state->activity_live_poll_source_id = 0;
        state->activity_live_poll_round = 0;
        return G_SOURCE_REMOVE;
    }

    state->activity_live_poll_round++;
    gchar *line = g_strdup_printf("Rolling history capture poll %u.", state->activity_live_poll_round);
    append_log(state, line);
    g_free(line);

    (void)send_activity_fetch_today_request(state, 0, "Rolling history file IDs (today)");
    (void)send_activity_fetch_today_request(state, 1, "Rolling history file IDs (today, alt)");

    if ((state->activity_live_poll_round % ACTIVITY_LIVE_PAST_PROBE_EVERY) == 0) {
        send_activity_fetch_past_channels(state, "Rolling history file IDs (past probe)");
    }

    return G_SOURCE_CONTINUE;
}

static void start_activity_live_poll(AppState *state) {
    stop_activity_live_poll(state);
    state->activity_live_poll_source_id = g_timeout_add_seconds(ACTIVITY_LIVE_POLL_INTERVAL_SECONDS, activity_live_poll_cb, state);
    append_log(state, "Enabled rolling history capture poll (every 60s).");
}

static gboolean auto_history_followup_cb(gpointer user_data) {
    AppState *state = user_data;
    if (!state->auth_initialized || state->sock_fd < 0) {
        state->activity_refetch_source_id = 0;
        return G_SOURCE_REMOVE;
    }

    if (state->activity_refetch_round >= ACTIVITY_AUTO_REFETCH_ROUNDS) {
        state->activity_refetch_source_id = 0;
        return G_SOURCE_REMOVE;
    }

    state->activity_refetch_round++;
    gchar *line = g_strdup_printf("Auto follow-up history sync round %u/%u.", state->activity_refetch_round, ACTIVITY_AUTO_REFETCH_ROUNDS);
    append_log(state, line);
    g_free(line);
    send_activity_fetch_today_channels(state, "Auto history file IDs (today)");
    send_activity_fetch_past_channels(state, "Auto history file IDs (past probe)");

    if (state->activity_refetch_round >= ACTIVITY_AUTO_REFETCH_ROUNDS) {
        state->activity_refetch_source_id = 0;
        return G_SOURCE_REMOVE;
    }
    return G_SOURCE_CONTINUE;
}

static gboolean send_encrypted_simple_command(AppState *state, uint32_t type, uint32_t subtype, const char *label) {
    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, type);
    pb_append_u32(cmd, 2, subtype);

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        return FALSE;
    }

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1;
    uint8_t op = 2;
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    if (label) {
        gchar *line = g_strdup_printf("%s: %s", label, ok ? "sent" : "failed");
        append_log(state, line);
        g_free(line);
    }

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    return ok;
}

/* XiaomiSystemService.setCurrentTime(): type 2, subtype 3, System.clock — runs before service.initialize() in GB onAuthSuccess. */
static gboolean send_gb_set_current_time(AppState *state) {
    GDateTime *now = g_date_time_new_now_local();
    GTimeZone *gtz = g_date_time_get_timezone(now);
    const gchar *tz_id = g_time_zone_get_identifier(gtz);
    if (tz_id == NULL) {
        tz_id = "";
    }

    GTimeSpan utc_off = g_date_time_get_utc_offset(now);
    int32_t zone_blocks = (int32_t)(utc_off / (15 * G_TIME_SPAN_MINUTE));

    Xiaomi__Date date = XIAOMI__DATE__INIT;
    date.year = g_date_time_get_year(now);
    date.month = g_date_time_get_month(now);
    date.day = g_date_time_get_day_of_month(now);

    Xiaomi__Time t = XIAOMI__TIME__INIT;
    t.hour = g_date_time_get_hour(now);
    t.minute = g_date_time_get_minute(now);
    t.has_second = TRUE;
    t.second = g_date_time_get_second(now);
    t.has_millisecond = TRUE;
    t.millisecond = g_date_time_get_microsecond(now) / 1000;

    Xiaomi__TimeZone tz = XIAOMI__TIME_ZONE__INIT;
    tz.has_zoneoffset = TRUE;
    tz.zoneoffset = zone_blocks;
    tz.has_dstoffset = TRUE;
    tz.dstoffset = 0;
    tz.name = g_strdup(tz_id);

    Xiaomi__Clock clk = XIAOMI__CLOCK__INIT;
    clk.date = &date;
    clk.time = &t;
    clk.timezone = &tz;
    clk.has_isnot24hour = TRUE;
    clk.isnot24hour = FALSE;

    Xiaomi__System sys = XIAOMI__SYSTEM__INIT;
    sys.clock = &clk;

    Xiaomi__Command cmd = XIAOMI__COMMAND__INIT;
    cmd.type = 2;
    cmd.has_subtype = TRUE;
    cmd.subtype = 3;
    cmd.system = &sys;

    size_t packed_len = xiaomi__command__get_packed_size(&cmd);
    uint8_t *packed = g_malloc(packed_len);
    xiaomi__command__pack(&cmd, packed);
    g_free(tz.name);
    g_date_time_unref(now);

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, packed, packed_len, &encrypted, &encrypted_len)) {
        g_free(packed);
        append_log(state, "Failed to encrypt set-clock command.");
        return FALSE;
    }
    g_free(packed);

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1;
    uint8_t op = 2;
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Sent clock/time (GB setCurrentTime)." : "Failed to send clock/time.");

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    return ok;
}

/* XiaomiHealthService.setUserInfo(): type 8, subtype 0, Health.userInfo */
static gboolean send_gb_user_info(AppState *state) {
    Xiaomi__UserInfo ui = XIAOMI__USER_INFO__INIT;
    ui.has_height = TRUE;
    ui.height = 170;
    ui.has_weight = TRUE;
    ui.weight = 70.0f;
    ui.has_birthday = TRUE;
    ui.birthday = 19900101u;
    ui.has_gender = TRUE;
    ui.gender = 1;
    ui.has_maxheartrate = TRUE;
    ui.maxheartrate = 185;
    ui.has_goalcalories = TRUE;
    ui.goalcalories = 500;
    ui.has_goalsteps = TRUE;
    ui.goalsteps = 10000;
    ui.has_goalstanding = TRUE;
    ui.goalstanding = 12;
    ui.has_goalmoving = TRUE;
    ui.goalmoving = 30;

    size_t ui_packed_len = xiaomi__user_info__get_packed_size(&ui);
    uint8_t *ui_packed = g_malloc(ui_packed_len);
    xiaomi__user_info__pack(&ui, ui_packed);

    GByteArray *health = g_byte_array_new();
    pb_append_len_bytes(health, 1, ui_packed, ui_packed_len);
    g_free(ui_packed);

    GByteArray *cmd = g_byte_array_new();
    pb_append_u32(cmd, 1, 8);
    pb_append_u32(cmd, 2, 0);
    pb_append_len_bytes(cmd, 10, health->data, health->len);

    uint8_t *encrypted = NULL;
    size_t encrypted_len = 0;
    if (!aes_ctr_encrypt(state->encryption_key, state->encryption_key, cmd->data, cmd->len, &encrypted, &encrypted_len)) {
        g_byte_array_unref(cmd);
        g_byte_array_unref(health);
        append_log(state, "Failed to encrypt set-user-info command.");
        return FALSE;
    }

    GByteArray *v2payload = g_byte_array_new();
    uint8_t chan = 1;
    uint8_t op = 2;
    g_byte_array_append(v2payload, &chan, 1);
    g_byte_array_append(v2payload, &op, 1);
    g_byte_array_append(v2payload, encrypted, encrypted_len);

    gboolean ok = send_spp_v2_packet(state, 3, state->spp_v2_seq++, v2payload->data, v2payload->len);
    append_log(state, ok ? "Sent user info (GB setUserInfo)." : "Failed to send user info.");

    g_free(encrypted);
    g_byte_array_unref(v2payload);
    g_byte_array_unref(cmd);
    g_byte_array_unref(health);
    return ok;
}

/*
 * Mirrors Gadgetbridge onAuthSuccess ordering:
 *   1) systemService.setCurrentTime() (if sync time in GB — we always send),
 *   2) XiaomiHealthService.initialize(): setUserInfo + health config GETs,
 *   3) XiaomiScheduleService.initialize() includes sleep mode GET (17, 8),
 *   4) XiaomiSystemService.initialize() starts with device info GET (2, 2), …
 * Then this app: realtime stats, battery/state, activity file list (same as before).
 */
void gb_run_post_auth_data_fetch(AppState *state) {
    if (!state->auth_initialized || state->sock_fd < 0) {
        return;
    }
    (void)send_gb_set_current_time(state);
    (void)send_gb_user_info(state);
    (void)send_encrypted_simple_command(state, 8, 8, "Requested SpO2 config");
    (void)send_encrypted_simple_command(state, 8, 10, "Requested heart rate config");
    (void)send_encrypted_simple_command(state, 8, 12, "Requested standing reminder config");
    (void)send_encrypted_simple_command(state, 8, 14, "Requested stress config");
    (void)send_encrypted_simple_command(state, 8, 21, "Requested goal notification config");
    (void)send_encrypted_simple_command(state, 8, 42, "Requested goals config");
    (void)send_encrypted_simple_command(state, 8, 35, "Requested vitality score config");
    (void)send_encrypted_simple_command(state, 17, 8, "Requested sleep mode schedule (schedule)");
    (void)send_encrypted_simple_command(state, 2, 2, "Requested device info (system)");
    (void)send_realtime_stats_start(state);
    (void)send_encrypted_simple_command(state, 2, 1, "Requested battery state");
    (void)send_encrypted_simple_command(state, 2, 78, "Requested device state");
    reset_activity_fetch_state(state);
    send_activity_fetch_today_channels(state, "Requested activity file IDs (today)");
    send_activity_fetch_past_channels(state, "Requested activity file IDs (past probe)");
    state->activity_refetch_round = 0;
    if (state->activity_refetch_source_id != 0) {
        g_source_remove(state->activity_refetch_source_id);
        state->activity_refetch_source_id = 0;
    }
    state->activity_refetch_source_id = g_timeout_add_seconds(6, auto_history_followup_cb, state);
    start_activity_live_poll(state);
}

void mib10_request_history_resync(AppState *state) {
    if (!state->auth_initialized) {
        append_log(state, "History fetch needs authenticated session.");
        return;
    }
    reset_activity_fetch_state(state);
    send_activity_fetch_today_channels(state, "Requested activity file IDs (today)");
    send_activity_fetch_past_channels(state, "Requested activity file IDs (past probe)");
    state->activity_refetch_round = 0;
    if (state->activity_refetch_source_id != 0) {
        g_source_remove(state->activity_refetch_source_id);
        state->activity_refetch_source_id = 0;
    }
    state->activity_refetch_source_id = g_timeout_add_seconds(6, auto_history_followup_cb, state);
    start_activity_live_poll(state);
}

static void handle_command_payload(AppState *state, const uint8_t *payload, size_t len) {
    ParsedAuthCommand parsed;
    if (!parse_auth_message(payload, len, &parsed)) {
        append_log(state, "Failed to parse protobuf command.");
        return;
    }

    if (!parsed.has_type || parsed.type != 1 || !parsed.has_subtype) {
        append_log(state, "Non-auth protobuf command received.");
        return;
    }

    if (parsed.subtype == 26 && parsed.has_watch_nonce) {
        append_log(state, "Received auth step 1 response (watch nonce).");
        (void)send_auth_step2(state, &parsed);
    } else if ((parsed.subtype == 27) || (parsed.subtype == 5)) {
        if (parsed.subtype == 27 || (parsed.has_auth_status && parsed.auth_status == 1)) {
            state->auth_initialized = (parsed.subtype == 27);
            append_log(state, state->auth_initialized ? "Auth success (encrypted session)." : "Auth success (plaintext).");
            set_status(state, "Authenticated and connected.");
            switch_to_dashboard(state);
            set_connect_busy(state, FALSE);
            if (state->auth_initialized) {
                gb_run_post_auth_data_fetch(state);
            }
        } else {
            append_log(state, "Auth failed.");
            set_status(state, "Authentication failed.");
            set_connect_busy(state, FALSE);
        }
    } else {
        gchar *line = g_strdup_printf("Unhandled auth subtype %u", parsed.subtype);
        append_log(state, line);
        g_free(line);
    }
}

static gboolean process_v2_packet(AppState *state, const uint8_t *packet, size_t packet_len) {
    if (packet_len < 8) return FALSE;
    uint8_t packet_type = packet[2] & 0x0F;
    uint8_t seq = packet[3];
    uint16_t payload_len = (uint16_t)packet[4] | ((uint16_t)packet[5] << 8);
    if (packet_len != (size_t)(8 + payload_len)) return FALSE;
    const uint8_t *payload = packet + 8;

    if (packet_type == 2) {
        append_log(state, "Received SPPv2 session config response.");
        return send_auth_step1_nonce_command(state);
    }

    if (packet_type == 3) {
        if (!send_spp_v2_ack(state, seq)) {
            return FALSE;
        }
        if (payload_len < 2) {
            append_log(state, "Invalid SPPv2 data payload.");
            return FALSE;
        }
        uint8_t raw_channel = payload[0] & 0x0F;
        uint8_t op_code = payload[1];
        const uint8_t *data = payload + 2;
        size_t data_len = payload_len - 2;

        if (raw_channel == 1) {
            if (op_code == 1) {
                handle_command_payload(state, data, data_len);
            } else {
                if (!state->auth_initialized) {
                    append_log(state, "Encrypted protobuf packet received before auth init.");
                } else {
                    uint8_t *plain = NULL;
                    size_t plain_len = 0;
                    /* Gadgetbridge V2 quirk: decrypt key and IV are both decryptionKey. */
                    if (!aes_ctr_crypt(state->decryption_key, state->decryption_key, data, data_len, &plain, &plain_len)) {
                        append_log(state, "Failed to decrypt encrypted protobuf packet.");
                    } else {
                        if (state->verbose_wire_log) {
                            gchar *hex = format_hex(plain, plain_len);
                            gchar *line = g_strdup_printf("DEC %s", hex);
                            append_log(state, line);
                            g_free(line);
                            g_free(hex);
                        }

                        log_decoded_command_proto(state, plain, plain_len);
                        g_free(plain);
                    }
                }
            }
        } else if (raw_channel == 5) {
            if (op_code == 2 && state->auth_initialized) {
                uint8_t *plain = NULL;
                size_t plain_len = 0;
                if (aes_ctr_crypt(state->decryption_key, state->decryption_key, data, data_len, &plain, &plain_len)) {
                    if (plain_len >= 4) {
                        const uint16_t total = mib10_read_u16_le(plain);
                        const uint16_t num = mib10_read_u16_le(plain + 2);
                        if (state->verbose_fetch_log) {
                            gchar *line = g_strdup_printf("ACT chunk %u/%u (%zu bytes)", num, total, plain_len);
                            append_log(state, line);
                            g_free(line);
                        }

                        if (num == 1) {
                            if (state->activity_chunk_accumulator != NULL) {
                                g_byte_array_set_size(state->activity_chunk_accumulator, 0);
                            } else {
                                state->activity_chunk_accumulator = g_byte_array_new();
                            }
                        } else if (state->activity_chunk_accumulator == NULL) {
                            state->activity_chunk_accumulator = g_byte_array_new();
                        }

                        if (plain_len > 4) {
                            g_byte_array_append(state->activity_chunk_accumulator, plain + 4, (guint)(plain_len - 4));
                        }

                        if (total > 0 && num == total) {
                            handle_activity_payload(state,
                                                    state->activity_chunk_accumulator->data,
                                                    state->activity_chunk_accumulator->len);
                            g_byte_array_set_size(state->activity_chunk_accumulator, 0);
                        }
                    } else {
                        append_log(state, "Activity chunk too short.");
                    }
                    g_free(plain);
                } else {
                    append_log(state, "Failed to decrypt activity chunk.");
                }
            } else if (state->verbose_wire_log) {
                append_log(state, "Activity channel packet received.");
            }
        } else if (state->verbose_wire_log) {
            append_log(state, "SPPv2 non-protobuf channel packet received.");
        }
        return TRUE;
    }

    if (packet_type == 1) {
        if (state->verbose_wire_log) {
            append_log(state, "Received SPPv2 ACK.");
        }
        return TRUE;
    }

    return TRUE;
}

static gboolean process_v1_packet(AppState *state, const uint8_t *packet, size_t packet_len) {
    if (packet_len < 11) return FALSE;
    uint16_t payload_sz = (uint16_t)packet[5] | ((uint16_t)packet[6] << 8);
    if (packet_len != (size_t)(payload_sz + 8)) return FALSE;
    if (packet[packet_len - 1] != 0xEF) return FALSE;

    uint8_t channel = packet[3] & 0x0F;
    uint8_t opcode = packet[7];
    const uint8_t *payload = packet + 10;
    size_t payload_len = payload_sz - 3;

    if (channel == 0 && opcode == 1 && payload_len > 0) {
        state->protocol_version = payload[0];
        gchar *line = g_strdup_printf("SPP version response: %u", state->protocol_version);
        append_log(state, line);
        g_free(line);
        if (state->protocol_version >= 2) {
            append_log(state, "Switching to SPPv2 and starting session.");
            state->spp_v2_seq = 0;
            return send_spp_v2_session_config(state);
        }
    }
    return TRUE;
}

static void process_rx_buffer(AppState *state) {
    while (state->rx_accumulator != NULL && state->rx_accumulator->len > 0) {
        uint8_t *buf = state->rx_accumulator->data;
        size_t len = state->rx_accumulator->len;

        if (len >= 3 && buf[0] == 0xBA && buf[1] == 0xDC && buf[2] == 0xFE) {
            if (len < 11) return;
            uint16_t payload_sz = (uint16_t)buf[5] | ((uint16_t)buf[6] << 8);
            size_t pkt_sz = payload_sz + 8;
            if (len < pkt_sz) return;
            (void)process_v1_packet(state, buf, pkt_sz);
            g_byte_array_remove_range(state->rx_accumulator, 0, pkt_sz);
            continue;
        }

        if (len >= 2 && buf[0] == 0xA5 && buf[1] == 0xA5) {
            if (len < 8) return;
            uint16_t payload_len = (uint16_t)buf[4] | ((uint16_t)buf[5] << 8);
            size_t pkt_sz = 8 + payload_len;
            if (len < pkt_sz) return;
            uint16_t got_crc = (uint16_t)buf[6] | ((uint16_t)buf[7] << 8);
            uint16_t calc_crc = crc16_arc(buf + 8, payload_len);
            if (got_crc != calc_crc) {
                append_log(state, "SPPv2 packet CRC mismatch, dropping byte.");
                g_byte_array_remove_range(state->rx_accumulator, 0, 1);
                continue;
            }
            (void)process_v2_packet(state, buf, pkt_sz);
            g_byte_array_remove_range(state->rx_accumulator, 0, pkt_sz);
            continue;
        }

        /* Resync to potential next packet preamble. */
        g_byte_array_remove_range(state->rx_accumulator, 0, 1);
    }
}

static gboolean try_connect_rfcomm(AppState *state, const bdaddr_t *bdaddr, int channel) {
    struct sockaddr_rc addr = {0};
    addr.rc_family = AF_BLUETOOTH;
    addr.rc_channel = (uint8_t)channel;
    bacpy(&addr.rc_bdaddr, bdaddr);

    state->sock_fd = socket(AF_BLUETOOTH, SOCK_STREAM, BTPROTO_RFCOMM);
    if (state->sock_fd < 0) {
        return FALSE;
    }

    if (connect(state->sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(state->sock_fd);
        state->sock_fd = -1;
        return FALSE;
    }

    return TRUE;
}

static gboolean send_xiaomi_version_request(AppState *state) {
    /* Matches Gadgetbridge's XiaomiSppSupport initializeDevice() first request. */
    const uint8_t packet[] = {
        0xBA, 0xDC, 0xFE, /* preamble */
        0x00,             /* channel Version */
        0xC0,             /* flags: flag + needsResponse */
        0x03, 0x00,       /* payload header size (opcode+serial+datatype) */
        0x00,             /* opcode READ */
        0x00,             /* frame serial */
        0x00,             /* data type PLAIN */
        0xEF              /* epilogue */
    };

    if (write_all(state->sock_fd, packet, sizeof(packet)) < 0) {
        gchar *line = g_strdup_printf("Write failed: %s", g_strerror(errno));
        append_log(state, line);
        g_free(line);
        return FALSE;
    }

    append_log(state, "Sent Xiaomi SPP V1 version request.");
    return TRUE;
}

static gpointer reader_thread_main(gpointer user_data) {
    AppState *state = user_data;
    uint8_t buffer[1024];

    while (state->reader_running) {
        ssize_t n = read(state->sock_fd, buffer, sizeof(buffer));
        if (n > 0) {
            if (state->verbose_wire_log) {
                gchar *hex = format_hex(buffer, (size_t)n);
                gchar *line = g_strdup_printf("RX %s", hex);
                append_log(state, line);
                g_free(line);

                if (looks_printable(buffer, (size_t)n)) {
                    gchar *txt = g_strndup((const gchar *)buffer, (gsize)n);
                    gchar *txt_line = g_strdup_printf("TXT %s", txt);
                    append_log(state, txt_line);
                    g_free(txt_line);
                    g_free(txt);
                }
                g_free(hex);
            }

            if (state->rx_accumulator == NULL) {
                state->rx_accumulator = g_byte_array_new();
            }
            g_byte_array_append(state->rx_accumulator, buffer, (guint)n);
            process_rx_buffer(state);
        } else if (n == 0) {
            append_log(state, "Socket closed by remote.");
            break;
        } else if (errno == EINTR) {
            continue;
        } else {
            gchar *line = g_strdup_printf("Read error: %s", g_strerror(errno));
            append_log(state, line);
            g_free(line);
            break;
        }
    }

    set_status(state, "Disconnected");
    switch_to_pairing(state);
    set_connect_busy(state, FALSE);
    return NULL;
}

gboolean parse_mac(const gchar *input, bdaddr_t *out_bdaddr, gchar **normalized) {
    if (input == NULL || strlen(input) != 17) {
        return FALSE;
    }

    gchar *upper = g_ascii_strup(input, -1);
    for (gchar *c = upper; *c; c++) {
        if (!(g_ascii_isxdigit(*c) || *c == ':')) {
            g_free(upper);
            return FALSE;
        }
    }

    if (str2ba(upper, out_bdaddr) < 0) {
        g_free(upper);
        return FALSE;
    }

    *normalized = upper;
    return TRUE;
}

void disconnect_socket(AppState *state) {
    state->reader_running = FALSE;
    stop_activity_live_poll(state);

    if (state->sock_fd >= 0) {
        shutdown(state->sock_fd, SHUT_RDWR);
    }
    if (state->reader_thread != NULL) {
        g_thread_join(state->reader_thread);
        state->reader_thread = NULL;
    }
    if (state->sock_fd >= 0) {
        close(state->sock_fd);
        state->sock_fd = -1;
    }
    if (state->rx_accumulator != NULL) {
        g_byte_array_unref(state->rx_accumulator);
        state->rx_accumulator = NULL;
    }
    if (state->activity_chunk_accumulator != NULL) {
        g_byte_array_unref(state->activity_chunk_accumulator);
        state->activity_chunk_accumulator = NULL;
    }
    state->auth_initialized = FALSE;
    state->protocol_version = 0;
    state->spp_v2_seq = 0;
}

gpointer connect_worker_main(gpointer user_data) {
    ConnectTask *task = user_data;
    AppState *state = task->state;
    gchar *normalized = task->normalized;
    const bdaddr_t *bdaddr = &task->bdaddr;

    set_status(state, "Pairing/connecting through BlueZ...");
    if (!bluez_pair_trust_connect(state, normalized)) {
        append_log(state, "BlueZ Pair/Trust/Connect did not fully succeed (continuing with RFCOMM attempts).");
    }

    set_status(state, "Discovering SPP channel via SDP...");
    int channel = discover_spp_channel(normalized);

    if (channel > 0) {
        gchar *log_line = g_strdup_printf("SDP found SPP RFCOMM channel %d. Connecting...", channel);
        append_log(state, log_line);
        g_free(log_line);

        if (!try_connect_rfcomm(state, bdaddr, channel)) {
            gchar *err = g_strdup_printf("RFCOMM connect failed on channel %d: %s", channel, g_strerror(errno));
            append_log(state, err);
            g_free(err);
        }
    } else {
        append_log(state, "SPP SDP lookup failed; trying RFCOMM channels 1..30.");
    }

    if (state->sock_fd < 0) {
        for (int ch = 1; ch <= 30 && state->sock_fd < 0; ch++) {
            if (channel > 0 && ch == channel) {
                continue;
            }
            gchar *log_line = g_strdup_printf("Connecting RFCOMM channel %d...", ch);
            append_log(state, log_line);
            g_free(log_line);

            if (try_connect_rfcomm(state, bdaddr, ch)) {
                channel = ch;
                break;
            }
        }
    }

    if (state->sock_fd < 0) {
        gchar *err = g_strdup_printf("All RFCOMM channel attempts failed (last errno: %s).", g_strerror(errno));
        append_log(state, err);
        g_free(err);
        set_status(state, "Connect failed.");
        set_connect_busy(state, FALSE);
        g_free(task->normalized);
        g_free(task);
        return NULL;
    }

    set_status(state, "Connected. Sending version request...");
    memset(state->phone_nonce, 0, sizeof(state->phone_nonce));
    memset(state->watch_nonce, 0, sizeof(state->watch_nonce));
    memset(state->decryption_key, 0, sizeof(state->decryption_key));
    memset(state->encryption_key, 0, sizeof(state->encryption_key));
    memset(state->decryption_nonce, 0, sizeof(state->decryption_nonce));
    memset(state->encryption_nonce, 0, sizeof(state->encryption_nonce));
    state->auth_initialized = FALSE;
    state->protocol_version = 0;
    state->spp_v2_seq = 0;
    state->sleep_hint_logged = FALSE;
    reset_activity_fetch_state(state);
    if (state->activity_chunk_accumulator != NULL) {
        g_byte_array_set_size(state->activity_chunk_accumulator, 0);
    }
    if (!send_xiaomi_version_request(state)) {
        set_status(state, "Connected, but first write failed.");
    } else {
        set_status(state, "Connected. Listening for SPP packets...");
    }

    state->reader_running = TRUE;
    state->reader_thread = g_thread_new("spp-reader", reader_thread_main, state);

    g_free(normalized);
    g_free(task);
    return NULL;
}
