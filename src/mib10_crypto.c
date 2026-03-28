#include "mib10_app.h"

#include <string.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
gchar *format_hex(const uint8_t *data, size_t len) {
    GString *s = g_string_new(NULL);
    for (size_t i = 0; i < len; i++) {
        g_string_append_printf(s, "%02X", data[i]);
        if (i + 1 < len) {
            g_string_append_c(s, ' ');
        }
    }
    return g_string_free(s, FALSE);
}

gboolean looks_printable(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (!g_ascii_isprint((gchar)data[i]) && data[i] != '\n' && data[i] != '\r' && data[i] != '\t') {
            return FALSE;
        }
    }
    return TRUE;
}
gboolean hex_to_bytes16(const char *hex, uint8_t out[16]) {
    if (hex == NULL || strlen(hex) != 32) {
        return FALSE;
    }
    for (size_t i = 0; i < 16; i++) {
        unsigned int v = 0;
        if (sscanf(hex + (i * 2), "%2x", &v) != 1) {
            return FALSE;
        }
        out[i] = (uint8_t)v;
    }
    return TRUE;
}

uint16_t crc16_arc(const uint8_t *data, size_t len) {
    int crc = 0;
    for (size_t i = 0; i < len; i++) {
        const uint8_t b = data[i];
        for (int j = 0; j < 8; j++) {
            crc <<= 1;
            if ((((crc >> 16) & 1) ^ ((b >> j) & 1)) == 1) {
                crc ^= 0x8005;
            }
        }
    }
    uint32_t value = (uint32_t)crc;
    value = ((value & 0x55555555u) << 1) | ((value & 0xAAAAAAAAu) >> 1);
    value = ((value & 0x33333333u) << 2) | ((value & 0xCCCCCCCCu) >> 2);
    value = ((value & 0x0F0F0F0Fu) << 4) | ((value & 0xF0F0F0F0u) >> 4);
    value = ((value & 0x00FF00FFu) << 8) | ((value & 0xFF00FF00u) >> 8);
    value = (value << 16) | (value >> 16);
    return (uint16_t)(value >> 16);
}

gboolean hmac_sha256(const uint8_t *key, size_t key_len,
                            const uint8_t *data, size_t data_len,
                            uint8_t out[32]) {
    unsigned int out_len = 0;
    unsigned char *res = HMAC(EVP_sha256(), key, (int)key_len, data, data_len, out, &out_len);
    return res != NULL && out_len == 32;
}

gboolean compute_auth_step3_hmac(const uint8_t secret_key[16],
                                        const uint8_t phone_nonce[16],
                                        const uint8_t watch_nonce[16],
                                        uint8_t out64[64]) {
    uint8_t mac_key_seed[32];
    uint8_t seed_input[32];
    memcpy(seed_input, phone_nonce, 16);
    memcpy(seed_input + 16, watch_nonce, 16);
    if (!hmac_sha256(seed_input, sizeof(seed_input), secret_key, 16, mac_key_seed)) {
        return FALSE;
    }

    const uint8_t label[] = "miwear-auth";
    uint8_t t[32] = {0};
    size_t offset = 0;
    uint8_t counter = 1;

    while (offset < 64) {
        uint8_t input[64];
        size_t in_len = 0;
        if (offset != 0) {
            memcpy(input, t, 32);
            in_len += 32;
        }
        memcpy(input + in_len, label, sizeof(label) - 1);
        in_len += sizeof(label) - 1;
        input[in_len++] = counter++;

        if (!hmac_sha256(mac_key_seed, sizeof(mac_key_seed), input, in_len, t)) {
            return FALSE;
        }
        size_t copy = (64 - offset < sizeof(t)) ? (64 - offset) : sizeof(t);
        memcpy(out64 + offset, t, copy);
        offset += copy;
    }
    return TRUE;
}

gboolean aes_ccm_encrypt_4byte_tag(const uint8_t key[16],
                                          const uint8_t nonce12[12],
                                          const uint8_t *plaintext,
                                          size_t plaintext_len,
                                          uint8_t **out,
                                          size_t *out_len) {
    gboolean ok = FALSE;
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        return FALSE;
    }

    uint8_t *ciphertext = g_malloc(plaintext_len + 4);
    int len = 0;
    uint8_t tag[4] = {0};

    if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ccm(), NULL, NULL, NULL) != 1) goto done;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_IVLEN, 12, NULL) != 1) goto done;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_TAG, 4, NULL) != 1) goto done;
    if (EVP_EncryptInit_ex(ctx, NULL, NULL, key, nonce12) != 1) goto done;
    if (EVP_EncryptUpdate(ctx, NULL, &len, NULL, (int)plaintext_len) != 1) goto done;
    if (EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, (int)plaintext_len) != 1) goto done;
    if (EVP_EncryptFinal_ex(ctx, ciphertext + len, &len) != 1) goto done;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_GET_TAG, 4, tag) != 1) goto done;

    memcpy(ciphertext + plaintext_len, tag, 4);
    *out = ciphertext;
    *out_len = plaintext_len + 4;
    ok = TRUE;

done:
    if (!ok) {
        g_free(ciphertext);
    }
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

gboolean aes_ctr_crypt(const uint8_t key[16],
                              const uint8_t iv[16],
                              const uint8_t *input,
                              size_t input_len,
                              uint8_t **out,
                              size_t *out_len) {
    gboolean ok = FALSE;
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        return FALSE;
    }

    uint8_t *buf = g_malloc(input_len);
    int len = 0;
    int final_len = 0;
    if (EVP_DecryptInit_ex(ctx, EVP_aes_128_ctr(), NULL, key, iv) != 1) goto done;
    if (EVP_DecryptUpdate(ctx, buf, &len, input, (int)input_len) != 1) goto done;
    if (EVP_DecryptFinal_ex(ctx, buf + len, &final_len) != 1) goto done;
    *out = buf;
    *out_len = (size_t)(len + final_len);
    ok = TRUE;

done:
    if (!ok) g_free(buf);
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

gboolean aes_ctr_encrypt(const uint8_t key[16],
                                const uint8_t iv[16],
                                const uint8_t *input,
                                size_t input_len,
                                uint8_t **out,
                                size_t *out_len) {
    gboolean ok = FALSE;
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        return FALSE;
    }

    uint8_t *buf = g_malloc(input_len);
    int len = 0;
    int final_len = 0;
    if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ctr(), NULL, key, iv) != 1) goto done;
    if (EVP_EncryptUpdate(ctx, buf, &len, input, (int)input_len) != 1) goto done;
    if (EVP_EncryptFinal_ex(ctx, buf + len, &final_len) != 1) goto done;
    *out = buf;
    *out_len = (size_t)(len + final_len);
    ok = TRUE;

done:
    if (!ok) g_free(buf);
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

void pb_append_varint(GByteArray *arr, uint64_t value) {
    while (value >= 0x80) {
        uint8_t b = (uint8_t)(value & 0x7f) | 0x80;
        g_byte_array_append(arr, &b, 1);
        value >>= 7;
    }
    uint8_t b = (uint8_t)value;
    g_byte_array_append(arr, &b, 1);
}

void pb_append_tag(GByteArray *arr, uint32_t field, uint32_t wire_type) {
    pb_append_varint(arr, ((uint64_t)field << 3) | wire_type);
}

void pb_append_len_bytes(GByteArray *arr, uint32_t field, const uint8_t *data, size_t len) {
    pb_append_tag(arr, field, 2);
    pb_append_varint(arr, len);
    g_byte_array_append(arr, data, len);
}

void pb_append_u32(GByteArray *arr, uint32_t field, uint32_t value) {
    pb_append_tag(arr, field, 0);
    pb_append_varint(arr, value);
}

void pb_append_float(GByteArray *arr, uint32_t field, float value) {
    pb_append_tag(arr, field, 5);
    union { float f; uint32_t u; } cvt;
    cvt.f = value;
    uint8_t b[4] = {
        (uint8_t)(cvt.u & 0xff),
        (uint8_t)((cvt.u >> 8) & 0xff),
        (uint8_t)((cvt.u >> 16) & 0xff),
        (uint8_t)((cvt.u >> 24) & 0xff)
    };
    g_byte_array_append(arr, b, 4);
}

gboolean pb_read_varint(const uint8_t *buf, size_t len, size_t *idx, uint64_t *out) {
    uint64_t value = 0;
    int shift = 0;
    while (*idx < len && shift <= 63) {
        uint8_t b = buf[(*idx)++];
        value |= ((uint64_t)(b & 0x7f)) << shift;
        if ((b & 0x80) == 0) {
            *out = value;
            return TRUE;
        }
        shift += 7;
    }
    return FALSE;
}

gboolean pb_skip_field(const uint8_t *buf, size_t len, size_t *idx, uint32_t wire_type) {
    uint64_t n = 0;
    switch (wire_type) {
        case 0:
            return pb_read_varint(buf, len, idx, &n);
        case 1:
            if (*idx + 8 > len) return FALSE;
            *idx += 8;
            return TRUE;
        case 2:
            if (!pb_read_varint(buf, len, idx, &n)) return FALSE;
            if (*idx + n > len) return FALSE;
            *idx += (size_t)n;
            return TRUE;
        case 5:
            if (*idx + 4 > len) return FALSE;
            *idx += 4;
            return TRUE;
        default:
            return FALSE;
    }
}
void summarize_pb_message(const ProtobufCMessage *msg, GString *out, int depth);

static gboolean pb_field_present(const ProtobufCMessage *msg, const ProtobufCFieldDescriptor *f) {
    const char *base = (const char *)msg;
    if (f->label == PROTOBUF_C_LABEL_REQUIRED) {
        return TRUE;
    }
    if (f->label == PROTOBUF_C_LABEL_REPEATED) {
        const size_t count = *(const size_t *)(base + f->quantifier_offset);
        return count > 0;
    }

    if (f->quantifier_offset != 0) {
        return *(const protobuf_c_boolean *)(base + f->quantifier_offset) ? TRUE : FALSE;
    }

    if (f->type == PROTOBUF_C_TYPE_STRING || f->type == PROTOBUF_C_TYPE_MESSAGE) {
        const void *ptr = *(void * const *)(base + f->offset);
        return ptr != NULL;
    }
    if (f->type == PROTOBUF_C_TYPE_BYTES) {
        const ProtobufCBinaryData *bd = (const ProtobufCBinaryData *)(base + f->offset);
        return bd->len > 0 && bd->data != NULL;
    }
    return TRUE;
}

static void pb_append_scalar_value(GString *out, ProtobufCType type, const void *ptr) {
    switch (type) {
        case PROTOBUF_C_TYPE_BOOL:
            g_string_append_printf(out, "%s", (*(const protobuf_c_boolean *)ptr) ? "true" : "false");
            break;
        case PROTOBUF_C_TYPE_INT32:
        case PROTOBUF_C_TYPE_SINT32:
        case PROTOBUF_C_TYPE_SFIXED32:
            g_string_append_printf(out, "%d", *(const int32_t *)ptr);
            break;
        case PROTOBUF_C_TYPE_UINT32:
        case PROTOBUF_C_TYPE_FIXED32:
        case PROTOBUF_C_TYPE_ENUM:
            g_string_append_printf(out, "%u", *(const uint32_t *)ptr);
            break;
        case PROTOBUF_C_TYPE_INT64:
        case PROTOBUF_C_TYPE_SINT64:
        case PROTOBUF_C_TYPE_SFIXED64:
            g_string_append_printf(out, "%" G_GINT64_FORMAT, *(const int64_t *)ptr);
            break;
        case PROTOBUF_C_TYPE_UINT64:
        case PROTOBUF_C_TYPE_FIXED64:
            g_string_append_printf(out, "%" G_GUINT64_FORMAT, *(const uint64_t *)ptr);
            break;
        case PROTOBUF_C_TYPE_FLOAT:
            g_string_append_printf(out, "%.3f", *(const float *)ptr);
            break;
        case PROTOBUF_C_TYPE_DOUBLE:
            g_string_append_printf(out, "%.3f", *(const double *)ptr);
            break;
        case PROTOBUF_C_TYPE_STRING: {
            const char *s = *(char * const *)ptr;
            g_string_append_printf(out, "\"%s\"", s ? s : "");
            break;
        }
        case PROTOBUF_C_TYPE_BYTES: {
            const ProtobufCBinaryData *bd = (const ProtobufCBinaryData *)ptr;
            gchar *hex = format_hex(bd->data, bd->len < 16 ? bd->len : 16);
            g_string_append_printf(out, "bytes[%zu]=%s%s", bd->len, hex, bd->len > 16 ? " ..." : "");
            g_free(hex);
            break;
        }
        default:
            g_string_append(out, "<unsupported>");
            break;
    }
}

void summarize_pb_message(const ProtobufCMessage *msg, GString *out, int depth) {
    if (msg == NULL || msg->descriptor == NULL) {
        return;
    }

    const ProtobufCMessageDescriptor *d = msg->descriptor;
    g_string_append_printf(out, "%s{", d->short_name ? d->short_name : "Message");

    int shown = 0;
    for (unsigned i = 0; i < d->n_fields; i++) {
        const ProtobufCFieldDescriptor *f = &d->fields[i];
        if (!pb_field_present(msg, f)) {
            continue;
        }
        const char *base = (const char *)msg;
        if (shown++ > 0) {
            g_string_append(out, ", ");
        }
        g_string_append_printf(out, "%s=", f->name);

        if (f->label == PROTOBUF_C_LABEL_REPEATED) {
            const size_t count = *(const size_t *)(base + f->quantifier_offset);
            g_string_append_printf(out, "[%zu]", count);
            if (count > 0 && depth < 1) {
                g_string_append(out, "{");
                if (f->type == PROTOBUF_C_TYPE_MESSAGE) {
                    ProtobufCMessage * const *arr = *(ProtobufCMessage * const * const *)(base + f->offset);
                    summarize_pb_message(arr[0], out, depth + 1);
                } else {
                    const void *arr = *(void * const *)(base + f->offset);
                    pb_append_scalar_value(out, f->type, arr);
                }
                g_string_append(out, "}");
            }
            continue;
        }

        if (f->type == PROTOBUF_C_TYPE_MESSAGE) {
            ProtobufCMessage *sub = *(ProtobufCMessage * const *)(base + f->offset);
            if (depth < 1 && sub != NULL) {
                summarize_pb_message(sub, out, depth + 1);
            } else {
                g_string_append(out, "{...}");
            }
        } else {
            pb_append_scalar_value(out, f->type, base + f->offset);
        }
    }

    g_string_append(out, "}");
}
