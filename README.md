# Mi Band 10 GTK Viewer (Linux)

Minimal GTK4 + libadwaita desktop app in C that:

- Connects to Xiaomi band through **BT Classic RFCOMM (SPP)** like Gadgetbridge Mi Band 10
- Discovers SPP channel via SDP for UUID `1101` (with channel 1 fallback)
- Sends the same initial Xiaomi SPP v1 version-request frame used by Gadgetbridge
- Prints incoming bytes in the UI (hex + printable text when possible)

This is intentionally basic: no auth/decryption, no protobuf decoding, no activity parsing.

## Build

Install build dependencies (names vary by distro):

- `cmake`
- `gcc` or `clang`
- `pkg-config`
- `gtk4`
- `libadwaita-1`
- `glib2`

Then:

```bash
cd tools/mi-band10-gtk
cmake -S . -B build
cmake --build build
```

## Run

```bash
./tools/mi-band10-gtk/build/mi-band10-viewer
```

In the app:

1. Click **Refresh** to populate the BlueZ device list
2. Select your band from the list
3. Click **Connect**
4. Watch log output

## Notes

- The device should be paired/trusted first.
- You may need proper Bluetooth permissions (or root for testing).
- Data may be encrypted binary frames depending on session/auth state.
- This app now follows Gadgetbridge's Mi Band 10 transport choice (BT Classic SPP), not BLE GATT.
