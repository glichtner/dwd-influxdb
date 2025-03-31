#!/bin/bash

set -e

SERVICE_NAME="dwd-tracking"
TEMPLATE_DIR="systemd"
SYSTEMD_DIR="/etc/systemd/system"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="$(which python3)"

echo "[*] Using working dir: $SCRIPT_DIR"
echo "[*] Using Python: $PYTHON_BIN"

# Generate service file from template
SERVICE_TEMPLATE="$SCRIPT_DIR/$TEMPLATE_DIR/${SERVICE_NAME}.service.template"
SERVICE_FILE="/tmp/${SERVICE_NAME}.service"

sed "s|{{WORKDIR}}|$SCRIPT_DIR|g; s|{{PYTHON}}|$PYTHON_BIN|g" "$SERVICE_TEMPLATE" > "$SERVICE_FILE"

# Copy service + timer to systemd
sudo cp "$SERVICE_FILE" "$SYSTEMD_DIR/${SERVICE_NAME}.service"
sudo cp "$SCRIPT_DIR/$TEMPLATE_DIR/${SERVICE_NAME}.timer" "$SYSTEMD_DIR/${SERVICE_NAME}.timer"

# Install dependencies
echo "[*] Installing Python dependencies..."
pip3 install --user -r "$SCRIPT_DIR/requirements.txt"

# Enable and start timer
echo "[*] Enabling systemd timer..."
sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.timer"

echo "[✓] Installed and running: ${SERVICE_NAME}.timer"
