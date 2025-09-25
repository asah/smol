#!/usr/bin/env python3

import os
import sys

try:
    from slack_sdk.webhook import WebhookClient
except Exception as e:
    sys.stderr.write(f"notify: missing slack_sdk? {e}
")
    sys.exit(0)

slack_url = os.environ.get("CODEX_NOTIFY_SLACK_WEBHOOK")
if not slack_url:
    sys.stderr.write("notify: CODEX_NOTIFY_SLACK_WEBHOOK not set; skipping.
")
    sys.exit(0)

webhook = WebhookClient(slack_url)
try:
    resp = webhook.send(text="Codex: ready for input")
    print(resp.status_code)
    print(resp.body)
except Exception as e:
    sys.stderr.write(f"notify: send failed: {e}
")
    sys.exit(0)
