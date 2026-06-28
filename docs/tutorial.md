# Tutorial: Getting Started with OpenClaw Bridge

This tutorial walks you through setting up the bridge, connecting to a real IM backend, and sending/receiving messages via WebSocket.

## Prerequisites

- Node.js 18+ (ES2022 support required)
- An IM platform account with API credentials (e.g., liangzimixin appId/appSecret)

## Step 1: Install the Bridge

```bash
git clone <repo-url> openclaw-bridge
cd openclaw-bridge
npm install
```

## Step 2: Install Channel Plugins

Install the openclaw channel plugins for the IM platforms you want to bridge:

```bash
# For liangzimixin (quantum IM)
npm install liangzimixin

# For WeChat (Weixin)
npm install @tencent-weixin/openclaw-weixin

# Any other openclaw channel plugin works too
npm install <any-openclaw-channel-plugin>
```

The bridge discovers plugins automatically — no configuration needed to tell it which plugins are installed.

## Step 3: Create Your Configuration

Copy the example config:

```bash
cp config.example.json config.json
```

### Minimal config for liangzimixin

Edit `config.json`:

```json
{
  "server": {
    "port": 9300,
    "path": "/bridge"
  },
  "channels": {
    "liangzimixin": {
      "enabled": true,
      "accounts": {
        "default": {
          "appId": "YOUR_APP_ID",
          "appSecret": "YOUR_APP_SECRET",
          "env": "production",
          "encryptionMode": "quantum_and_plain"
        }
      }
    }
  },
  "logging": {
    "level": "info"
  }
}
```

Replace `YOUR_APP_ID` and `YOUR_APP_SECRET` with your actual credentials.

### Multi-account config

You can run multiple accounts on the same channel:

```json
{
  "channels": {
    "liangzimixin": {
      "enabled": true,
      "accounts": {
        "work": {
          "appId": "WORK_APP_ID",
          "appSecret": "WORK_APP_SECRET"
        },
        "personal": {
          "appId": "PERSONAL_APP_ID",
          "appSecret": "PERSONAL_APP_SECRET"
        }
      }
    }
  }
}
```

### Multi-channel config

Bridge to multiple IM platforms simultaneously:

```json
{
  "channels": {
    "liangzimixin": {
      "enabled": true,
      "accounts": {
        "default": {
          "appId": "YOUR_LIANGZIMIXIN_APP_ID",
          "appSecret": "YOUR_LIANGZIMIXIN_APP_SECRET"
        }
      }
    },
    "openclaw-weixin": {
      "enabled": true,
      "accounts": {
        "primary": {
          "token": "YOUR_WEIXIN_BOT_TOKEN",
          "baseUrl": "https://ilinkai.weixin.qq.com",
          "cdnBaseUrl": "https://novac2c.cdn.weixin.qq.com/c2c"
        }
      }
    }
  }
}
```

### Disabling a channel

Set `"enabled": false` to skip a channel without removing its config:

```json
{
  "channels": {
    "liangzimixin": {
      "enabled": false,
      "accounts": { ... }
    }
  }
}
```

## Step 4: Build and Start the Server

```bash
npm run build
npm start
```

You should see output like:

```
[2026-06-25T10:00:00.000Z] [INFO] [bridge/main] Starting OpenClaw Bridge Server {"version":"1.0.0"}
[2026-06-25T10:00:00.100Z] [INFO] [bridge/channel-manager] Discovered plugin {"id":"liangzimixin","channels":["liangzimixin"],"pkgPath":"..."}
[2026-06-25T10:00:00.200Z] [INFO] [bridge/plugin-loader] Loading plugin entry point {"pluginId":"liangzimixin","entryPath":"..."}
[2026-06-25T10:00:00.300Z] [INFO] [bridge/plugin-loader] Found ChannelPlugin via direct export {"pluginId":"liangzimixin","channelId":"liangzimixin"}
[2026-06-25T10:00:00.400Z] [INFO] [bridge/server] Bridge server listening {"host":"0.0.0.0","port":9300,"path":"/bridge"}
[2026-06-25T10:00:00.500Z] [INFO] [bridge/openclaw-adapter] Calling gateway.startAccount() {"channelId":"liangzimixin","accountId":"default"}
[2026-06-25T10:00:01.000Z] [INFO] [bridge/main] Started channel account {"channelId":"liangzimixin","accountId":"default"}
[2026-06-25T10:00:01.100Z] [INFO] [bridge/main] Bridge server is ready
```

The server is now listening on `ws://0.0.0.0:9300/bridge`.

> **Tip:** Use `npm run dev` for watch mode during development — the server rebuilds automatically on file changes.

## Step 5: Connect with a WebSocket Client

### Using Node.js (ws library)

```javascript
import WebSocket from "ws";

const ws = new WebSocket("ws://localhost:9300/bridge");

ws.on("open", () => {
  console.log("Connected to bridge!");
});

ws.on("message", (data) => {
  const msg = JSON.parse(data.toString());
  console.log(`[${msg.type}]`, JSON.stringify(msg.payload, null, 2));
});

ws.on("close", (code, reason) => {
  console.log(`Disconnected: ${code} ${reason}`);
});
```

On connection, you'll receive a **welcome** message:

```json
{
  "v": 1,
  "id": "br-xxx-1",
  "type": "welcome",
  "channel": "*",
  "payload": {
    "version": "1.0.0",
    "channels": {
      "liangzimixin": {
        "status": "connected",
        "accounts": ["default"]
      }
    }
  },
  "ts": 1719400000000
}
```

### Using Python (websockets)

```python
import asyncio
import json
import websockets

async def main():
    async with websockets.connect("ws://localhost:9300/bridge") as ws:
        # Receive welcome
        msg = json.loads(await ws.recv())
        print(f"[{msg['type']}] {json.dumps(msg['payload'], indent=2)}")

        # List channels
        await ws.send(json.dumps({
            "v": 1, "id": "1", "type": "list_channels",
            "channel": "*", "payload": {}
        }))
        msg = json.loads(await ws.recv())
        print(f"[{msg['type']}] {json.dumps(msg['payload'], indent=2)}")

asyncio.run(main())
```

### Using wscat (CLI)

```bash
npx wscat -c ws://localhost:9300/bridge
```

Then type JSON messages directly:

```
> {"v":1,"id":"1","type":"list_channels","channel":"*","payload":{}}
< {"v":1,"id":"br-xxx","type":"channels_list","channel":"*","payload":{"channels":{...}},"ts":...}
```

## Step 6: List Available Channels

Send a `list_channels` message to see all channels, accounts, and their connection status:

```json
{
  "v": 1,
  "id": "list-1",
  "type": "list_channels",
  "channel": "*",
  "payload": {}
}
```

Response:

```json
{
  "v": 1,
  "id": "list-1",
  "type": "channels_list",
  "channel": "*",
  "payload": {
    "channels": {
      "liangzimixin": {
        "label": "量子密信",
        "accounts": {
          "default": {
            "status": "connected",
            "detail": "Plugin gateway started"
          }
        }
      }
    }
  },
  "ts": 1719400000000
}
```

## Step 7: Subscribe to Inbound Messages

Before you can receive messages from a channel, you must subscribe:

```json
{
  "v": 1,
  "id": "sub-1",
  "type": "subscribe",
  "channel": "liangzimixin",
  "payload": {
    "channel": "liangzimixin",
    "accountId": "default"
  }
}
```

You'll receive a `channel_status` response confirming the subscription:

```json
{
  "v": 1,
  "id": "br-xxx",
  "type": "channel_status",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "status": "connected",
    "detail": "Plugin gateway started"
  },
  "ts": 1719400000000
}
```

### Subscribe with sender filter

Only receive messages from specific users:

```json
{
  "v": 1,
  "id": "sub-2",
  "type": "subscribe",
  "channel": "liangzimixin",
  "payload": {
    "channel": "liangzimixin",
    "accountId": "default",
    "filter": {
      "fromUserIds": ["user-alice", "user-bob"]
    }
  }
}
```

## Step 8: Send Messages

### Send a text message

```json
{
  "v": 1,
  "id": "send-1",
  "type": "send_text",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "to": "recipient-user-id",
    "text": "Hello from the bridge!"
  }
}
```

On success, you'll receive a `send_ack`:

```json
{
  "v": 1,
  "id": "send-1",
  "type": "send_ack",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "requestId": "send-1",
    "messageId": "msg-abc123"
  },
  "ts": 1719400000000
}
```

On failure, you'll receive a `send_error`:

```json
{
  "v": 1,
  "id": "send-1",
  "type": "send_error",
  "channel": "liangzimixin",
  "payload": {
    "requestId": "send-1",
    "code": "send_failed",
    "message": "Recipient not found"
  },
  "ts": 1719400000000
}
```

### Send a media message

```json
{
  "v": 1,
  "id": "send-2",
  "type": "send_media",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "to": "recipient-user-id",
    "mediaUrl": "https://example.com/image.png",
    "text": "Check this out!",
    "mediaType": "image/png"
  }
}
```

### Send a typing indicator

```json
{
  "v": 1,
  "id": "send-3",
  "type": "send_typing",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "to": "recipient-user-id",
    "typing": true
  }
}
```

## Step 9: Receive Inbound Messages

Once subscribed, inbound messages from the backend arrive as `inbound_message`:

```json
{
  "v": 1,
  "id": "br-xxx",
  "type": "inbound_message",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "messageId": "msg-xyz789",
    "chatId": "chat-123",
    "senderId": "user-alice",
    "senderName": "Alice",
    "msgType": "text",
    "text": "Hey, how are you?",
    "timestamp": 1719400000000,
    "wasEncrypted": true,
    "raw": { ... }
  },
  "ts": 1719400000000
}
```

**The sender is automatically recorded.** When an inbound message arrives, the bridge persists the sender's user ID to `contacts.json`. This enables the online notification feature on restart.

### Media inbound messages

```json
{
  "v": 1,
  "id": "br-xxx",
  "type": "inbound_message",
  "channel": "liangzimixin",
  "accountId": "default",
  "payload": {
    "messageId": "msg-abc456",
    "chatId": "chat-123",
    "senderId": "user-bob",
    "senderName": "Bob",
    "msgType": "media",
    "text": "",
    "timestamp": 1719400000000,
    "mediaUrl": "https://cdn.example.com/photo.jpg",
    "mediaType": "image",
    "raw": { ... }
  },
  "ts": 1719400000000
}
```

## Step 10: Contact Persistence & Online Notifications

### How contacts are recorded

Every time an inbound message arrives, the bridge automatically records the sender to `contacts.json` (stored alongside `config.json`). The file looks like:

```json
{
  "contacts": {
    "liangzimixin:default:3c641fe588c14faf90802e72a09c1a44": {
      "userId": "3c641fe588c14faf90802e72a09c1a44",
      "channel": "liangzimixin",
      "accountId": "default",
      "displayName": "Alice",
      "firstSeenAt": 1719400000000,
      "lastSeenAt": 1719400000000
    }
  }
}
```

Contacts are deduplicated by `channel:accountId:userId` — if the same user sends another message, only `lastSeenAt` is updated.

### Online notifications on restart

When the server restarts, it loads `contacts.json` and sends an online notification to every persisted contact:

```
[INFO] Loaded contacts {"count":1,"path":"/path/to/contacts.json"}
[INFO] Sending online notifications {"channel":"liangzimixin","accountId":"default","contactCount":1}
[INFO] Online notification sent {"channel":"liangzimixin","accountId":"default","userId":"3c641fe588c14faf90802e72a09c1a44"}
```

Each contact receives the message: **🟢 Bridge is back online**

This means your contacts will know immediately when the bridge is available again after a restart.

### Resetting contacts

To clear all contacts, simply delete `contacts.json`:

```bash
rm contacts.json
```

The file will be recreated automatically when new inbound messages arrive.

## Step 11: Unsubscribe

Stop receiving messages from a channel account:

```json
{
  "v": 1,
  "id": "unsub-1",
  "type": "unsubscribe",
  "channel": "liangzimixin",
  "payload": {
    "channel": "liangzimixin",
    "accountId": "default"
  }
}
```

## Step 12: Keep-Alive (Ping/Pong)

The server sends periodic ping frames. If you're using a WS library that auto-responds to pings (most do), no action is needed. Otherwise, you can send explicit pings:

```json
{
  "v": 1,
  "id": "ping-1",
  "type": "ping",
  "channel": "*",
  "payload": {}
}
```

Response:

```json
{
  "v": 1,
  "id": "ping-1",
  "type": "pong",
  "channel": "*",
  "payload": {},
  "ts": 1719400000000
}
```

Clients that don't respond to pings within 2× `clientHeartbeatMs` (default: 60 seconds) are terminated.

## Step 13: QR Code Login (WeChat and other QR-auth plugins)

Some channel plugins (e.g., WeChat) require scanning a QR code to authenticate. The bridge provides both HTTP and WebSocket interfaces for QR login.

### HTTP: Browser-friendly QR page

Open this URL in a browser to display the QR code:

```
http://localhost:9300/plugin/openclaw-weixin/default/qr
```

The page shows the QR code image and automatically polls for login status. Once you scan the QR code with your phone and confirm, the page updates to show "Connected!" and the account is automatically started.

### HTTP: Programmatic JSON API

For custom UIs or scripts:

```bash
# Start QR login — returns QR data URL and session key
curl http://localhost:9300/plugin/openclaw-weixin/default/qr/json

# Poll for status (use sessionKey from the start response)
curl "http://localhost:9300/plugin/openclaw-weixin/default/qr/status?sessionKey=YOUR_SESSION_KEY"
```

### WebSocket: QR login via WS protocol

If you're already connected via WebSocket, use the WS protocol:

```json
// Step 1: Start QR login
{
  "v": 1,
  "id": "qr-1",
  "type": "qr_start",
  "channel": "openclaw-weixin",
  "accountId": "default",
  "payload": { "accountId": "default" }
}

// Response: qr_result with QR data URL and session key
{
  "v": 1,
  "id": "br-xxx",
  "type": "qr_result",
  "channel": "openclaw-weixin",
  "accountId": "default",
  "payload": {
    "qrDataUrl": "data:image/png;base64,...",
    "message": "Scan QR code to login",
    "sessionKey": "abc-123"
  }
}

// Step 2: Wait for scan (long-poll)
{
  "v": 1,
  "id": "qr-2",
  "type": "qr_wait",
  "channel": "openclaw-weixin",
  "accountId": "default",
  "payload": { "sessionKey": "abc-123" }
}

// Response: connected!
{
  "v": 1,
  "id": "br-xxx",
  "type": "qr_result",
  "channel": "openclaw-weixin",
  "accountId": "default",
  "payload": {
    "connected": true,
    "message": "Login successful",
    "accountId": "bot-123456"
  }
}
```

After a successful QR login, the bridge automatically starts the account — no restart needed.

### First-time WeChat setup

1. Install the WeChat plugin: `npm install @tencent-weixin/openclaw-weixin`
2. Add a placeholder config in `config.json`:
   ```json
   {
     "channels": {
       "openclaw-weixin": {
         "enabled": true,
         "accounts": {
           "default": {}
         }
       }
     }
   }
   ```
3. Start the server: `npm start`
4. Open `http://localhost:9300/plugin/openclaw-weixin/default/qr` in a browser
5. Scan the QR code with WeChat
6. The account is automatically configured and started

## Running the E2E Test

The project includes an end-to-end test that verifies the full message round-trip:

```bash
npm run build
npm run test:e2e
```

The test:
1. Starts the server with your `config.json`
2. Connects a WS client
3. Lists channels and verifies connected status
4. Sends 3 messages and waits for ack/error responses

**Prerequisites:** A valid `config.json` with real credentials for at least one channel.

The IRC and Mattermost channels also have self-contained, credential-free E2E
suites (`npm run test:e2e:irc`, `npm run test:e2e:mattermost`) that spin up a
real IM server and exchange 3 messages round-trip — no `config.json` needed.
For Mattermost, provision the bot + sender accounts first with
`scripts/provision-mattermost.sh` against a running Mattermost server. Both
suites run automatically in GitHub Actions.

## Troubleshooting

### "No channel plugins discovered"

The bridge couldn't find any `openclaw.plugin.json` manifests in `node_modules/`. Make sure you've installed at least one channel plugin:

```bash
npm install liangzimixin
```

### "Channel configured but no adapter available"

You have a channel in `config.json` but its plugin isn't installed. Either install the plugin or set `"enabled": false`.

### "PluginRuntime not initialized"

This happens when a plugin's `register()` function was never called. The bridge handles this automatically for both export patterns. If you see this, check that the plugin's `openclaw.plugin.json` points to the correct entry point.

### Account stays in "reconnecting" status

The plugin's `gateway.startAccount()` may have failed. Check the server logs for errors. Common causes:
- Invalid credentials
- Network connectivity issues to the backend
- Backend service is down
- For WeChat: the account may need QR login first (see "Account needs setup" log message)

### "Plugin does not support QR login"

The plugin's gateway adapter doesn't implement `loginWithQrStart()` or `loginWithQrWait()`. Not all plugins use QR code authentication. Check the plugin's documentation for its setup method.

### QR code fetch fails

The bridge server couldn't reach the plugin's backend to fetch the QR code. Common causes:
- Network connectivity issues (e.g., `ilinkai.weixin.qq.com` unreachable)
- Firewall blocking outbound HTTPS requests
- Backend service is down

### Client not receiving inbound messages

Make sure you've subscribed to the correct `channel:accountId` pair. The subscription key format is `"channelId:accountId"`. Check that:
1. You sent a `subscribe` message
2. The `channel` and `accountId` match what `list_channels` reports
3. If using sender filters, the sender ID matches your filter

### Custom config path

```bash
node dist/server.js --config /path/to/my-config.json
```

### Debug logging

Set `"logging": { "level": "debug" }` in `config.json` for verbose output showing all plugin interactions, message routing, and subscription changes.
