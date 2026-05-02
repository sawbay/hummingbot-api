# Test Setup (Docker Flow)

The most reliable way to test the Hummingbot API is by running the entire stack in Docker. This ensures all dependencies and networking are correctly configured.

## 1. Build the API Image
Build the local image to ensure your latest code changes are included.
```bash
make build
```

## 2. Start All Services
Spin up the API, PostgreSQL, and EMQX (MQTT broker).
```bash
docker compose up -d
```

## 3. Verify the Stack
Wait a few seconds for services to initialize, then confirm the API is responsive.
```bash
curl -u admin:admin http://localhost:8000/
```

## 4. Required Tools
Ensure the following CLI tools are installed on your host:
- **curl**: For REST API interaction.
- **jq**: For parsing JSON responses.
- **wscat**: For WebSocket testing (`npm install -g wscat`).
