# Cải tiến Orchestration cho Terminal Trading

Để biến `hummingbot-api` thành một hệ thống backend cực kỳ nhanh và tối ưu cho một **Terminal Trading** (nơi yêu cầu độ trễ tính bằng mili-giây từ lúc bot gửi dữ liệu đến lúc hiển thị trên màn hình), kiến trúc orchestration hiện tại có một số điểm thắt cổ chai (bottleneck) và có thể được thiết kế lại/cải tiến ở các điểm sau:

### 1. Thay đổi cơ chế WebSocket từ "Polling" sang "Event-Driven" (Push model)
*   **Hiện tại:** Trong `services/executor_ws_manager.py`, hệ thống đang sử dụng cơ chế vòng lặp `while True` kết hợp `asyncio.sleep(interval)` để **poll** (kéo) dữ liệu từ `BotsOrchestrator` và `ExecutorService`, sau đó băm (hash) dữ liệu để so sánh xem có thay đổi không mới gửi qua WebSocket (`_bot_status_push_loop`, `_logs_push_loop`,...). Cơ chế này tạo ra độ trễ cố định (latency) bằng với `update_interval` và gây tốn CPU không cần thiết.
*   **Cải tiến:** Sử dụng mô hình **Event-Driven** (Observer pattern hoặc Event Bus/Pub-Sub cục bộ).
    *   Trong `utils/mqtt_manager.py`, khi nhận được thông điệp MQTT từ bot (như `log`, `hb` - heartbeat, `status_updates`, `performance`), thay vì chỉ lưu vào biến bộ nhớ, hàm xử lý (`_process_message`) nên **bắn trực tiếp một event** sang `WebSocketManager`.
    *   `WebSocketManager` ngay lập tức đẩy (push) gói tin đó qua WebSocket tới các client đang subscribe. Điều này giúp terminal nhận logs, trades, và heartbeats **gần như tức thời (real-time)** ngay khi broker nhận được.

### 2. Tối ưu hóa Luồng Dữ liệu (Deltas vs Snapshots)
*   **Hiện tại:** Việc lấy trạng thái trả về toàn bộ payload trạng thái mỗi khi có thay đổi.
*   **Cải tiến:**
    *   Khi Terminal vừa kết nối (Subscribe), API sẽ gửi một **Snapshot** (trạng thái đầy đủ hiện tại của bot, logs gần nhất).
    *   Sau đó, hệ thống chỉ gửi các **Deltas/Patches** (những thay đổi nhỏ nhắn, ví dụ: 1 dòng log mới, 1 trade mới, sự thay đổi số dư) thay vì gửi lại toàn bộ JSON khổng lồ chứa mọi trạng thái. Việc này giảm băng thông và giúp Terminal render mượt mà hơn.

### 3. Tách bạch các luồng (Channels) cho WebSocket
*   **Hiện tại:** Một số subscription như `bot_status` đang gộp chung trạng thái, performance, v.v.
*   **Cải tiến:** Cung cấp các Subscriptions độc lập và cực nhỏ gọn cho Terminal:
    *   `bot_heartbeat:{bot_id}`: Chỉ nhận ping/pong từ bot để terminal cập nhật đèn báo online/offline tức thời.
    *   `bot_logs:{bot_id}`: Stream trực tiếp logs.
    *   `bot_trades:{bot_id}`: Chỉ stream lịch sử lệnh/trade fills.
    Điều này giúp UI của Terminal có thể chỉ subscribe vào tab/widget mà user đang nhìn, không bị quá tải bởi dữ liệu không cần thiết.

### 4. WebSocket RPC cho Commands (Điều khiển bot siêu tốc)
*   **Hiện tại:** Để "tạm dừng bot, khởi động lại bots, tắt/bật controller", Terminal có thể đang phải gọi qua REST API (vd: `POST /stop-bot`), API lại gửi lệnh qua Docker hoặc MQTT, sau đó Terminal chờ WebSocket cập nhật trạng thái. Gọi REST có overhead của HTTP (handshake, headers).
*   **Cải tiến:** Hỗ trợ **WebSocket RPC** cho việc gửi lệnh.
    *   Terminal gửi trực tiếp JSON command qua WebSocket: `{"action": "command", "bot_id": "my_bot", "cmd": "start"}`.
    *   `WebSocketManager` chuyển lệnh trực tiếp qua `MQTTManager.publish_command()` (vì Hummingbot nội bộ dùng MQTT để ra lệnh rất nhanh).
    *   Khi lệnh được thực thi, bot trả về response qua MQTT, API nhận được và push thẳng kết quả về qua WebSocket cho Terminal (như là Command Acknowledgement). Tốc độ ra lệnh sẽ mượt mà giống hệt bạn đang gõ trên terminal native.

### 5. Khởi tạo & Quản lý Vòng Đời Container (Docker)
*   **Hiện tại:** Bot được khởi tạo và quản lý thông qua Docker service (`create_hummingbot_instance`). Việc khởi động một container có thể mất vài giây.
*   **Cải tiến:**
    *   **Pre-warm Containers:** Thay vì tạo container mới mỗi khi cần chạy bot, bạn có thể tạo sẵn một "pool" các containers Hummingbot đang chạy ở chế độ nhàn rỗi (idle). Khi người dùng muốn khởi động chiến lược, API chỉ cần cấu hình (thông qua MQTT `config` và `import`) và ra lệnh `start` trên một container nhàn rỗi. Tốc độ khởi động bot sẽ giảm từ vài giây xuống còn vài mili-giây.
