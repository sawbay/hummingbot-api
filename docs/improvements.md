# Cải tiến Orchestration cho Terminal Trading

Để biến `hummingbot-api` thành một hệ thống backend cực kỳ nhanh và tối ưu cho một **Terminal Trading** (nơi yêu cầu độ trễ tính bằng mili-giây từ lúc bot gửi dữ liệu đến lúc hiển thị trên màn hình), kiến trúc orchestration hiện tại có một số điểm thắt cổ chai (bottleneck) và có thể được thiết kế lại/cải tiến. 

Dưới đây là các điểm hạn chế hiện tại và **Tổng hợp Kiến trúc (Master Architecture)** để khắc phục:

### 1. Từ bỏ Polling, Chuyển sang Event-Driven (Pub/Sub) Toàn Diện
*   **Hiện tại:** Trong `services/executor_ws_manager.py`, hệ thống đang sử dụng cơ chế vòng lặp `while True` kết hợp `asyncio.sleep(interval)` để **poll** (kéo) dữ liệu từ `BotsOrchestrator` và `ExecutorService`, sau đó băm (hash) dữ liệu để so sánh xem có thay đổi không mới gửi qua WebSocket (`_bot_status_push_loop`, `_logs_push_loop`,...). Cơ chế này tạo ra độ trễ cố định (latency) bằng với `update_interval` và gây tốn CPU không cần thiết.
*   **Cải tiến (Event Bus):** Thay vì `while True` và `asyncio.sleep`, chúng ta sẽ dùng thư viện (như `asyncio.Queue` hoặc thư viện Pub/Sub cục bộ của Python) để làm Event Bus. Khi `MQTTManager` nhận message, hoặc `DockerService` có event, chúng lập tức publish lên Event Bus nội bộ. Các hàm stream của WebSocket chỉ việc `await bus.subscribe()`, nhận event và gửi thẳng xuống client. Độ trễ gần như bằng 0.

### 2. Tối ưu hóa Luồng Dữ liệu: Deltas vs Snapshots & Chia nhỏ Channel
*   **Hiện tại:** Việc lấy trạng thái trả về toàn bộ payload trạng thái mỗi khi có thay đổi. Một số subscription như `bot_status` đang gộp chung trạng thái, performance, v.v.
*   **Cải tiến:** Định nghĩa lại giao thức WebSocket để cung cấp các Subscriptions độc lập và cực nhỏ gọn cho Terminal:
    *   **Snapshots vs Deltas:** Khi client subscribe `bot_logs:botA`, server trả về 100 logs gần nhất (Snapshot), sau đó mỗi khi có log mới từ MQTT, gửi đúng 1 dòng log đó (Delta) thay vì gửi lại toàn bộ JSON khổng lồ.
    *   **Chia nhỏ Channels:**
        *   `bot_heartbeat`: Dùng LWT (Last Will) của MQTT + tín hiệu notify định kỳ để terminal cập nhật đèn báo online/offline tức thời.
        *   `bot_logs`: Stream trực tiếp log topic.
        *   `bot_trades`: Lắng nghe topic events của Hummingbot (OrderFilledEvent, v.v.).
        *   `bot_performance`: Gửi metrics theo Deltas.
    Điều này giúp UI của Terminal có thể chỉ subscribe vào tab/widget mà user đang nhìn, không bị quá tải bởi dữ liệu không cần thiết.

### 3. WebSocket RPC cho Điều khiển Bot (Commands)
*   **Hiện tại:** Để "tạm dừng bot, khởi động lại bots, tắt/bật controller", Terminal có thể đang phải gọi qua REST API (vd: `POST /stop-bot`), API lại gửi lệnh qua Docker hoặc MQTT, sau đó Terminal chờ WebSocket cập nhật trạng thái. Gọi REST có overhead của HTTP (handshake, headers).
*   **Cải tiến:** Cho phép gửi commands trực tiếp qua kết nối WebSocket đã mở, thay vì phải mở HTTP request mới.
    *   **Flow:** Terminal (WS) -> API WS Router -> `MQTTManager.publish_command` -> Bot. Bot trả kết quả -> MQTT -> API -> Terminal (Ack). Tốc độ cực nhanh và mượt mà giống hệt bạn đang gõ trên terminal native.

### 4. Quản lý Controller Mức Độ Sâu (Deep Controller Management)
*   Xây dựng cơ chế gửi custom MQTT message để tương tác với script `v2_with_controllers.py`.
*   Hỗ trợ lệnh: Bật, Tắt, hoặc thay đổi cấu hình nóng (hot-reload) của từng controller độc lập mà không cần khởi động lại toàn bộ bot.

### 5. Theo dõi vòng đời Docker siêu tốc (Docker Event Stream)
*   **Hiện tại:** Bot được khởi tạo và quản lý thông qua Docker service, đôi khi có độ trễ trong việc cập nhật trạng thái container.
*   **Cải tiến:** Sử dụng `docker.client.events()` chạy background thay vì polling trạng thái container. Bắn tín hiệu "Created", "Started", "Died" lập tức về Terminal qua WebSocket.
*   *(Lưu ý về "Pre-warm Containers": Việc tạo sẵn pool container nhàn rỗi để giảm thời gian khởi động bot tốn khá nhiều RAM do Hummingbot khá nặng. Bước này nên để là Option hoặc làm trong Phase 2 sau khi các phần lõi Event-Driven đã ổn định).*
