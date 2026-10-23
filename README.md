# Berkeley Time Sync — Đồ án Hệ thống phân tán

Ứng dụng mô phỏng đồng bộ đồng hồ theo Berkeley cho cụm tối đa **6 Node**, với một Master và các Worker. Mọi Node dùng cùng một chương trình và có thể đổi vai trò qua Bully Election hoặc chỉ định Master thủ công.

Điểm chạy hiện tại: `BerkeleyTimeSync/main.py`. Hai thư mục `TimeServerApp/` và `TimeClientApp/` là ứng dụng cũ, chưa sử dụng luồng lõi mới; không dùng chúng để nghiệm thu các chức năng mô tả bên dưới.

## Trạng thái hiện tại

| Hạng mục | Trạng thái |
|---|---|
| Đồng hồ ảo, Berkeley/RTT, phiên, timeout, ACK và hiệu chỉnh trực tiếp (Step) | Đã triển khai; có kiểm thử tự động trong bộ nhớ |
| Discovery, heartbeat và bầu chọn | Đã tích hợp; đã kiểm tra chuỗi heartbeat → mất Master → Bully → đồng bộ lại trong bộ nhớ; chưa nghiệm thu broadcast/LAN |
| Runtime và GUI Node dùng chung | Đã có Dashboard/biểu đồ toàn cụm trên Worker và nhật ký chi tiết; chưa nghiệm thu trực quan và vận hành sáu máy |
| Chọn card IPv4 | Có đọc cấu hình Windows ở luồng nền và nhập thủ công; lần đọc tại môi trường phát triển bị timeout 10 giây, còn chờ kiểm chứng trên máy thử |
| Script firewall | Đã cập nhật và kiểm tra cú pháp/mô phỏng; chưa áp dụng để kiểm thử kết nối thực tế |
| Bộ kiểm thử lưu trong dự án | **143/143 test đạt trên Python 3.12.14** ở lần kiểm tra hiện tại |
| Kiểm tra tích hợp bổ sung | 9 kiểm tra trong bộ nhớ đạt; kịch bản chưa được lưu thành file test và chưa nằm trong 143 bài trên |
| Bản phát hành Node `.exe` mới | Chưa build và chưa kiểm chứng trên Windows không cài Python |
| Sáu máy LAN, báo cáo/slide/video và ít nhất ba lượt tập demo | Chưa nghiệm thu |

Kết quả test mô phỏng không chứng minh khả năng hội tụ hoặc chịu lỗi trên sáu máy vật lý. P0/P1 chưa được đánh dấu hoàn tất nghiệm thu toàn bộ.

## Cấu trúc mã

```text
BerkeleyTimeSync/
├── main.py                     # Điểm chạy Node mới
├── core/                       # Mô hình, đồng hồ, giao thức, phiên, Berkeley, bộ lọc, Bully, cluster_status
├── network/                    # TCP/UDP, discovery, heartbeat, đọc card IPv4
├── controllers/                # Điều phối Node, thành viên, lãnh đạo, đồng bộ, cluster_controller, sync_journal
├── runtime/                    # Luồng sở hữu Controller và hàng đợi lệnh GUI
├── gui/                        # Cửa sổ Node, bảng sáu Node, biểu đồ
├── tests/                      # unittest: protocol, runtime, synchronization, monitoring, dashboard, journal
├── Mo_Cong_Firewall.bat         # Thêm rule TCP 9999 và UDP 8888
├── Dong_Cong_Firewall.bat       # Gỡ đúng rule do cặp script quản lý
├── TimeServerApp/              # Ứng dụng cũ
├── TimeClientApp/              # Ứng dụng cũ
├── Docs/                       # Tài liệu cục bộ, đang được .gitignore bỏ qua
└── Release/                    # Bản build cục bộ, đang được .gitignore bỏ qua
```

`core/node_controller.py` chỉ còn là import tương thích; logic điều phối nằm ở `controllers/node_controller.py`. Các file trong `Docs/`, `Release/` và `outputs/` không được bảo đảm xuất hiện khi clone repository.

## Môi trường

- Windows 10/11, có Python kèm Tkinter. Bộ kiểm thử hiện được chạy bằng Python **3.12.14**; các phiên bản Python khác chưa được xác nhận trong lượt nghiệm thu này.
- Ứng dụng và bộ test dùng thư viện chuẩn Python, không cần cài thư viện từ pip để chạy mã nguồn.
- Tự nhận diện card mạng dùng Windows PowerShell và các cmdlet NetTCPIP. Nếu không đọc được, dùng chế độ nhập IPv4/broadcast thủ công.
- Chỉ thao tác thêm/gỡ rule firewall cần quyền Administrator. Chạy GUI bằng tài khoản thông thường.
- PyInstaller là công cụ tùy chọn khi đóng gói; không phải phụ thuộc để chạy ứng dụng từ mã nguồn.

## Chạy ứng dụng Node

Mở PowerShell tại thư mục repository, nơi có README này và thư mục `BerkeleyTimeSync/`:

```powershell
python -m BerkeleyTimeSync.main
```

Có thể mở với cấu hình điền sẵn:

```powershell
python -m BerkeleyTimeSync.main --node-id 1 --name "Thanh vien 1" --ip 192.168.137.10 --broadcast 192.168.137.255
```

Các IP trên chỉ là ví dụ. Thay bằng IP thực sự được cấp cho card mạng của máy. Lệnh mở cửa sổ; Node chỉ bắt đầu hoạt động khi bấm **Khởi động Node**.

Xem các tham số cấu hình:

```powershell
python -m BerkeleyTimeSync.main --help
```

Cũng có thể chạy `python BerkeleyTimeSync/main.py` từ thư mục repository.

## Cấu hình sáu máy LAN/Hotspot

1. Cho sáu máy Windows dùng chung mạng LAN/Hotspot có thể truyền TCP, UDP unicast và UDP broadcast giữa các máy. Không cần Internet để đồng bộ.
2. Trên mỗi máy, gán một **Node ID duy nhất từ 1 đến 6** và nhập tên thành viên.
3. Chọn đúng **Card mạng IPv4** trong GUI. Chương trình điền IP và broadcast theo prefix/subnet của card; không tự đoán mạng LAN từ tuyến Internet. Nếu danh sách chưa cập nhật, bấm **Làm mới card mạng**.
4. Giữ **Bind IPv4 = 0.0.0.0** để nhận broadcast trên Windows. Đây là địa chỉ lắng nghe, không phải IP dùng để quảng bá cho các Node khác.
5. Nếu chọn **Nhập IP thủ công**, điền IP của chính máy đó và broadcast đúng subnet. Ví dụ `10.20.4.10/23` có broadcast `10.20.5.255`; không mặc định mọi mạng đều là `/24`.
6. Giữ các cổng mặc định bên dưới và chạy script mở firewall trên từng máy trước khi thử kết nối.
7. Bấm **Khởi động Node**. Theo dõi discovery, heartbeat và bầu chọn. Khi hệ thống ổn định ở chế độ tự động, Node hoạt động có ID cao nhất dự kiến trở thành Master.

| Kênh | Cổng mặc định | Công việc |
|---|---|---|
| TCP | 9999 | Đồng bộ Berkeley, bầu chọn và dữ liệu giám sát toàn cụm |
| UDP unicast | 8888 | PING/PONG, heartbeat |
| UDP broadcast | 8888 | Discovery và phản hồi phát hiện Node |

Các Node dùng cùng cổng discovery đích; cổng UDP mặc định cũng là 8888. Đổi cổng là cấu hình thử nghiệm nâng cao và phải đồng bộ với mạng/firewall; hai script hiện chỉ quản lý cổng mặc định.

**Chạy trên một máy:** không mở sáu cửa sổ với cùng địa chỉ bind và cùng cổng mặc định. Mỗi Node sở hữu socket riêng, nên sẽ xung đột cổng. Để xem GUI, có thể chạy một Node; để mô phỏng sáu Node mà không cấu hình mạng riêng, dùng bộ test. Để thử sáu ứng dụng độc lập, dùng các máy ảo có IP riêng trên mạng chung.

**Lưu ý mạng ảo:** bỏ giới hạn firewall không làm UDP broadcast đi xuyên router/NAT. Các Node vẫn cần nằm trong miền mạng có thể nhận broadcast của nhau. Không suy ra kết nối thành công chỉ từ việc máy có Internet hoặc đã có rule Allow.

## Mở và gỡ rule firewall

Trên từng máy Windows tham gia thử nghiệm:

- Trước khi test, nhấp phải `BerkeleyTimeSync/Mo_Cong_Firewall.bat` → **Run as administrator**.
- Sau khi test, nhấp phải `BerkeleyTimeSync/Dong_Cong_Firewall.bat` → **Run as administrator**.

Script mở thêm hai rule **Inbound Allow**, TCP 9999 và UDP 8888, áp dụng cho **mọi profile (Domain/Private/Public)** và **mọi IP nguồn**. Đây là cấu hình đã chọn cho môi trường test. Các máy có đường mạng tới máy thử có thể gửi dữ liệu tới hai cổng này khi rule có hiệu lực.

Rule tồn tại sau khi đóng ứng dụng hoặc khởi động lại Windows; phải chạy script đóng để gỡ. Hai script không tắt firewall và không thay đổi cấu hình antivirus.

Rule được nhận diện bằng Name riêng, nhóm `BerkeleyTimeSync.Project` và dấu sở hữu trong Description. Script kiểm tra nội dung rule trước khi xử lý:

- Chạy script mở lại không tạo trùng nếu rule đúng đã được bật.
- Rule trùng Name nhưng khác nội dung/dấu sở hữu được giữ nguyên và báo lỗi.
- Script đóng chỉ gỡ rule khớp với cặp script này. Rule cũ có tên `Berkeley Time Server` và các rule khác được giữ nguyên.
- Gỡ hai rule không bảo đảm cổng bị chặn nếu rule khác vẫn cho phép; đây không phải thao tác khôi phục/reset toàn bộ firewall.
- Nếu một thao tác thất bại, xem kết quả riêng từng cổng: có thể chỉ một rule đã được thêm/gỡ.

Rule Allow không bảo đảm vượt qua rule Block hoặc chính sách quản trị của máy. Dùng kết quả kết nối thực tế để nghiệm thu.

## Sử dụng Dashboard

| Điều khiển/khu vực | Cách dùng |
|---|---|
| Khởi động / Dừng Node | Bắt đầu/kết thúc runtime và kết nối mạng; cấu hình mạng được khóa khi Node hoạt động |
| Đồng bộ ngay | Chỉ bật khi máy hiện tại là Master; mở một phiên với các Worker đủ điều kiện heartbeat |
| Tự động + 5/10 giây | Chọn giá trị và bấm **Áp dụng chu kỳ** khi đang chạy; xem dòng **Đang áp dụng** để biết cấu hình thực tế |
| Chỉ định máy này làm Master | Manual Override cho chính Node đang thao tác; thực hiện lần lượt trên một máy mỗi lần |
| +5s / −10s / +60s | Tạo lệch trực tiếp cho đồng hồ mô phỏng để quan sát phản ứng |
| Nhập độ lệch | Giây có dấu, tối đa ba chữ số thập phân, trong khoảng ±86400 giây |
| Tạm dừng giờ / Đặt về giờ hệ thống | Chỉ tác động đồng hồ ảo; có thể khiến mẫu của phiên đang chạy bị từ chối |
| Cụm 6 Node | Card hiện vai trò, IP, liên lạc, giờ, Ping RTT, Delta, hiệu chỉnh, ACK/hiệu chỉnh và tuổi mẫu; card Master thêm số Worker ONLINE trên tối đa 5 Worker. Bảng có thêm RTT đồng bộ và bộ lọc |
| Biểu đồ hội tụ | Master và Worker vẽ Delta thực đo trước hiệu chỉnh mỗi phiên; mỗi Node/phiên chỉ thêm một điểm, đo lại ở phiên sau để kiểm chứng kết quả Step |
| Nhật ký | Ghi Master/nhiệm kỳ/phiên, thành viên, báo giờ/RTT, Delta/bộ lọc, kế hoạch hiệu chỉnh, ACK, thay đổi trạng thái, lý do kết thúc và danh sách thiếu; giữ tối đa 2000 dòng trong bộ nhớ |
| Xác nhận thay thế Node cũ | Chỉ dùng sau khi xác minh Node cũ đã dừng và ứng viên mới đúng máy; Controller từ chối thay thế Node cũ còn sống/nghi ngờ |

Khi Manual Master mất heartbeat, cụm có thể quay về bầu chọn tự động. Các thao tác Manual Override đồng thời không có cơ chế đồng thuận toàn cục.

### Đọc trạng thái đúng

- **DISCOVERY/ONLINE**: nhận được liên lạc discovery; chưa đồng nghĩa với xác nhận heartbeat.
- **ACCEPTED**: Worker đã nhận lệnh, chưa xác nhận áp dụng xong; vẫn được hỗ trợ khi đọc dữ liệu từ bản cũ.
- **APPLIED**: Worker báo đã áp dụng hiệu chỉnh. Bản Step trả trạng thái này ngay sau khi chỉnh đồng hồ ảo.
- **Phiên COMPLETED**: các lệnh hiệu chỉnh cần thiết đã được xác nhận; không đồng nghĩa với đo được mọi đồng hồ đã hội tụ.
- **PARTIAL**: thiếu thành viên/phản hồi/xác nhận hoặc có mẫu không dùng được; xem log và danh sách thiếu.
- Giờ trên card của Node từ xa là **mẫu cuối nhận được**, không phải đồng hồ trực tiếp. Bảng có phiên và tuổi mẫu.
- Master phát `CLUSTER_STATUS` qua TCP mỗi giây cho các peer đã biết. Worker dùng dữ liệu này cho bảng/biểu đồ toàn cụm; đồng hồ lớn và trạng thái đồng hồ cục bộ vẫn lấy từ chính Node đó.
- **MASTER/ONLINE**: trạng thái ONLINE theo bản tin của Master. **CŨ/ONLINE**: bản tin đã cũ từ 4 giây kể từ lúc nhận; đây là quan sát trước đó, không phải xác nhận Node vẫn ONLINE ở thời điểm đang xem.
- Tuổi mẫu `≈` cộng tuổi mẫu do Master ghi với thời gian cục bộ từ lúc nhận bản tin, chưa gồm độ trễ truyền chưa biết. Dòng nguồn Master hiển thị riêng tuổi bản tin; hai loại tuổi này khác nhau.
- Card Master đếm Worker có heartbeat ONLINE theo nguồn tương ứng; nhãn `(Master)` hoặc `(CŨ)` cho biết số liệu từ bản tin Master. `--` nghĩa chưa có dữ liệu, không thay bằng số 0. Discovery đơn thuần không được tính là kết nối đã xác nhận.
- Worker chưa nhận số đo sẽ để trống phần tương ứng. Một mẫu của phiên trước có thể còn hiển thị trong lúc thu mẫu phiên mới; đọc số phiên trên mẫu và trạng thái toàn phiên riêng biệt.
- Biểu đồ đánh dấu × cho mẫu bị loại, giữ tối đa 180 mẫu/Node và bắt đầu lịch sử mới khi đổi nhiệm kỳ Master.
- Bản tin trùng/cũ hoặc sai Master/nhiệm kỳ/boot không làm mới tuổi dữ liệu. Node khởi động lại không được kế thừa số đo của boot trước.
- Nhật ký chỉ ghi thay đổi dữ liệu/trạng thái, không ghi lặp theo mỗi nhịp GUI. “Lập lệnh” là tạo kế hoạch gửi, không tự khẳng định truyền thành công; thiếu ACK khác với REJECTED. Đọc ACK và lý do theo đúng phiên.
- Log/biểu đồ chưa được lưu bền vững hoặc xuất file tự động.

## Ghi chú thuật toán và đối chiếu đặc tả

Đồng hồ dùng thời gian Unix ở độ chính xác mili giây, đo khoảng thời gian bằng monotonic. Hiển thị giờ cuộn qua nửa đêm không làm mất thông tin ngày trong phép tính. Đây là đồng hồ mô phỏng; chương trình không đặt giờ hệ điều hành.

Master đo RTT và ước lượng Delta bằng bốn mốc: Master gửi, Worker nhận, Worker gửi, Master nhận. Dùng hai mốc Worker để tính ảnh hưởng của thời gian xử lý thay vì xem toàn bộ khoảng đó là độ trễ truyền. Ước lượng vẫn phụ thuộc giả định đường truyền gần đối xứng và độ lệch tương đối ổn định trong phép đo.

Mẫu RTT trên 500 ms bị loại do chất lượng đo. Mọi mẫu còn lại, cùng mẫu Master, được dùng để tính trung bình; không có ngưỡng độ lệch hay bước chọn nhóm đồng hồ gần nhau.

Độ lệch lớn không làm mẫu bị loại khỏi phép tính trung bình. Mẫu RTT quá cao không nhận hiệu chỉnh trong phiên đó. Thay đổi này bỏ chính sách lọc độ lệch mô tả trong bản đặc tả PDF cũ theo yêu cầu mới.

Luồng đồng bộ dùng Step: Master và Worker cộng/trừ ngay mức hiệu chỉnh Berkeley vào đồng hồ ảo. Hiệu chỉnh âm có thể làm giờ nhảy lùi; không thay đổi giờ Windows. Đây là thay đổi thiết kế so với Slewing trong đặc tả cũ để demo không phải chờ hàng chục phút khi lệch lớn. Nếu yêu cầu không nhảy lùi là bắt buộc, Step không đáp ứng yêu cầu đó. API Slewing vẫn được giữ trong VirtualClock nhưng luồng đồng bộ không gọi nó để áp dụng lệnh. Timeout và lịch chạy vẫn dựa trên monotonic, không dựa trên giờ ảo có thể nhảy.

Thông điệp được kiểm tra schema, nguồn theo bảng thành viên, boot ID, nhiệm kỳ và phiên. Các kiểm tra này không phải xác thực mật mã; giao thức hiện chưa có mã hóa hay cơ chế đăng nhập Node.

## Chạy bộ kiểm thử

Từ thư mục repository:

```powershell
python -B -m unittest discover -s BerkeleyTimeSync/tests -p "test_*.py" -v
```

| File | Phạm vi |
|---|---|
| `test_protocol.py` | 15 test: schema, Unicode/UTF-8, frame TCP chia/gộp, giới hạn kích thước, ngữ cảnh phiên |
| `test_runtime.py` | 9 test: luồng sở hữu, hàng đợi, lỗi lệnh/dịch vụ, startup và shutdown |
| `test_sync.py` | 19 test: sáu SyncController mô phỏng, RTT, timeout, thiếu ACK, Step, độ lệch lớn, đo lại sau hiệu chỉnh, lệnh trùng/cũ, đầu vào không hợp lệ và API Slewing |
| `test_cluster_status.py` | 30 test: schema dữ liệu toàn cụm, liên kết Node/phiên, giá trị thiếu và tính bất biến |
| `test_cluster_controller.py` | 23 test: phát/nhận dữ liệu giám sát, thứ tự bản tin, dữ liệu cũ, đổi Master/boot và hàng đợi ưu tiên |
| `test_dashboard.py` | 35 test: ACK, chiếu dữ liệu lên bảng/card, tuổi mẫu, số Worker, biểu đồ và chống vẽ lặp |
| `test_sync_journal.py` | 16 test: nhật ký theo phiên, RTT/Delta/bộ lọc, hiệu chỉnh Step, ACK, timeout và chống ghi lặp |

Tổng cộng 147 bài kiểm thử lưu trong dự án. Các test dùng bộ điều khiển giả hoặc thời gian/gói tin mô phỏng; các bài runtime/sync chặn tạo socket. Chúng không mở cửa sổ GUI, không thay đổi firewall và không xác nhận discovery/bầu chọn qua mạng thật. Bài đổi Master trong `test_sync.py` chủ động cung cấp lãnh đạo mới; không phải bài kiểm chứng thuật toán Bully.

Ghi nhận lịch sử trước khi chuyển sang Step: đã chạy riêng 9 kiểm tra tích hợp trong bộ nhớ với LeadershipController, heartbeat, Bully và SyncController thật, thay vận chuyển bằng gói tin mô phỏng có serialize/parse: tự bầu Node 6; đồng bộ năm Worker; không bầu trước deadline từ PING cuối; mất Master và bầu Node 5; hủy phiên cũ/từ chối hiệu chỉnh cũ và đồng bộ lại; hội tụ sau slewing; Worker mất mạng rồi trở lại trong cùng boot; chỉ định Master 2; mất Master thủ công và quay về Bully. Cả 9 kiểm tra đạt, độ lệch sau slewing trong kịch bản đó là 0 ms. Kịch bản này chưa được lưu thành file, không được tính vào bộ kiểm thử hiện tại và không tự chạy lại bằng lệnh trên; kết quả không thay thế kiểm tra transport, discovery, GUI hoặc LAN thực tế.

## Kịch bản nghiệm thu sáu máy LAN/Hotspot

Thực hiện sau khi hoàn thiện mã, ghi kết quả thực tế thay vì dùng kết quả mô phỏng làm bằng chứng:

1. Khởi chạy sáu Node có ID khác nhau trên mạng chung; kiểm tra discovery, bảng thành viên và Master tự động.
2. Tạo lệch +5s/−10s, đồng bộ và theo dõi ACK APPLIED và số đo của các phiên tiếp theo; ghi chênh lệch bằng số đo, không chỉ bằng mắt.
3. Tạo một Node lệch +60s; kiểm tra mẫu hợp lệ được tính vào trung bình, Step được áp dụng và đo lại sai số ở phiên tiếp theo.
4. Dừng một Worker giữa phiên; xác nhận phiên kết thúc trong hạn và các Node khác tiếp tục.
5. Dừng Master giữa phiên; kiểm tra bầu Master mới, hủy phiên cũ và tiếp tục đồng bộ.
6. Chỉ định Master thủ công; kiểm tra chuyển vai trò và phục hồi sau khi Manual Master dừng.
7. Khởi động lại Worker/Master với cùng Node ID; xác minh ứng viên, chấp nhận thay thế khi hợp lệ và theo dõi đồng bộ lại.
8. Đóng ứng dụng và mở lại; kiểm tra giải phóng cổng. Chạy script gỡ rule sau khi kết thúc.
9. Khi có bản build mới, kiểm tra trên Windows không có Python. Ghi máy, phiên bản, mạng, thời điểm, sai số và lỗi.
10. Chuẩn bị báo cáo, slide, video và tập demo ít nhất ba lần trên sáu máy theo đặc tả.

Máy ảo là phương án kiểm tra cá nhân bổ sung: đặt mỗi Node trên một máy ảo có IP riêng và đường TCP/UDP/broadcast phù hợp. Kết quả này không thay thế yêu cầu nghiệm thu sáu máy vật lý theo đặc tả của nhóm.

## Đóng gói Node thành .exe — hướng dẫn chưa nghiệm thu

Chỉ thực hiện khi muốn chuẩn bị bản build Windows mới; hiện chưa có bằng chứng bản `BerkeleyNode.exe` được build và chạy thành công. Không dùng các file `TimeServer.exe`/`TimeClient.exe` cũ làm bản phát hành của Node mới.

Cài công cụ đóng gói trong môi trường Python dùng để build:

```powershell
python -m pip install pyinstaller
```

Từ thư mục repository, chọn tên lượt build chưa tồn tại để giữ các bản trước:

```powershell
$buildTag = "Node-build-01"
$distPath = "BerkeleyTimeSync/Release/$buildTag"
$workPath = "BerkeleyTimeSync/build/$buildTag"
if ((Test-Path -LiteralPath $distPath) -or (Test-Path -LiteralPath $workPath)) {
    throw "Thu muc da ton tai. Hay chon buildTag moi."
}
python -m PyInstaller --onefile --noconsole --name BerkeleyNode --paths "." --distpath $distPath --workpath "$workPath/work" --specpath "$workPath/spec" BerkeleyTimeSync/main.py
```

Đầu ra dự kiến: `BerkeleyTimeSync/Release/Node-build-01/BerkeleyNode.exe`. Nếu build báo lỗi, giữ lại log để xử lý. Gói bàn giao cần có hai script firewall bên cạnh hướng dẫn sử dụng; không yêu cầu chạy GUI bằng Administrator.

Phải kiểm tra bản build trên máy Windows đích không cài Python trước khi ghi nhận “chạy độc lập”. Các tùy chọn đóng gói tham khảo từ [tài liệu PyInstaller](https://pyinstaller.org/en/stable/usage.html).

## Đồng bộ khi độ lệch ban đầu lớn

Đã bỏ ngưỡng chênh lệch 10 giây và cơ chế chọn nhóm đồng hồ gần nhau. Master tính trung bình từ mẫu của chính nó và mọi báo giờ Worker hợp lệ, không loại mẫu vì độ lệch lớn. Kiểm tra chất lượng RTT, timeout và tính hợp lệ của thông điệp vẫn áp dụng. Nút +60s chỉ tạo độ lệch mô phỏng. Step áp dụng ngay hiệu chỉnh, kể cả khi lệch vài phút. Hai Node lệch 447,175 giây còn chênh tối đa 1 ms sau một phiên trong kiểm thử mô phỏng; đây không phải cam kết sai số trên LAN. Biểu đồ vẫn giữ số đo trước hiệu chỉnh của phiên hiện tại, nên cần phiên đo tiếp theo để thấy kết quả. Dùng cùng bản mới trên mọi Node; EXE cũ vẫn chạy cơ chế cũ cho tới khi được build lại.
