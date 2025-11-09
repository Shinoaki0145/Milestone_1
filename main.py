import multiprocessing
import os


try:
    from semantic_scholar import run_semantic_range_sequential
    from metadata import save_metadata_range_parallel
    from down_extract_clean import download_arxiv_range_parallel
except ImportError as e:
    print(f"LỖI: Không tìm thấy file script. Đảm bảo bạn đặt file này chung thư mục với các file kia.")
    print(f"Chi tiết lỗi: {e}")
    exit()


RUN_CONFIG = "TEST"


# Tập trung tất cả cấu hình tại một nơi
CONFIGS = {
    "TEST": {
        "start_month": "2023-05", "start_id": 4593,
        "end_month": "2023-05", "end_id": 5092,
        "save_dir": "./23127238_test",
        "max_parallels": 10
    },
    "TEST1": {
        "start_month": "2023-05", "start_id": 5093,
        "end_month": "2023-05", "end_id": 5592,
        "save_dir": "./23127238_test",
        "max_parallels": 10
    },
    "BA_DAT": {
        "start_month": "2023-04", "start_id": 14607,
        "end_month": "2023-05", "end_id": 4592,
        "save_dir": "./23127238_badat",
        "max_parallels": 10
    },
    "THIEN_NHAN": {
        "start_month": "2023-05", "start_id": 4593,
        "end_month": "2023-05", "end_id": 9594,
        "save_dir": "./23127238_thiennhan",
        "max_parallels": 10
    },
    "NAM_VIET": {
        "start_month": "2023-05", "start_id": 9595,
        "end_month": "2023-05", "end_id": 14596,
        "save_dir": "./23127238_namviet",
        "max_parallels": 10
    }
}


# Hàm main để chạy các tiến trình
if __name__ == "__main__":
    # 1. Lấy cấu hình đã chọn
    config = CONFIGS.get(RUN_CONFIG)
    if not config:
        print(f"Lỗi: Cấu hình '{RUN_CONFIG}' không tồn tại. Vui lòng kiểm tra lại.")
        exit()

    print(f"--- BẮT ĐẦU CHẠY SONG SONG VỚI CẤU HÌNH: {RUN_CONFIG} ---")
    print(f"--- Thư mục lưu: {config['save_dir']} ---")
    
    # 2. Tạo thư mục lưu (nếu chưa có)
    os.makedirs(config['save_dir'], exist_ok=True)

    # 3. Tạo các đối tượng Process
    # Mỗi Process sẽ chạy một hàm mục tiêu (target) với các tham số (args)
    
    print("Khởi tạo tiến trình 1: Semantic Scholar...")
    p1 = multiprocessing.Process(
        target=run_semantic_range_sequential,
        args=(
            config['start_month'], config['start_id'],
            config['end_month'], config['end_id'],
            config['save_dir']
        )
    )

    print("Khởi tạo tiến trình 2: Metadata...")
    p2 = multiprocessing.Process(
        target=save_metadata_range_parallel,
        args=(
            config['start_month'], config['start_id'],
            config['end_month'], config['end_id'],
            config['save_dir'], config['max_parallels']
        )
    )

    print("Khởi tạo tiến trình 3: Download & Clean...")
    p3 = multiprocessing.Process(
        target=download_arxiv_range_parallel,
        args=(
            config['start_month'], config['start_id'],
            config['end_month'], config['end_id'],
            config['save_dir'], config['max_parallels']
        )
    )

    # 4. Bắt đầu chạy cả 3 tiến trình cùng lúc
    print("\n--- BẮT ĐẦU CHẠY 3 TIẾN TRÌNH ---")
    p1.start()
    p2.start()
    p3.start()

    # 5. Chờ (join) cho đến khi cả 3 tiến trình hoàn thành
    # .join() sẽ "block" (dừng) ở đây cho đến khi tiến trình đó kết thúc
    p1.join()
    print("--- HOÀN THÀNH: Tiến trình 1 (Semantic Scholar) ---")
    
    p2.join()
    print("--- HOÀN THÀNH: Tiến trình 2 (Metadata) ---")
    
    p3.join()
    print("--- HOÀN THÀNH: Tiến trình 3 (Download & Clean) ---")

    print("\n==============================================")
    print("TẤT CẢ 3 TIẾN TRÌNH ĐÃ CHẠY XONG!")
    print(f"Vui lòng kiểm tra kết quả trong thư mục: {os.path.abspath(config['save_dir'])}")
    print("==============================================")