import arxiv
import os
import time
import re
import json
from datetime import datetime       

def get_metadata_all_versions(arxiv_id, client):
    """
    Hàm này lấy metadata cho MỘT paper, bao gồm TẤT CẢ các ngày
    chỉnh sửa bằng cách quét từng phiên bản (v1, v2, ...).
    
    Trả về một dictionary chứa metadata, hoặc None nếu thất bại.
    """
    
    latest_version = 0
    base_paper = None

    # --- Bước 1: Tìm phiên bản mới nhất ---
    try:
        # Tìm paper cơ sở để biết phiên bản mới nhất là bao nhiêu
        search_base = arxiv.Search(id_list=[arxiv_id])
        base_paper = next(client.results(search_base))
        
        # entry_id có dạng 'http://arxiv.org/abs/2304.14607v3'
        entry_id_url = base_paper.entry_id
        
        # Dùng regex (giống hệt file của bạn) để tìm số phiên bản
        match = re.search(r'v(\d+)$', entry_id_url)
        
        if not match:
            latest_version = 1
        else:
            latest_version = int(match.group(1)) 

    except StopIteration:
        print(f"  LỖI: Không tìm thấy paper với ID {arxiv_id} (thậm chí v1).")
        return None
    except Exception as e:
        print(f"  LỖI khi tìm phiên bản mới nhất của {arxiv_id}: {e}")
        return None

    # --- Bước 2: Lặp qua từng phiên bản để lấy ngày ---
    
    print(f"  Tìm thấy {latest_version} phiên bản cho {arxiv_id}. Đang lấy metadata...")
    
    submission_date = None
    revised_dates_list = []
    
    # Dùng để lưu metadata từ phiên bản mới nhất
    final_title = base_paper.title
    final_authors = [author.name for author in base_paper.authors]
    final_venue = base_paper.journal_ref

    for v in range(1, latest_version + 1):
        versioned_id = f"{arxiv_id}v{v}"
        
        try:
            search_version = arxiv.Search(id_list=[versioned_id])
            paper_version = next(client.results(search_version))
            
            # Ngày 'published' của paper_version CHÍNH LÀ ngày nộp/sửa đổi
            # của phiên bản đó.
            version_date = paper_version.published.isoformat()

            if v == 1:
                submission_date = version_date
            else:
                revised_dates_list.append(version_date)
            
            # Lấy metadata từ phiên bản mới nhất (an toàn nhất)
            if v == latest_version:
                 final_title = paper_version.title
                 final_authors = [author.name for author in paper_version.authors]

            time.sleep(0.5) # Giảm tải cho API

        except StopIteration:
            print(f"  LỖI: Đã tìm thấy v{latest_version} nhưng không thể tìm thấy {versioned_id}?")
            continue 
        except Exception as e:
            print(f"  LỖI khi lấy metadata cho {versioned_id}: {e}")
            continue
            
    # --- Bước 3: Tổng hợp kết quả ---
    if submission_date is None:
        print(f"  LỖI: Không thể lấy được metadata (thậm chí v1) cho {arxiv_id}.")
        return None

    metadata = {
        'title': final_title,
        'authors': final_authors,
        'submission_date': submission_date, # Ngày nộp (từ v1)
        'revised_dates': revised_dates_list,  # Danh sách ngày sửa (từ v2, v3...)
    }
    
    return metadata

def process_metadata_range(start_month, start_id, end_month, end_id, save_dir="./metadata"):
    """
    Hàm này chạy logic lặp qua các ID (giống file down_TeX_source.py)
    và gọi hàm get_metadata_all_versions để lấy metadata.
    """
    
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    processed_count = 0
    failed_consecutive = 0
    max_consecutive_failures = 3 # Giống file của bạn
    
    print(f"Starting metadata processing from {start_prefix}.{start_id:05d} to {end_prefix}.{end_id:05d}")
    print(f"Saving .json files to directory: {save_dir}\n")
    
    # Tạo thư mục lưu trữ (giống logic file của bạn)
    os.makedirs(save_dir, exist_ok=True)
    
    # Tạo MỘT client để tái sử dụng, hiệu quả hơn
    client = arxiv.Client()
    
    # --- TRƯỜNG HỢP 1: TRONG CÙNG 1 THÁNG ---
    if start_month == end_month:
        print(f"Phase: Processing all from {start_month} ({start_id} → {end_id})...")
        current_id = start_id
        while current_id <= end_id:
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\nProcessing Paper: {arxiv_id}")
            
            # Gọi hàm lấy metadata
            metadata = get_metadata_all_versions(arxiv_id, client)
            
            if metadata:
                # Lưu file JSON
                json_filename = f"{arxiv_id.replace('.', '-')}.json"
                output_json_path = os.path.join(save_dir, json_filename)
                
                try:
                    with open(output_json_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=4, ensure_ascii=False)
                    print(f"  -> SUCCESS: Đã lưu {json_filename}")
                    processed_count += 1
                    failed_consecutive = 0
                except Exception as e:
                    print(f"  LỖI khi lưu file JSON {output_json_path}: {e}")
                    failed_consecutive += 1 # Coi như thất bại
            else:
                # Lỗi đã được in bên trong hàm get_metadata_all_versions
                failed_consecutive += 1
                
            current_id += 1
        print(f"Finished {start_month}.\n")

    # --- TRƯỜNG HỢP 2: KHÁC THÁNG (LOGIC 2 GIAI ĐOẠN) ---
    else:
        # Phase 1: Xử lý tháng đầu tiên
        print(f"Phase 1: Processing {start_month} starting at ID {start_id}...")
        current_id = start_id
        while failed_consecutive < max_consecutive_failures:
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\nProcessing Paper: {arxiv_id}")
            
            metadata = get_metadata_all_versions(arxiv_id, client)
            
            if metadata:
                json_filename = f"{arxiv_id.replace('.', '-')}.json"
                output_json_path = os.path.join(save_dir, json_filename)
                
                try:
                    with open(output_json_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)
                    print(f"  -> SUCCESS: Đã lưu {json_filename}")
                    processed_count += 1
                    failed_consecutive = 0
                except Exception as e:
                    print(f"  LỖI khi lưu file JSON {output_json_path}: {e}")
                    failed_consecutive += 1
            else:
                failed_consecutive += 1
                
            current_id += 1
        
        print(f"Completed {start_month}. Dừng lại sau {max_consecutive_failures} lần thất bại liên tiếp.\n")
        
        # Phase 2: Xử lý tháng cuối cùng
        print(f"Phase 2: Processing {end_month} starting at ID 1, going forward to ID {end_id}...")
        failed_consecutive = 0 # Reset bộ đếm
        
        current_id = 1
        while current_id <= end_id:
            arxiv_id = f"{end_prefix}.{current_id:05d}"
            print(f"\nProcessing Paper: {arxiv_id}")

            metadata = get_metadata_all_versions(arxiv_id, client)
            
            if metadata:
                json_filename = f"{arxiv_id.replace('.', '-')}.json"
                output_json_path = os.path.join(save_dir, json_filename)
                
                try:
                    with open(output_json_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)
                    print(f"  -> SUCCESS: Đã lưu {json_filename}")
                    processed_count += 1
                    failed_consecutive = 0
                except Exception as e:
                    print(f"  LỖI khi lưu file JSON {output_json_path}: {e}")
                    failed_consecutive += 1 # Vẫn tiếp tục
            else:
                failed_consecutive += 1 # Vẫn tiếp tục
                
            current_id += 1
        
        print(f"Reached end ID {end_id} in {end_month}.")
    
    # --- TỔNG KẾT ---
    print(f"\n{'='*50}")
    print(f"Metadata processing complete!")
    print(f"Successfully processed and saved: {processed_count} papers")
    print(f"Files saved to: {os.path.abspath(save_dir)}")

# --- CÁCH CHẠY ---
if __name__ == "__main__":
    # Sử dụng cấu hình giống hệt file của bạn
    process_metadata_range(
        start_month="2023-04",
        start_id=14607,
        end_month="2023-05", 
        end_id=4592,
        save_dir="./metadata_output" # Đổi tên thư mục lưu
    )

    # # Ví dụ cho "Nhân"
    # process_metadata_range(
    #     start_month="2023-05",
    #     start_id=4593,
    #     end_month="2023-05", 
    #     end_id=9594,
    #     save_dir="./metadata_output"
    # )
    
    # # Ví dụ cho "Việt"
    # process_metadata_range(
    #     start_month="2023-05",
    #     start_id=9595,
    #     end_month="2023-05", 
    #     end_id=14596,
    #     save_dir="./metadata_output"
    # )