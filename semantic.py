import os
import time
import re
import requests  
import json    
import arxiv     
from datetime import datetime       
# from dateutil.relativedelta import relativedelta 

def get_references_json(arxiv_id, output_json_path): 
    """
    Hàm semantic đã SỬA LỖI LOGIC ĐƯỜNG DẪN.
    Nó sẽ lưu trực tiếp vào 'output_json_path'.
    """
    
    print(f"  Bắt đầu 'Bước Semantic' cho: {arxiv_id}")    

    if os.path.exists(output_json_path):
        print(f"    File '{os.path.basename(output_json_path)}' đã tồn tại. Bỏ qua.")
        return True

    arxiv_client = arxiv.Client()
    url = f"https://api.semanticscholar.org/graph/v1/paper/arXiv:{arxiv_id}"
    params = {"fields": "references,references.externalIds"} 
    output_references_dict = {}
    
    try:
        response = requests.get(url, params=params)
        print("    Đang chờ 3 giây (Rate Limit)...")
        time.sleep(3) 

        if response.status_code == 429:
            retries = 3
            while retries > 0:
                retries -= 1
                print(f"    LỖI 429: Bị Rate Limit. Chờ 20 giây (Còn {retries} lần thử)...")
                time.sleep(20)
                # Thử gọi lại
                response = requests.get(url, params=params)
                print("    Đang chờ 3 giây (Rate Limit)...")
                time.sleep(3)
                if response.status_code != 429:
                    break # Thoát vòng lặp retry nếu thành công
            
            if response.status_code == 429:
                print(f"    Thất bại sau 3 lần thử. Bỏ qua bài này.")
                return False

        if response.status_code != 200:
            print(f"    LỖI: Semantic Scholar API trả về {response.status_code}")
            return False

        data = response.json()
        references_list = data.get("references", [])
        
        print(f"    Tìm thấy {len(references_list)} tham khảo. Đang lọc arXiv ID...")

        for ref in references_list:
            if not ref:
                continue
            external_ids = ref.get("externalIds") or {}
            ref_arxiv_id = external_ids.get("ArXiv")
            
            if ref_arxiv_id and ref_arxiv_id not in output_references_dict:
                try:
                    base_ref_id = re.sub(r'v\d+$', '', ref_arxiv_id)
                    search = arxiv.Search(id_list=[base_ref_id])
                    paper = next(arxiv_client.results(search))
                    
                    ref_metadata = {
                        "title": paper.title,
                        "authors": [a.name for a in paper.authors], 
                        "submission_date": paper.published.isoformat(),
                        "revised_dates": [paper.updated.isoformat()]
                    }
                    print(ref_metadata)
                    output_references_dict[base_ref_id] = ref_metadata
                    time.sleep(0.5) 
                except Exception:
                    pass 

        # SỬA LẠI: Lưu vào 'output_json_path'
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(output_references_dict, f, indent=4)
            
        print(f"    Đã lưu file '{os.path.basename(output_json_path)}' với {len(output_references_dict)} arXiv ID.")
        return True

    except requests.exceptions.RequestException as e:
        print(f"    LỖI (requests): {e}")
        return False
    except Exception as e:
        # Bắt lỗi (ví dụ: 'NoneType' hoặc 'Errno 2')
        print(f"    LỖI (ngoại lệ chung): {e}")
        return False


def run_semantic_for_range(start_month, start_id, end_month, end_id, 
                             source_dir="./sources", 
                             references_dir="./references"): #
    """
    Hàm này chạy "Bước Semantic" cho một dải ID.
    Nó sẽ quét 'source_dir' và lưu kết quả vào 'references_dir'.
    """
    
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    processed_count = 0
    
    print(f"Starting SEMANTIC pass.")
    print(f"Scanning for .tar.gz files in: {source_dir}")
    print(f"Saving .json files to: {references_dir}\n")
    
    os.makedirs(references_dir, exist_ok=True)
    
    if start_month == end_month:
        print(f"Phase: Processing all from {start_month} ({start_id} → {end_id})...")
        current_id = start_id
        while current_id <= end_id:
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\nChecking Paper: {arxiv_id}")
            
            # 1. Kiểm tra xem file TeX (v1) có tồn tại trong './sources' không
            # v1_file_path = os.path.join(source_dir, f"{arxiv_id}v1.tar.gz")
            
            # if os.path.isfile(v1_file_path):
            json_filename = f"{arxiv_id.replace('.', '-')}.json"
            output_json_path = os.path.join(references_dir, json_filename)
            
            if get_references_json(arxiv_id, output_json_path):
                processed_count += 1
            # else:
            #     print(f"  SKIPPING: File '{v1_file_path}' not found in {source_dir}.")
            
            current_id += 1
        print(f"Finished {start_month}.\n")
        
    else:
        # Phase 1: Xử lý tháng đầu tiên
        print(f"Phase 1: Processing {start_month} starting at ID {start_id}...")
        current_id = start_id
        failed_consecutive = 0
        max_consecutive_failures = 3 
        
        while failed_consecutive < max_consecutive_failures:
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\nChecking Paper: {arxiv_id}")
            
            # v1_file_path = os.path.join(source_dir, f"{arxiv_id}v1.tar.gz")
            
            # if os.path.isfile(v1_file_path):
            json_filename = f"{arxiv_id.replace('.', '-')}.json"
            output_json_path = os.path.join(references_dir, json_filename)
            
            if get_references_json(arxiv_id, output_json_path):
                processed_count += 1
            failed_consecutive = 0
            # else:
            #     print(f"  File v1 not found in {source_dir}.")
            #     failed_consecutive += 1
            
            current_id += 1
        
        print(f"Completed {start_month}. Stopped after {max_consecutive_failures} consecutive misses.\n")
        
        # Phase 2: Xử lý tháng thứ hai
        print(f"Phase 2: Processing {end_month} starting at ID 1, going forward to ID {end_id}...")
        current_id = 1
        while current_id <= end_id:
            arxiv_id = f"{end_prefix}.{current_id:05d}"
            print(f"\nChecking Paper: {arxiv_id}")
            
            v1_file_path = os.path.join(source_dir, f"{arxiv_id}v1.tar.gz")

            if os.path.isfile(v1_file_path):
                json_filename = f"{arxiv_id.replace('.', '-')}.json"
                output_json_path = os.path.join(references_dir, json_filename)

                if get_references_json(arxiv_id, output_json_path):
                    processed_count += 1
            else:
                print(f"  SKIPPING: File '{v1_file_path}' not found in {source_dir}.")
            
            current_id += 1
            
        print(f"Reached end ID {end_id} in {end_month}.")
    
    print(f"\n{'='*50}")
    print(f"Semantic pass complete!")
    print(f"Processed 'references.json' for {processed_count} papers.")
    print(f"Source directory scanned: {os.path.abspath(source_dir)}")
    print(f"Output directory: {os.path.abspath(references_dir)}")


# Đạt
if __name__ == "__main__":
    run_semantic_for_range(
        start_month="2023-04",
        start_id=14607,
        end_month="2023-05", 
        end_id=4592,
        source_dir="./sources",       
        references_dir="./references" 
    )
    
# # Nhân
# if __name__ == "__main__":
#     run_semantic_for_range(
#         start_month="2023-05",
#         start_id=4593,
#         end_month="2023-05", 
#         end_id=9594,
#         source_dir="./sources",       # <-- Thư mục chứa TeX
#         references_dir="./references" # <-- Thư mục lưu JSON
#     )
    
# # Việt 
# if __name__ == "__main__":
#     run_semantic_for_range(
#         start_month="2023-05",
#         start_id=9595,
#         end_month="2023-05", 
#         end_id=14596,
#         source_dir="./sources",       # <-- Thư mục chứa TeX
#         references_dir="./references" # <-- Thư mục lưu JSON
#     )