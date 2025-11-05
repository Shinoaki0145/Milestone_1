import arxiv
import os

def get_source(arxiv_id, save_dir="./sources"):
    os.makedirs(save_dir, exist_ok=True)
    
    try:
        paper = next(arxiv.Client().results(arxiv.Search(id_list=[arxiv_id])))
        
        paper.download_source(dirpath=save_dir, filename=f"{arxiv_id}.tar.gz")
        print(f"Successfully downloaded: {arxiv_id}")
        return True
    except Exception as e:
        print(f"Error downloading {arxiv_id}: {str(e)}")
        return False

def download_arxiv_range(start_month, start_id, end_month, end_id, save_dir="./sources"):
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    downloaded = 0
    failed_consecutive = 0
    max_consecutive_failures = 3
    
    print(f"Starting forward download from {start_prefix}.{start_id:05d} to {end_prefix}.{end_id:05d}")
    print(f"Saving to directory: {save_dir}\n")
    
    print(f"Phase 1: Downloading from {start_month} starting at ID {start_id}...")
    current_id = start_id
    while failed_consecutive < max_consecutive_failures:
        arxiv_id = f"{start_prefix}.{current_id:05d}"
        if get_source(arxiv_id, save_dir):
            downloaded += 1
            failed_consecutive = 0
        else:
            failed_consecutive += 1
        current_id += 1
    
    print(f"Completed {start_month}. No more papers found after {max_consecutive_failures} consecutive failures.\n")
    
    print(f"Phase 2: Downloading from {end_month} starting at ID 1, going forward to ID {end_id}...")
    failed_consecutive = 0
    
    current_id = 1
    while current_id <= end_id:
        arxiv_id = f"{end_prefix}.{current_id:05d}"
        if get_source(arxiv_id, save_dir):
            downloaded += 1
            failed_consecutive = 0
        else:
            failed_consecutive += 1
        current_id += 1
    
    print(f"Reached end ID {end_id} in {end_month}.")
    
    print(f"\n{'='*50}")
    print(f"Download complete!")
    print(f"Successfully downloaded: {downloaded} papers")
    print(f"Files saved to: {os.path.abspath(save_dir)}")

def check_file(dir, start_id, end_id):
    existing_files = set(os.listdir(dir))
    list_files = []
    for file in existing_files:
        if file.endswith(".tar.gz"):
            id_part = file.split(".")[1]
            id = int(id_part)
            if not (start_id <= id <= end_id):
                os.remove(os.path.join(dir, file))
            else:
                list_files.append(int(id_part))
    list_files.sort()
    return list_files
    
# # Đạt
# if __name__ == "__main__":
#     download_arxiv_range(
#         start_month="2023-04",
#         start_id=14607,
#         end_month="2023-05", 
#         end_id=4592,
#         save_dir="./sources"
#     )
    
# # Nhân
# if __name__ == "__main__":
#     download_arxiv_range(
#         start_month="2023-05",
#         start_id=4593,
#         end_month="2023-05", 
#         end_id=9594,
#         save_dir="./sources"
#     )
    
# # Việt 
if __name__ == "__main__":
    # start_id=9595
    # end_id=14596

    download_arxiv_range(
        start_month="2023-05",
        start_id=9595,
        end_month="2023-05", 
        end_id=14596,
        save_dir="./sources"
    )
    # list_files = check_file(dir="./sources",start_id=start_id, end_id=end_id)
    # for i in range(start_id, end_id + 1):
    #     if i not in list_files:
    #         arxiv_id = f"2305.{i:05d}"
    #         # print(f"Downloading missing file: {arxiv_id}")
    #         # get_source(arxiv_id, "./sources")
    #         print(arxiv_id)