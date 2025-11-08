import arxiv
import os
import time
import re
import tarfile
import glob
import shutil
import subprocess
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Thread-safe locks and counters
download_lock = Lock()
stats = {
    "downloaded": 0, 
    "failed_download": 0,
    "deleted_version": 0,
    "extracted": 0,
    "failed_extract": 0,
    "deleted_files": 0,
    "pdfs": 0
}

def detect_and_fix_filetype(tar_path):
    """
    Check filetype (tar.gz, PDF, GZ, ...)
    Returns: (path, filetype, original_name_if_gz)
    """
    try:
        result = subprocess.run(["file", tar_path], capture_output=True, text=True, errors='ignore') # errors
        output = result.stdout.strip()
    except FileNotFoundError:
        print("X 'file' command not found. Please install 'file' utility.")
        return tar_path, "unknown", None
    except Exception as e:
        print(f"X Error running 'file' command on {tar_path}: {e}")
        return tar_path, "unknown", None

    if "PDF document" in output:
        with download_lock:
            stats["pdfs"] += 1
        print(f"{os.path.basename(tar_path)} => Detected as PDF")
        return tar_path, "pdf", None

    elif "gzip compressed data" in output:
        match = re.search(r', was "([^"]+)"', output)
        if match:
            original_name = os.path.basename(match.group(1))
            return tar_path, "gz", original_name
        else:
            return tar_path, "tar.gz", None

    elif "tar archive" in output:
        return tar_path, "tar.gz", None

    else:
        print(f"Unknown format: {os.path.basename(tar_path)} => {output}")
        return tar_path, "unknown", None

def extract_and_clean_single_tar(tar_path, destination_folder, base_name):
    file_name = os.path.basename(tar_path)
    
    fixed_path, filetype, original_name = detect_and_fix_filetype(tar_path)

    if filetype == "pdf":
        return (file_name, True, 0, "pdf")

    if filetype == "unknown":
        print(f"Skipping unsupported format: {file_name}")
        return (file_name, False, 0, "unknown")

    # Use the provided base_name (cleaned without dots)
    extract_path = os.path.join(destination_folder, base_name)
    os.makedirs(extract_path, exist_ok=True)

    local_deleted_files = 0

    try:
        if filetype == "tar.gz":
            with tarfile.open(fixed_path, 'r:*') as tar:
                tar.extractall(path=extract_path)
            print(f"Extracted {file_name} (as TAR.GZ)")

        elif filetype == "gz":
            if original_name is None:
                original_name = base_name + ".file" 
                print(f"Warning: Could not detect original filename for {file_name}, saving as {original_name}")

            output_filename = os.path.join(extract_path, original_name)

            with gzip.open(fixed_path, 'rb') as f_in:
                with open(output_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Extracted {file_name} (as GZ)")

    except Exception as e:
        print(f"X    Error extracting {file_name}: {e}")
        try:
            shutil.rmtree(extract_path)
        except Exception:
            pass
        return (file_name, False, 0, "tar_fail")

    try:
        # Duyệt qua tất cả các file trong thư mục đã giải nén
        for root, dirs, files in os.walk(extract_path):
            for file_name_in_folder in files:
                
                # Lấy phần mở rộng (extension) của file
                _, ext = os.path.splitext(file_name_in_folder)
                
                if ext.lower() not in ['.tex', '.bib']:
                    file_path = os.path.join(root, file_name_in_folder)
                    try:
                        os.remove(file_path)
                        local_deleted_files += 1
                    except Exception as e:
                        print(f"X    Cannot delete {file_path}: {e}")

        return (file_name, True, local_deleted_files, "tar_ok")

    except Exception as e:
        print(f"Error cleaning {file_name}: {e}")
        return (file_name, False, local_deleted_files, "tar_fail")

def get_source_all_versions(arxiv_id, save_dir="./23127238"):
    os.makedirs(save_dir, exist_ok=True)
    
    client = arxiv.Client()
    versions_downloaded = 0
    latest_version = 0

    # Split arxiv_id into prefix and suffix
    if '.' in arxiv_id:
        prefix, suffix = arxiv_id.split('.')
    else:
        print(f"X  ERROR: Invalid arxiv_id format {arxiv_id}")
        return False

    # Create paper folder: yymm-id
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    os.makedirs(paper_folder, exist_ok=True)

    try:
        search_base = arxiv.Search(id_list=[arxiv_id])
        base_paper = next(client.results(search_base))
        
        entry_id_url = base_paper.entry_id
        
        match = re.search(r'v(\d+)$', entry_id_url)
        
        if not match:
            latest_version = 1
        else:
            latest_version = int(match.group(1)) 

        print(f"  \nDetect version {latest_version} as the latest for {arxiv_id}. Starting download from v1...")

    except StopIteration:
        print(f"X  ERROR: No paper found with ID {arxiv_id} (even v1)")
        return False
    except Exception as e:
        print(f"X  ERROR: When finding latest version of {arxiv_id}: {e}")
        return False

    for v in range(1, latest_version + 1):
        versioned_id = f"{arxiv_id}v{v}"
        # Clean base_name for version folder: yymm-idvN
        clean_version_base = f"{prefix}-{suffix}v{v}"

        try:
            search_version = arxiv.Search(id_list=[versioned_id])
            paper_version = next(client.results(search_version))

            filename = f"{versioned_id}.tar.gz"
            temp_filepath = os.path.join(paper_folder, filename)
            print(f"    Downloading: {filename}...")

            paper_version.download_source(dirpath=paper_folder, filename=filename)

            # Immediately extract and clean
            file_name, success, deleted_files, ftype = extract_and_clean_single_tar(temp_filepath, paper_folder, clean_version_base)
            
            if success:
                versions_downloaded += 1
                with download_lock:
                    stats["extracted"] += 1
                    stats["deleted_files"] += deleted_files
                print(f"    Extracted and cleaned: {versioned_id}")
            else:
                with download_lock:
                    stats["failed_extract"] += 1
                print(f"X   Failed to extract/clean: {versioned_id}")

            # Delete the tar.gz file after extraction
            try:
                os.remove(temp_filepath)
                print(f"    Deleted temporary file: {filename}")
            except Exception as e:
                print(f"X   Error deleting {filename}: {e}")

            time.sleep(0.5) 

        except StopIteration:
            print(f"X  ERROR: Found v{latest_version} but could not find {versioned_id}?")
            with download_lock:
                stats["deleted_version"] += 1
            continue 
        
        except Exception as e:
            # ---- any other error → thực sự failed ----
            print(f"X Download error for {versioned_id}: {e}")
            continue
            
    if versions_downloaded == 0 and latest_version > 0:
        print(f"X  ERROR: Found v{latest_version} but could not download any versions.")
        return False
    elif versions_downloaded == latest_version:
        with download_lock:
            stats["downloaded"] += 1
        print(f"  Successfully downloaded and extracted: {versions_downloaded} / {latest_version} versions for {arxiv_id}.")
        return True
    else:
        print(f"X  ERROR: Only downloaded {versions_downloaded} / {latest_version} versions for {arxiv_id}.")
        return False

def download_single_paper(arxiv_id, save_dir, count_stats=True):
    """Wrapper function for parallel execution"""
    success = get_source_all_versions(arxiv_id, save_dir)
    if count_stats and not success:
        with download_lock:
            stats["failed_download"] += 1
    return arxiv_id, success


def download_arxiv_range_parallel(start_month, start_id, end_month, end_id, 
                                    save_dir="./23127238", max_parallels=20):
    
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    # Reset stats
    for k in stats:
        stats[k] = 0
    
    max_consecutive_failures = 3
    
    print(f"Starting parallel download with {max_parallels} parallels")
    print(f"Range: {start_prefix}.{start_id:05d} to {end_prefix}.{end_id:05d}")
    print(f"Saving to directory: {save_dir}\n")
    
    if start_month == end_month:
        print(f"Phase: Downloading all from {start_month} ({start_id} → {end_id})...")
        
        arxiv_ids = [f"{start_prefix}.{current_id:05d}" for current_id in range(start_id, end_id + 1)]
        
        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            future_to_id = {
                executor.submit(download_single_paper, arxiv_id, save_dir): arxiv_id 
                for arxiv_id in arxiv_ids
            }
            
            completed = 0
            for future in as_completed(future_to_id):
                arxiv_id = future_to_id[future]
                completed += 1
                
                try:
                    paper_id, success = future.result()
                    status = "O" if success else "X"
                    print(f"[{completed}/{len(arxiv_ids)}] {status} {paper_id}")
                except Exception as e:
                    print(f"[{completed}/{len(arxiv_ids)}] ✗ {arxiv_id} - Exception: {e}")
        
        print(f"Finished {start_month}.\n")
    
    else:
        # Phase 1: Download from start_month with sliding window
        print(f"Phase 1: Downloading from {start_month} starting at ID {start_id}...")
        
        current_id = start_id
        failed_consecutive = 0
        completed_count = 0
        should_stop = False
        
        # Track ID của lần thành công cuối cùng
        last_success_id = start_id - 1
        
        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            active_futures = {}
            results_buffer = {}
            next_id_to_process = start_id
            
            while not should_stop:
                # Submit new tasks
                while len(active_futures) < max_parallels and not should_stop:
                    arxiv_id = f"{start_prefix}.{current_id:05d}"
                    # Không count stats cho các probe downloads
                    future = executor.submit(download_single_paper, arxiv_id, save_dir, count_stats=False)
                    active_futures[future] = (arxiv_id, current_id)
                    current_id += 1

                if active_futures:
                    for done_future in as_completed(list(active_futures.keys())):
                        arxiv_id, id_num = active_futures.pop(done_future)
                        
                        try:
                            paper_id, success = done_future.result()
                            results_buffer[id_num] = (paper_id, success)
                        except Exception as e:
                            print(f"X {arxiv_id} - Exception: {e}")
                            results_buffer[id_num] = (arxiv_id, False)
                        
                        # Process results in order
                        while next_id_to_process in results_buffer:
                            paper_id, success = results_buffer.pop(next_id_to_process)
                            completed_count += 1
                            status = "O" if success else "X"
                            
                            # Update counters trước
                            if success:
                                failed_consecutive = 0
                                last_success_id = next_id_to_process
                            else:
                                failed_consecutive += 1
                            
                            # Check nếu đạt điều kiện stop
                            if failed_consecutive >= max_consecutive_failures:
                                print(f"\nReached {max_consecutive_failures} consecutive failures at ID {next_id_to_process}.")
                                print(f"  Last successful ID: {last_success_id}")
                                print(f"  Stopping submission. Discarding all failures after last success...")
                                should_stop = True
                            
                            # Chỉ count nếu paper nằm TRƯỚC hoặc BẰNG last_success_id
                            # Tất cả papers sau last_success_id đều là probe
                            if next_id_to_process <= last_success_id or (not should_stop and success):
                                with download_lock:
                                    if success:
                                        stats["downloaded"] += 1
                                    else:
                                        stats["failed_download"] += 1
                                
                                print(f"[Phase 1: {completed_count}] {status} {paper_id}")
                            else:
                                # Đây là probe download (sau last_success_id), không count
                                print(f"[Phase 1: {completed_count}] {status} {paper_id} (probe - discarded)")
                            
                            next_id_to_process += 1

                        if should_stop:
                            break

                if not active_futures:
                    break
        
        print(f"Completed {start_month}.\n")
        
        # Phase 2: Download from end_month
        print(f"Phase 2: Downloading from {end_month} starting at ID 1, going forward to ID {end_id}...")
        
        phase2_ids = [f"{end_prefix}.{current_id:05d}" for current_id in range(1, end_id + 1)]
        
        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            future_to_id = {
                executor.submit(download_single_paper, arxiv_id, save_dir): arxiv_id 
                for arxiv_id in phase2_ids
            }
            
            completed = 0
            for future in as_completed(future_to_id):
                arxiv_id = future_to_id[future]
                completed += 1
                
                try:
                    paper_id, success = future.result()
                    status = "O" if success else "X"
                    print(f"[Phase 2: {completed}/{len(phase2_ids)}] {status} {paper_id}")
                except Exception as e:
                    print(f"[Phase 2: {completed}/{len(phase2_ids)}] ✗ {arxiv_id} - Exception: {e}")
        
        print(f"Reached end ID {end_id} in {end_month}.")
    
    print()
    print("=" * 100)
    print(f"Download, extraction, and cleaning complete!")
    print(f"  Papers fully downloaded  : {stats['downloaded']}")
    print(f"  Real download failures   : {stats['failed_download']}")
    print(f"  Versions deleted (404)   : {stats['deleted_version']}")
    print(f"  Versions extracted       : {stats['extracted']}")
    print(f"  Extract failures         : {stats['failed_extract']}")
    print(f"  PDFs detected            : {stats['pdfs']}")
    print(f"  Other files deleted      : {stats['deleted_files']}")
    print(f"  Output folder            : {os.path.abspath('./23127238')}")
    print("=" * 100)
    print()


if __name__ == "__main__":
    # Configuration
    START_MONTH = "2023-05"
    START_ID = 9938
    END_MONTH = "2023-05"
    END_ID = 9950
    SAVE_DIR = "./23127238"
    MAX_PARALLELS = 20
    
    
    # # Đạt
    # START_MONTH = "2023-04"
    # START_ID = 14607
    # END_MONTH = "2023-05"
    # END_ID = 4592
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 20
    
    
    # # Nhân
    # START_MONTH = "2023-05"
    # START_ID = 4593
    # END_MONTH = "2023-05"
    # END_ID = 9594
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 20
    
    
    # #Việt
    # START_MONTH = "2023-05"
    # START_ID = 9595
    # END_MONTH = "2023-05"
    # END_ID = 14596
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 20
    
    
    
    download_arxiv_range_parallel(
        start_month=START_MONTH,
        start_id=START_ID,
        end_month=END_MONTH,
        end_id=END_ID,
        save_dir=SAVE_DIR,
        max_parallels=MAX_PARALLELS
    )