import arxiv
import os
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import deque

# Thread-safe locks and counters
download_lock = Lock()
stats = {
    "downloaded": 0, 
    "failed": 0
}

def get_source_all_versions(arxiv_id, save_dir="./sources"):
    os.makedirs(save_dir, exist_ok=True)
    
    client = arxiv.Client()
    versions_downloaded = 0
    latest_version = 0

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
        print(f"X  ERROR when finding latest version of {arxiv_id}: {e}")
        return False

    for v in range(1, latest_version + 1):
        versioned_id = f"{arxiv_id}v{v}"
        
        try:
            search_version = arxiv.Search(id_list=[versioned_id])
            paper_version = next(client.results(search_version))

            filename = f"{versioned_id}.tar.gz"
            print(f"    Downloading: {filename}...")

            paper_version.download_source(dirpath=save_dir, filename=filename)

            versions_downloaded += 1
            time.sleep(0.5) 

        except StopIteration:
            print(f"X  ERROR: Found v{latest_version} but could not find {versioned_id}?")
            continue 
        
        except Exception as e:
            print(f"X  ERROR when downloading {versioned_id}: {e}")
            continue
            
    if versions_downloaded == 0 and latest_version > 0:
        print(f"X  ERROR: Found v{latest_version} but could not download any versions.")
        return False
    elif versions_downloaded == latest_version:
        print(f"  Successfully downloaded: {versions_downloaded} / {latest_version} versions for {arxiv_id}.")
        return True
    else:
        return False

def download_single_paper(arxiv_id, save_dir):
    """Wrapper function for parallel execution"""
    success = get_source_all_versions(arxiv_id, save_dir)
    
    with download_lock:
        if success:
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1
    
    return (arxiv_id, success)

def download_arxiv_range_parallel(start_month, start_id, end_month, end_id, 
                                  save_dir="./sources", max_parallels=20):
    """
    Download arxiv papers in parallel with sliding window approach
    
    Args:
        start_month: Starting month in format 'YYYY-MM'
        start_id: Starting paper ID number
        end_month: Ending month in format 'YYYY-MM'
        end_id: Ending paper ID number
        save_dir: Directory to save downloaded sources
        max_parallels: Number of parallel download threads (default: 20)
    """
    
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon
    
    # Reset stats
    stats["downloaded"] = 0
    stats["failed"] = 0
    
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
                    with download_lock:
                        stats["failed"] += 1
        
        print(f"Finished {start_month}.\n")
    
    else:
        # Phase 1: Download from start_month with sliding window
        print(f"Phase 1: Downloading from {start_month} starting at ID {start_id}...")
        
        current_id = start_id
        failed_consecutive = 0
        completed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            # Dictionary to track futures and their IDs
            active_futures = {}
            results_buffer = {}  # Store results indexed by ID
            next_id_to_process = start_id
            
            # Keep the pipeline full
            while failed_consecutive < max_consecutive_failures:
                # Submit new tasks to keep with parallel pool busy
                while len(active_futures) < max_parallels and failed_consecutive < max_consecutive_failures:
                    arxiv_id = f"{start_prefix}.{current_id:05d}"
                    future = executor.submit(download_single_paper, arxiv_id, save_dir)
                    active_futures[future] = (arxiv_id, current_id)
                    current_id += 1
                
                # Wait for at least one to complete


                # if active_futures:
                #     done, _ = as_completed(active_futures.keys()), None
                    
                #     for future in list(active_futures.keys()):
                #         if future.done():
                #             arxiv_id, id_num = active_futures.pop(future)
                            
                #             try:
                #                 paper_id, success = future.result()
                #                 results_buffer[id_num] = (paper_id, success)
                #             except Exception as e:
                #                 print(f"X {arxiv_id} - Exception: {e}")
                #                 results_buffer[id_num] = (arxiv_id, False)
                    
                #     # Process results in order
                #     while next_id_to_process in results_buffer:
                #         paper_id, success = results_buffer.pop(next_id_to_process)
                #         completed_count += 1
                #         status = "O" if success else "X"
                #         print(f"[Phase 1: {completed_count}] {status} {paper_id}")
                        
                #         if success:
                #             failed_consecutive = 0
                #         else:
                #             failed_consecutive += 1
                            
                #         next_id_to_process += 1
                        
                #         # Check if we should stop
                #         if failed_consecutive >= max_consecutive_failures:
                #             print(f"\nReached {max_consecutive_failures} consecutive failures.")
                #             # Cancel remaining futures
                #             for remaining_future in active_futures.keys():
                #                 remaining_future.cancel()
                #             active_futures.clear()
                #             break
                
                # if not active_futures and failed_consecutive < max_consecutive_failures:
                #     break




                if active_futures:
                    for done_future in as_completed(list(active_futures.keys())):
                        arxiv_id, id_num = active_futures.pop(done_future)
                        
                        try:
                            paper_id, success = done_future.result()
                            results_buffer[id_num] = (paper_id, success)
                        except Exception as e:
                            print(f"X {arxiv_id} - Exception: {e}")
                            results_buffer[id_num] = (arxiv_id, False)
                        
                        # Process results in increasing ID order
                        while next_id_to_process in results_buffer:
                            paper_id, success = results_buffer.pop(next_id_to_process)
                            completed_count += 1
                            status = "O" if success else "X"
                            print(f"[Phase 1: {completed_count}] {status} {paper_id}")
                            
                            if success:
                                failed_consecutive = 0
                            else:
                                failed_consecutive += 1
                            
                            next_id_to_process += 1
                            
                            # Stop when too many consecutive failures
                            if failed_consecutive >= max_consecutive_failures:
                                print(f"\nReached {max_consecutive_failures} consecutive failures.")
                                for remaining_future in active_futures.keys():
                                    remaining_future.cancel()
                                active_futures.clear()
                                break

                        # Exit outer loop if we've hit failure limit
                        if failed_consecutive >= max_consecutive_failures:
                            break

                if not active_futures and failed_consecutive < max_consecutive_failures:
                    break

        
        print(f"Completed {start_month}. No more papers found after {max_consecutive_failures} consecutive failures.\n")
        
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
                    with download_lock:
                        stats["failed"] += 1
        
        print(f"Reached end ID {end_id} in {end_month}.")
    
    print()
    print("=" * 100)
    print(f"Download complete!")
    print(f"Successfully downloaded:    {stats['downloaded']} papers")
    print(f"x   Failed:                 {stats['failed']} papers")
    print(f"Files saved to: {os.path.abspath(save_dir)}")
    print("=" * 100)
    print()


if __name__ == "__main__":
    download_arxiv_range_parallel(
        start_month="2023-04",
        start_id=15000,
        end_month="2023-05", 
        end_id=10,
        save_dir="./sources",
        max_parallels=20
    )
        
# # Đạt
# if __name__ == "__main__":
#     download_arxiv_range_parallel(
#         start_month="2023-04",
#         start_id=14607,
#         end_month="2023-05", 
#         end_id=4592,
#         save_dir="./sources",
#         max_parallels=20
#     )
    
# # Nhân
# if __name__ == "__main__":
#     download_arxiv_range(
#         start_month="2023-05",
#         start_id=4593,
#         end_month="2023-05", 
#         end_id=9594,
#         save_dir="./sources",
#         max_parallels=20
#     )
    
# # Việt 
# if __name__ == "__main__":
#     download_arxiv_range(
#         start_month="2023-05",
#         start_id=9595,
#         end_month="2023-05", 
#         end_id=14596,
#         save_dir="./sources",
#         max_parallels=20
#     )