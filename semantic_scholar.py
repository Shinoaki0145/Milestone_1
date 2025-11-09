import arxiv
import os
import re
import json
import time
import requests
import shutil

# Global statistics
stats = {
    "processed": 0,
    "failed": 0,
    "rate_limited": 0
}

rate_limited_ids = []


def get_references_json(arxiv_id, save_dir, count_as_failed=True, cleanup_on_probe_fail=False):
    """
    count_as_failed=False → dùng để dò giới hạn, không tính vào failed
    cleanup_on_probe_fail=True → xóa thư mục nếu fail trong quá trình dò
    """
    global rate_limited_ids

    if '.' not in arxiv_id:
        print(f"ERROR: Invalid arXiv ID format: {arxiv_id}")
        if count_as_failed:
            stats["failed"] += 1
        return False

    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    output_path = os.path.join(paper_folder, "references.json")

    # Make temp folder - it will be deleted if probe fails
    os.makedirs(paper_folder, exist_ok=True)

    url = f"https://api.semanticscholar.org/graph/v1/paper/arXiv:{arxiv_id}"
    params = {"fields": "references,references.externalIds"}
    client = arxiv.Client()
    references_dict = {}

    print(f"Querying Semantic Scholar API for {arxiv_id}...")

    try:
        response = requests.get(url, params=params, timeout=30)
        time.sleep(3)

        if response.status_code == 429:
            print(f"429 RATE LIMITED => Adding {arxiv_id} to retry list")
            rate_limited_ids.append(arxiv_id)
            stats["rate_limited"] += 1
            if cleanup_on_probe_fail:
                try:
                    shutil.rmtree(paper_folder)
                    print(f"  Cleaned up probe folder: {paper_folder}")
                except:
                    pass
            return False

        if response.status_code != 200:
            print(f"API ERROR {response.status_code}")
            if count_as_failed:
                stats["failed"] += 1
            if cleanup_on_probe_fail:
                try:
                    shutil.rmtree(paper_folder)
                    print(f"  Cleaned up probe folder: {paper_folder}")
                except:
                    pass
            return False

        data = response.json()
        references = data.get("references", [])
        print(f"Found {len(references)} references...")

        for ref in references:
            if not ref:
                continue
            external_ids = ref.get("externalIds") or {}
            ref_arxiv_id = external_ids.get("ArXiv")
            if not ref_arxiv_id:
                continue

            base_ref_id = re.sub(r'v\d+$', '', ref_arxiv_id)
            if base_ref_id in references_dict:
                continue

            try:
                search = arxiv.Search(id_list=[base_ref_id])
                paper = next(client.results(search))
                ref_meta = {
                    "title": paper.title,
                    "authors": [a.name for a in paper.authors],
                    "submission_date": paper.published.strftime("%Y-%m-%d") if paper.published else None,
                    "semantic_scholar_id": ref.get("paperId")
                }
                references_dict[base_ref_id] = ref_meta
                time.sleep(0.5)
            except StopIteration:
                pass
            except Exception as e:
                print(f"  Warning: Failed metadata for {base_ref_id}: {e}")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(references_dict, f, indent=4, ensure_ascii=False)

        print(f"SUCCESS: Saved {len(references_dict)} refs => {os.path.basename(paper_folder)}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        if count_as_failed:
            stats["failed"] += 1
        if cleanup_on_probe_fail:
            try:
                shutil.rmtree(paper_folder)
                print(f"  Cleaned up probe folder: {paper_folder}")
            except:
                pass
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        if count_as_failed:
            stats["failed"] += 1
        if cleanup_on_probe_fail:
            try:
                shutil.rmtree(paper_folder)
                print(f"  Cleaned up probe folder: {paper_folder}")
            except:
                pass
        return False


def run_semantic_range_sequential(start_month, start_id, end_month, end_id, 
                                    save_dir="./23127238"):
    
    global rate_limited_ids

    # Reset
    stats["processed"] = stats["failed"] = stats["rate_limited"] = 0
    rate_limited_ids = []

    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon

    max_consecutive_failures = 3
    print(f"Starting SEMANTIC processing (sequential, no threads)")
    print(f"Range: {start_prefix}.{start_id:05d} => {end_prefix}.{end_id:05d}")

    if start_month == end_month:
        print(f"Phase: Processing {start_month} ({start_id} => {end_id})...")
        current_id = start_id
        total = end_id - start_id + 1

        for i in range(total):
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\n[{i+1}/{total}] {arxiv_id}")

            success = get_references_json(arxiv_id, save_dir, count_as_failed=True, cleanup_on_probe_fail=False)
            if success:
                stats["processed"] += 1

            current_id += 1

        print(f"Finished {start_month}.\n")

    else:
        # # Phase 1: Download from start_month with sliding window
        # print(f"Phase 1: Probing {start_month} from ID {start_id}...")
        # current_id = start_id
        # failed_consecutive = 0
        # last_success_id = start_id - 1

        # while failed_consecutive < max_consecutive_failures:
        #     arxiv_id = f"{start_prefix}.{current_id:05d}"
        #     print(f"\n[Phase 1] Probing {arxiv_id}")

        #     success = get_references_json(
        #         arxiv_id, save_dir,
        #         count_as_failed=False,
        #         cleanup_on_probe_fail=True
        #     )

        #     if success:
        #         stats["processed"] += 1
        #         failed_consecutive = 0
        #         last_success_id = current_id
        #     else:
        #         failed_consecutive += 1

        #     if failed_consecutive >= max_consecutive_failures:
        #         print(f"\n{max_consecutive_failures} consecutive failures => Stopping Phase 1")
        #         print(f"  Last success: {start_prefix}.{last_success_id:05d}")
        #         break

        #     current_id += 1

        # print(f"Completed {start_month}.\n")
        
        
        
        # Phase 1: Download from start_month with sliding window
        print(f"Phase 1: Probing {start_month} from ID {start_id}...")
        current_id = start_id
        failed_consecutive = 0
        last_success_id = start_id - 1 # Track ID of last successful download
        recent_failed_folders = []  # Save recent failed folders for cleanup

        while failed_consecutive < max_consecutive_failures:
            arxiv_id = f"{start_prefix}.{current_id:05d}"
            print(f"\n[Phase 1] Probing {arxiv_id}")

            # Don't clean up on probe fail yet
            success = get_references_json(
                arxiv_id, save_dir,
                count_as_failed=False,
                cleanup_on_probe_fail=False
            )

            folder_path = os.path.join(save_dir, f"{start_prefix}-{current_id:05d}")

            if success:
                stats["processed"] += 1
                failed_consecutive = 0
                recent_failed_folders.clear()
                last_success_id = current_id
            else:
                failed_consecutive += 1
                recent_failed_folders.append(folder_path)
                
                if len(recent_failed_folders) > max_consecutive_failures:
                    recent_failed_folders.pop(0)

            if failed_consecutive >= max_consecutive_failures:
                print(f"\n{max_consecutive_failures} consecutive failures => Stopping Phase 1")
                print(f"  Last success: {start_prefix}.{last_success_id:05d}")

                print("  Cleaning up last 3 failed folders...")
                for f in recent_failed_folders:
                    try:
                        shutil.rmtree(f)
                        print(f"   - Deleted {f}")
                    except Exception as e:
                        print(f"   - Could not delete {f}: {e}")
                break

            current_id += 1

        print(f"Completed {start_month}.\n")


        # Phase 2: Download from end_month
        print(f"Phase 2: Processing {end_month} from 00001 => {end_id}...")
        for current_id in range(1, end_id + 1):
            arxiv_id = f"{end_prefix}.{current_id:05d}"
            print(f"\n[Phase 2] {arxiv_id}")

            success = get_references_json(arxiv_id, save_dir, count_as_failed=True, cleanup_on_probe_fail=False)
            if success:
                stats["processed"] += 1

        print(f"Reached end ID {end_id} in {end_month}.")

    # Retry rate-limited IDs
    if rate_limited_ids:
        print(f"\n{'='*60}")
        print(f"RETRYING {len(rate_limited_ids)} RATE-LIMITED IDs...")
        print(f"{'='*60}")

        retry_success = 0
        for arxiv_id in rate_limited_ids:
            print(f"\nRetrying {arxiv_id} (30s delay)...")
            time.sleep(30)
            if get_references_json(arxiv_id, save_dir, count_as_failed=True, cleanup_on_probe_fail=False):
                retry_success += 1
                stats["processed"] += 1

        print(f"Retry: {retry_success} succeeded")

    print()
    print("=" * 100)
    print("SEMANTIC PROCESSING COMPLETE!")
    print(f"  Successfully processed : {stats['processed']}")
    print(f"  Failed (real errors)   : {stats['failed']}")
    print(f"  Rate limited (first)   : {stats['rate_limited']}")
    print(f"  Output folder          : {os.path.abspath(save_dir)}")
    print("=" * 100)
    print()


if __name__ == "__main__":
    # Configuration
    
    # TEST
    START_MONTH = "2023-04"
    START_ID = 15010
    END_MONTH = "2023-05"
    END_ID = 1
    SAVE_DIR = "./23127238"
    
    
    # # BÁ ĐẠT
    # START_MONTH = "2023-04"
    # START_ID = 14607
    # END_MONTH = "2023-05"
    # END_ID = 4592
    # SAVE_DIR = "./23127238"
    
    
    # # THIỆN NHÂN
    # START_MONTH = "2023-05"
    # START_ID = 4593
    # END_MONTH = "2023-05"
    # END_ID = 9594
    # SAVE_DIR = "./23127238"
    
    
    # # NAM VIỆT
    # START_MONTH = "2023-05"
    # START_ID = 9595
    # END_MONTH = "2023-05"
    # END_ID = 14596
    # SAVE_DIR = "./23127238"
    
    
    
    run_semantic_range_sequential(
        start_month=START_MONTH,
        start_id=START_ID,
        end_month=END_MONTH,
        end_id=END_ID,
        save_dir=SAVE_DIR
    )