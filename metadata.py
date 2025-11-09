import arxiv
import os
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Thread-safe locks and counters
metadata_lock = Lock()

# Global statistics
stats = {
    "saved": 0,
    "failed": 0,
}

def get_metadata_all_versions(arxiv_id, save_dir):
    """
    Get metadata for all versions of an arXiv paper and save to a JSON file
    """
    client = arxiv.Client()

    # Split arxiv_id into prefix and suffix
    if '.' not in arxiv_id:
        print(f"X  ERROR: Invalid arxiv_id format {arxiv_id}")
        return False

    prefix, suffix = arxiv_id.split('.')
    paper_folder = os.path.join(save_dir, f"{prefix}-{suffix}")
    metadata_path = os.path.join(paper_folder, "metadata.json")

    os.makedirs(paper_folder, exist_ok=True)

    latest_version = 0
    base_paper = None

    try:
        search_base = arxiv.Search(id_list=[arxiv_id])
        base_paper = next(client.results(search_base))
        entry_id_url = base_paper.entry_id
        match = re.search(r'v(\d+)$', entry_id_url)
        latest_version = int(match.group(1)) if match else 1
    except StopIteration:
        print(f"X  ERROR: Not found {arxiv_id} (even v1)")
        return False
    except Exception as e:
        print(f"X  ERROR finding latest version of {arxiv_id}: {e}")
        return False

    print(f"  Found {latest_version} version(s) for {arxiv_id}. Collecting metadata...")

    # Get metadata from base paper (v1)
    final_title = base_paper.title
    final_authors = [author.name for author in base_paper.authors]
    submission_date = base_paper.published.strftime("%Y-%m-%d") if base_paper.published else None
    categories = base_paper.categories
    abstract = base_paper.summary.replace("\n", " ").strip()
    pdf_url = base_paper.pdf_url
    revised_dates = []

    if latest_version > 1:
        for v in range(2, latest_version + 1):
        #for v in range(1, latest_version + 1):
            try:
                version_id = f"{arxiv_id}v{v}"
                search_v = arxiv.Search(id_list=[version_id])
                paper_v = next(client.results(search_v))
                revised_dates.append(paper_v.updated.strftime("%Y-%m-%d") if paper_v.updated else None)
            except:
                revised_dates.append(None)

    if submission_date is None:
        print(f"X  ERROR: No submission date for {arxiv_id} (even v1)")
        return False
    
    
    metadata = {
        "arxiv_id": arxiv_id,
        "paper_title": final_title,
        "authors": final_authors,
        "submission_date": submission_date,
        "revised_dates": revised_dates,
        "latest_version": latest_version,
        "categories": categories,
        "abstract": abstract,
        "pdf_url": pdf_url,
    }

    if base_paper.journal_ref:
        metadata["publication_venue"] = base_paper.journal_ref

    # Save metadata to JSON file
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        print(f"  SUCCESS: Saved metadata.json => {paper_folder}")
        return True
    except Exception as e:
        print(f"X  ERROR saving metadata.json for {arxiv_id}: {e}")
        try:
            os.remove(metadata_path)
        except:
            pass
        return False


def save_metadata_single(arxiv_id, save_dir, count_stats=True):
    """Wrapper function for parallel execution"""
    success = get_metadata_all_versions(arxiv_id, save_dir)
    if count_stats:
        with metadata_lock:
            if success:
                stats["saved"] += 1
            else:
                stats["failed"] += 1
    return arxiv_id, success


def save_metadata_range_parallel(start_month, start_id, end_month, end_id,
                                    save_dir="./23127238", max_parallels=10):
    
    start_year, start_mon = start_month.split('-')
    end_year, end_mon = end_month.split('-')
    start_prefix = start_year[2:] + start_mon
    end_prefix = end_year[2:] + end_mon

    # Reset stats
    for k in stats:
        stats[k] = 0

    max_consecutive_failures = 3
    print(f"Starting metadata download (parallel: {max_parallels})")
    print(f"Range: {start_prefix}.{start_id:05d} => {end_prefix}.{end_id:05d}")

    if start_month == end_month:
        print(f"Phase: Processing {start_month} ({start_id} => {end_id})...")
        arxiv_ids = [f"{start_prefix}.{i:05d}" for i in range(start_id, end_id + 1)]

        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            future_to_id = {executor.submit(save_metadata_single, aid, save_dir): aid for aid in arxiv_ids}
            completed = 0
            for future in as_completed(future_to_id):
                arxiv_id = future_to_id[future]
                completed += 1
                try:
                    _, success = future.result()
                    status = "SUCCESS" if success else "FAILED"
                    print(f"[{completed}/{len(arxiv_ids)}] {status} {arxiv_id}")
                except Exception as e:
                    print(f"[{completed}/{len(arxiv_ids)}] EXCEPTION {arxiv_id} - {e}")

        print(f"Finished {start_month}.\n")

    else:
        # Phase 1: Download from start_month with sliding window
        print(f"Phase 1: Processing {start_month} from ID {start_id} → end of month...")
        current_id = start_id
        failed_consecutive = 0
        completed_count = 0
        should_stop = False
        
        # Track ID of last successful download
        last_success_id = start_id - 1

        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            active_futures = {}
            results_buffer = {}
            next_id_to_process = start_id

            while not should_stop:
                # Submit new tasks
                while len(active_futures) < max_parallels and not should_stop:
                    arxiv_id = f"{start_prefix}.{current_id:05d}"
                    # Do not count stats for probe downloads
                    future = executor.submit(save_metadata_single, arxiv_id, save_dir, count_stats=False)
                    active_futures[future] = (arxiv_id, current_id)
                    current_id += 1

                if not active_futures:
                    break

                for done_future in as_completed(list(active_futures.keys())):
                    arxiv_id, id_num = active_futures.pop(done_future)
                    try:
                        _, success = done_future.result()
                        results_buffer[id_num] = (arxiv_id, success)
                    except Exception as e:
                        results_buffer[id_num] = (arxiv_id, False)

                    # Process results in order
                    while next_id_to_process in results_buffer:
                        paper_id, success = results_buffer.pop(next_id_to_process)
                        completed_count += 1
                        status = "SUCCESS" if success else "FAILED"

                        # Update counters before
                        if success:
                            failed_consecutive = 0
                            last_success_id = next_id_to_process
                        else:
                            failed_consecutive += 1

                        # Check if reached stop condition
                        if failed_consecutive >= max_consecutive_failures:
                            print(f"\n{max_consecutive_failures} consecutive failures at ID {next_id_to_process}")
                            print(f"  Last success: {start_prefix}.{last_success_id:05d}")
                            should_stop = True

                        # Only count if paper is BEFORE or EQUAL to last_success_id
                        if next_id_to_process <= last_success_id or (not should_stop and success):
                            with metadata_lock:
                                if success:
                                    stats["saved"] += 1
                                else:
                                    stats["failed"] += 1
                            print(f"[Phase 1: {completed_count}] {status} {paper_id}")
                        else:
                            print(f"[Phase 1: {completed_count}] {status} {paper_id} (probe)")

                        next_id_to_process += 1

                    if should_stop:
                        break

        print(f"Completed {start_month}.\n")

        # Phase 2: Download from end_month
        print(f"Phase 2: Processing {end_month} from 00001 => {end_id}...")
        phase2_ids = [f"{end_prefix}.{i:05d}" for i in range(1, end_id + 1)]

        with ThreadPoolExecutor(max_workers=max_parallels) as executor:
            future_to_id = {executor.submit(save_metadata_single, aid, save_dir): aid for aid in phase2_ids}
            completed = 0
            for future in as_completed(future_to_id):
                arxiv_id = future_to_id[future]
                completed += 1
                try:
                    _, success = future.result()
                    status = "SUCCESS" if success else "FAILED"
                    print(f"[Phase 2: {completed}/{len(phase2_ids)}] {status} {arxiv_id}")
                except Exception as e:
                    print(f"[Phase 2: {completed}/{len(phase2_ids)}] EXCEPTION {arxiv_id} - {e}")

        print(f"Reached end ID {end_id} in {end_month}")


    
    print()
    print("=" * 100)
    print("METADATA PROCESSING COMPLETE!")
    print(f"  Successfully saved    : {stats['saved']}")
    print(f"  Failed to save        : {stats['failed']}")
    print(f"  Output folder         : {os.path.abspath(save_dir)}")
    print("=" * 100)
    print()


if __name__ == "__main__":
    # Configuration
    
    # TEST
    START_MONTH = "2023-05"
    START_ID = 9938
    END_MONTH = "2023-05"
    END_ID = 9950
    SAVE_DIR = "./23127238"
    MAX_PARALLELS = 10
    
    
    # # BÁ ĐẠT
    # START_MONTH = "2023-04"
    # START_ID = 14607
    # END_MONTH = "2023-05"
    # END_ID = 4592
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 10


    # # THIỆN NHÂN
    # START_MONTH = "2023-05"
    # START_ID = 4593
    # END_MONTH = "2023-05"
    # END_ID = 9594
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 10
    
    
    # # NAM VIỆT
    # START_MONTH = "2023-05"
    # START_ID = 9595
    # END_MONTH = "2023-05"
    # END_ID = 14596
    # SAVE_DIR = "./23127238"
    # MAX_PARALLELS = 10



    save_metadata_range_parallel(
        start_month=START_MONTH,
        start_id=START_ID,
        end_month=END_MONTH,
        end_id=END_ID,
        save_dir=SAVE_DIR,
        max_parallels=MAX_PARALLELS
    )