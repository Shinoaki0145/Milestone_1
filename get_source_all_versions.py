import arxiv
import os
import time
import re


def get_source_all_versions(base_id, save_dir="./sources"):
    
    os.makedirs(save_dir, exist_ok=True)
    
    client = arxiv.Client()
    versions_downloaded = 0
    latest_version = 0

    try:
        search_base = arxiv.Search(id_list=[base_id])
        base_paper = next(client.results(search_base))
        
        entry_id_url = base_paper.entry_id
        
        match = re.search(r'v(\d+)$', entry_id_url)
        
        if not match:
            latest_version = 1
        else:
            latest_version = int(match.group(1)) 
            
        print(f"  Detect version {latest_version} as the latest. Starting download from v1...")

    except StopIteration:
        print(f"  ERROR: No paper found with ID {base_id} (even v1).")
        return False
    except Exception as e:
        print(f"  ERROR when finding latest version of {base_id}: {e}")
        return False

    for v in range(1, latest_version + 1):
        versioned_id = f"{base_id}v{v}"
        
        try:
            search_version = arxiv.Search(id_list=[versioned_id])
            paper_version = next(client.results(search_version))

            filename = f"{versioned_id}.tar.gz"
            print(f"    Downloading: {filename}...")

            paper_version.download_source(dirpath=save_dir, filename=filename)

            versions_downloaded += 1
            time.sleep(0.5) 

        except StopIteration:

            print(f"  ERROR: Found v{latest_version} but could not find {versioned_id}?")
            continue 
        
        except Exception as e:
            print(f"  ERROR when downloading {versioned_id}: {e}")
            continue
            
    if versions_downloaded == 0 and latest_version > 0:
        print(f"  ERROR: Found v{latest_version} but could not download any versions.")
        return False
    elif versions_downloaded == latest_version:
        print(f"  Successfully downloaded: {versions_downloaded} / {latest_version} versions for {base_id}.")
        return True
    else:
        return False