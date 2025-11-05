import os
import re
import tarfile
import glob
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Thread-safe locks and counters
stats_lock = Lock()
stats = {"extracted": 0, "failed": 0, "deleted_files": 0, "deleted_folders": 0}

source_folder = "sources"
destination_folder = "extracted_and_cleaned_figures"

def extract_and_clean_single_tar(tar_path, destination_folder):
    """
    Extract a single tar.gz file and clean figures from it
    Returns: (tar_filename, success, deleted_files_count, deleted_folders_count)
    """
    file_name = os.path.basename(tar_path)
    #base_name = file_name.replace('.tar.gz', '')
    base_name = re.sub(r'\.tar(\.gz)?$', '', file_name)
    
    extract_path = os.path.join(destination_folder, base_name)
    os.makedirs(extract_path, exist_ok=True)
    
    local_deleted_files = 0
    local_deleted_folders = 0
    
    # Extract the tar file
    try:
        #with tarfile.open(tar_path, 'r:gz') as tar:
        with tarfile.open(tar_path, 'r:*') as tar:
            tar.extractall(path=extract_path)
        print(f"✓ Extracted {file_name}")
    except Exception as e:
        print(f"✗ Error extracting {file_name}: {str(e)}")
        return (file_name, False, 0, 0)
    
    # Clean figures from the extracted content
    image_extensions = [
        '*.png', '*.jpg', '*.jpeg', '*.gif', '*.bmp', 
        '*.pdf', '*.eps', '*.svg', '*.tif', '*.tiff',
        '*.PNG', '*.JPG', '*.JPEG', '*.PDF', '*.EPS'
    ]
    
    try:
        # Delete image files
        for root, dirs, files in os.walk(extract_path):
            for ext in image_extensions:
                pattern = os.path.join(root, ext)
                for file_path in glob.glob(pattern):
                    try:
                        os.remove(file_path)
                        local_deleted_files += 1
                    except Exception as e:
                        print(f"✗ Cannot delete {file_path}: {str(e)}")
            
            # Delete figure folders
            for dir_name in dirs[:]:
                if dir_name.lower() in ['figures', 'figure', 'figs', 'fig', 'images', 'image', 'imgs', 'img', 'media']:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        shutil.rmtree(dir_path)
                        local_deleted_folders += 1
                        dirs.remove(dir_name)
                    except Exception as e:
                        print(f"✗ Cannot delete folder {dir_path}: {str(e)}")
                        
        for root, dirs, files in os.walk(extract_path, topdown=False):
            if root == extract_path:
                continue
            if not dirs and not files:
                try:
                    os.rmdir(root)
                    print(f"🗑️ Removed empty folder: {root}")
                except Exception as e:
                    print(f"✗ Cannot remove empty folder {root}: {str(e)}")
                    
        return (file_name, True, local_deleted_files, local_deleted_folders)
        
    except Exception as e:
        print(f"✗ Error cleaning {file_name}: {str(e)}")
        for root, dirs, files in os.walk(extract_path, topdown=False):
            if root == extract_path:
                continue
            if not dirs and not files:
                try:
                    os.rmdir(root)
                    print(f"🗑️ Removed empty folder: {root}")
                except Exception as e:
                    print(f"✗ Cannot remove empty folder {root}: {str(e)}")
        return (file_name, True, local_deleted_files, local_deleted_folders)

def parallel_extract_and_clean(source_folder, destination_folder, max_workers=20):
    """
    Extract and clean tar.gz files in parallel
    
    Args:
        source_folder: Folder containing .tar.gz files
        destination_folder: Folder to extract files to
        max_workers: Number of parallel threads (default: 20)
    """
    
    os.makedirs(destination_folder, exist_ok=True)
    
    # Get all tar.gz files
    #tar_files = glob.glob(os.path.join(source_folder, "*.tar.gz"))
    tar_files = glob.glob(os.path.join(source_folder, "**", "*.tar*"), recursive=True)

    print(f"Found {len(tar_files)} .tar.gz files")
    print(f"Starting parallel extraction with {max_workers} workers")
    print(f"Extracting to: {destination_folder}\n")
    print("="*50)
    
    # Reset stats
    stats["extracted"] = 0
    stats["failed"] = 0
    stats["deleted_files"] = 0
    stats["deleted_folders"] = 0
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_tar = {
            executor.submit(extract_and_clean_single_tar, tar_path, destination_folder): tar_path
            for tar_path in tar_files
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_tar):
            tar_path = future_to_tar[future]
            completed += 1
            
            try:
                file_name, success, deleted_files, deleted_folders = future.result()
                
                with stats_lock:
                    if success:
                        stats["extracted"] += 1
                    else:
                        stats["failed"] += 1
                    stats["deleted_files"] += deleted_files
                    stats["deleted_folders"] += deleted_folders
                
                status = "✓" if success else "✗"
                print(f"[{completed}/{len(tar_files)}] {status} {file_name} (deleted: {deleted_files} files, {deleted_folders} folders)")
                
            except Exception as e:
                file_name = os.path.basename(tar_path)
                print(f"[{completed}/{len(tar_files)}] ✗ {file_name} - Exception: {e}")
                with stats_lock:
                    stats["failed"] += 1
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Extraction and cleaning complete!")
    print(f"Successfully extracted: {stats['extracted']} files")
    print(f"Failed: {stats['failed']} files")
    print(f"Total deleted image files: {stats['deleted_files']}")
    print(f"Total deleted figure folders: {stats['deleted_folders']}")
    print(f"Files saved to: {os.path.abspath(destination_folder)}")
    print(f"{'='*50}")

# Run the parallel extraction
if __name__ == "__main__":
    parallel_extract_and_clean(
        source_folder="sources",
        destination_folder="extracted_and_cleaned_figures",
        max_workers=20
    )