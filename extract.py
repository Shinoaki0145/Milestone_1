import os
import re
import tarfile
import glob
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Thread-safe locks and counters
stats_lock = Lock()
stats = {
    "extracted": 0,
    "failed": 0,
    "deleted_files": 0,
    "deleted_folders": 0,
    "copied_pdfs": 0
}

source_folder = "sources"
destination_folder = "extracted_and_cleaned_figures"

def detect_and_fix_filetype(tar_path):
    """
    Check filetype (tar.gz, PDF, ...)
    If PDF => Convert file extension
    If tar, tar.gz, I call them is tar => Do nothing
    """
    result = subprocess.run(["file", tar_path], capture_output=True, text=True)
    output = result.stdout.strip()

    if "PDF document" in output:
        new_path = re.sub(r'\.tar(\.gz)?$', '.pdf', tar_path)
        # new_path = re.sub(r'\.tar\.gz$', '.pdf', tar_path)

        if not new_path.endswith(".pdf"):
            new_path += ".pdf"

        # Remane root folder
        os.rename(tar_path, new_path)
        pdf_name = os.path.basename(new_path)

        # Copy to folder dest
        # os.makedirs(destination_folder, exist_ok=True)
        # shutil.copy(new_path, os.path.join(destination_folder, pdf_name))

        with stats_lock:
            stats["copied_pdfs"] += 1

        print(f"{os.path.basename(tar_path)} => Detected as PDF (converted)")
        return new_path, "pdf"

    elif "gzip compressed data" in output or "tar archive" in output:
        return tar_path, "tar.gz"

    else:
        print(f"Unknown format: {os.path.basename(tar_path)} => {output}")
        return tar_path, "unknown"


def extract_and_clean_single_tar(tar_path, destination_folder):
    file_name = os.path.basename(tar_path)
    fixed_path, filetype = detect_and_fix_filetype(tar_path)

    if filetype == "pdf":
        # Not record fail
        return (file_name, True, 0, 0, "pdf")

    if filetype != "tar.gz":
        print(f"Skipping unsupported format: {file_name}")
        return (file_name, False, 0, 0, "unknown")

    base_name = re.sub(r'\.tar(\.gz)?$', '', file_name)
    # base_name = re.sub(r'\.tar\.gz$', '', file_name)

    extract_path = os.path.join(destination_folder, base_name)
    os.makedirs(extract_path, exist_ok=True)

    local_deleted_files = 0
    local_deleted_folders = 0

    try:
        with tarfile.open(fixed_path, 'r:*') as tar:
        # with tarfile.open(fixed_path, 'r:gz') as tar:
            tar.extractall(path=extract_path)
        print(f"Extracted {file_name}")
    except Exception as e:
        print(f"X   Error extracting {file_name}: {e}")
        return (file_name, False, 0, 0, "tar_fail")

    # Remove figures
    figure_extensions = [
        '*.png', '*.jpg', '*.jpeg', '*.gif', '*.bmp',
        '*.pdf', '*.eps', '*.svg', '*.tif', '*.tiff',
        '*.PNG', '*.JPG', '*.JPEG', '*.PDF', '*.EPS'
    ]

    try:
        for root, dirs, files in os.walk(extract_path):
            for ext in figure_extensions:
                for file_path in glob.glob(os.path.join(root, ext)):
                    try:
                        os.remove(file_path)
                        local_deleted_files += 1
                    except Exception as e:
                        print(f"X   Cannot delete {file_path}: {e}")

            # Remove folder figure
            for dir_name in dirs[:]:
                if dir_name.lower() in ['figures', 'figure', 'figs', 'fig', 'images', 'image', 'imgs', 'img']:
                #if dir_name.lower() in ['figures', 'figure', 'figs', 'fig', 'images', 'image', 'imgs', 'img', 'media']:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        shutil.rmtree(dir_path)
                        local_deleted_folders += 1
                        dirs.remove(dir_name)
                    except Exception as e:
                        print(f"X   Cannot delete folder {dir_path}: {e}")

        # Remove empty folder
        for root, dirs, files in os.walk(extract_path, topdown=False):
            if root == extract_path:
                continue
            if not dirs and not files:
                try:
                    os.rmdir(root)
                    print(f"Removed empty folder: {root}")
                except Exception as e:
                    print(f"X   Cannot remove empty folder {root}: {e}")

        return (file_name, True, local_deleted_files, local_deleted_folders, "tar_ok")

    except Exception as e:
        print(f"Error cleaning {file_name}: {e}")
        return (file_name, False, local_deleted_files, local_deleted_folders, "tar_fail")


def parallel_extract_and_clean(source_folder, destination_folder, max_parallels=20):
    os.makedirs(destination_folder, exist_ok=True)
    tar_files = glob.glob(os.path.join(source_folder, "**", "*.tar*"), recursive=True)

    print(f"Found {len(tar_files)} files")
    print(f"Starting parallel extraction with {max_parallels} parallels")
    print("=" * 100)
    print()

    # Reset stats
    for k in stats:
        stats[k] = 0

    with ThreadPoolExecutor(max_workers=max_parallels) as executor:
        future_to_tar = {
            executor.submit(extract_and_clean_single_tar, tar_path, destination_folder): tar_path
            for tar_path in tar_files
        }

        completed = 0
        for future in as_completed(future_to_tar):
            tar_path = future_to_tar[future]
            completed += 1
            try:
                file_name, success, deleted_files, deleted_folders, ftype = future.result()
                with stats_lock:
                    if ftype == "pdf":
                        pass
                    elif success:
                        stats["extracted"] += 1
                    else:
                        stats["failed"] += 1
                    stats["deleted_files"] += deleted_files
                    stats["deleted_folders"] += deleted_folders

                if ftype == "pdf":
                    status = "PDF copied"
                elif success:
                    status = "O Extracted"
                else:
                    status = "X Failed"

                print(f"[{completed}/{len(tar_files)}] {status}: {file_name} "
                      f"(deleted: {deleted_files} files, {deleted_folders} folders)")

            except Exception as e:
                file_name = os.path.basename(tar_path)
                print(f"[{completed}/{len(tar_files)}] ✗ {file_name} - Exception: {e}")
                with stats_lock:
                    stats["failed"] += 1
    
    print()
    print("=" * 100)
    print("Extraction and cleaning complete!\n")
    print(f"Extracted TAR.GZ files: {stats['extracted']}")
    print(f"Convert PDF files:      {stats['copied_pdfs']}")
    print(f"X   Failed files:       {stats['failed']}")
    print(f"Deleted figure files:   {stats['deleted_files']}")
    print(f"Deleted figure folders: {stats['deleted_folders']}")
    print(f"Output folder: {os.path.abspath(destination_folder)}")
    print("=" * 100)
    print()


# Run the parallel extraction
if __name__ == "__main__":
    parallel_extract_and_clean(
        source_folder="sources",
        destination_folder="extracted_and_cleaned_figures",
        max_parallels=20
    )
