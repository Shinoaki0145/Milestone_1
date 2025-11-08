import os
import re
import tarfile
import glob
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import gzip

# Thread-safe locks and counters
stats_lock = Lock()
stats = {
    "extracted": 0,
    "failed": 0,
    "deleted_files": 0,
    "pdfs": 0
}

source_folder = "sources"
destination_folder = "extracted_and_cleaned_figures"


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
        with stats_lock:
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


def extract_and_clean_single_tar(tar_path, destination_folder):
    file_name = os.path.basename(tar_path)
    
    fixed_path, filetype, original_name = detect_and_fix_filetype(tar_path)

    if filetype == "pdf":
        return (file_name, True, 0, "pdf")

    if filetype == "unknown":
        print(f"Skipping unsupported format: {file_name}")
        return (file_name, False, 0, "unknown")

    base_name = re.sub(r'\.tar(\.gz)?$', '', file_name)
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

        # Logic không xóa folder (từ các yêu cầu trước) vẫn được giữ nguyên
        # ...

        return (file_name, True, local_deleted_files, "tar_ok")

    except Exception as e:
        print(f"Error cleaning {file_name}: {e}")
        return (file_name, False, local_deleted_files, "tar_fail")
    # ### KẾT THÚC THAY ĐỔI ###


def parallel_extract_and_clean(source_folder, destination_folder, max_parallels=20):
    os.makedirs(destination_folder, exist_ok=True)
    tar_files = glob.glob(os.path.join(source_folder, "**", "*.tar*"), recursive=True)

    print(f"Found {len(tar_files)} files")
    print(f"Starting parallel extraction with {max_parallels} parallels")
    print("=" * 100)
    print()

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
                file_name, success, deleted_files, ftype = future.result()
                with stats_lock:
                    if ftype == "pdf":
                        pass
                    elif success:
                        stats["extracted"] += 1
                    else:
                        stats["failed"] += 1
                    stats["deleted_files"] += deleted_files

                if ftype == "pdf":
                    status = "PDF detected"
                elif success:
                    status = "O Extracted"
                else:
                    status = "X Failed"

                print(f"[{completed}/{len(tar_files)}] {status}: {file_name} "
                      f"(deleted: {deleted_files} files)")

            except Exception as e:
                file_name = os.path.basename(tar_path)
                print(f"[{completed}/{len(tar_files)}] ✗ {file_name} - Exception: {e}")
                with stats_lock:
                    stats["failed"] += 1
    
    print()
    print("=" * 100)
    print("Extraction and cleaning complete!\n")
    print(f"Extracted archives (TAR/GZ): {stats['extracted']}")
    print(f"Detected PDF files:        {stats['pdfs']}")
    print(f"X    Failed files:           {stats['failed']}")
    print(f"Deleted other files:       {stats['deleted_files']}") # Cập nhật log
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