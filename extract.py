import os
import tarfile
import glob

source_folder = "test"
destination_folder = "extracted_folder"

os.makedirs(destination_folder, exist_ok=True)

tar_files = glob.glob(os.path.join(source_folder, "*.tar.gz"))

print(f"Tìm thấy {len(tar_files)} file .tar.gz")

for tar_path in tar_files:
    file_name = os.path.basename(tar_path)
    base_name = file_name.replace('.tar.gz', '')
    
    extract_path = os.path.join(destination_folder, base_name)
    os.makedirs(extract_path, exist_ok=True)
    
    print(f"Đang extract {file_name}...")
    
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            tar.extractall(path=extract_path)
        print(f"✓ Hoàn thành extract {file_name} vào {extract_path}")
    except Exception as e:
        print(f"✗ Lỗi khi extract {file_name}: {str(e)}")

print(f"\nĐã extract tất cả file vào folder '{destination_folder}'")






print("\n" + "="*50)
print("Bắt đầu xóa figures...")
print("="*50)


image_extensions = [
    '*.png', '*.jpg', '*.jpeg', '*.gif', '*.bmp', 
    '*.pdf', '*.eps', '*.svg', '*.tif', '*.tiff',
    '*.PNG', '*.JPG', '*.JPEG', '*.PDF', '*.EPS'
]

deleted_count = 0


for root, dirs, files in os.walk(destination_folder):
    for ext in image_extensions:
        pattern = os.path.join(root, ext)
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                deleted_count += 1
                print(f"✓ Đã xóa: {file_path}")
            except Exception as e:
                print(f"✗ Không thể xóa {file_path}: {str(e)}")
    
    for dir_name in dirs[:]:
        if dir_name.lower() in ['figures', 'figs', 'images', 'figure']:
            dir_path = os.path.join(root, dir_name)
            try:
                import shutil
                shutil.rmtree(dir_path)
                print(f"✓ Đã xóa folder: {dir_path}")
                dirs.remove(dir_name)
            except Exception as e:
                print(f"✗ Không thể xóa folder {dir_path}: {str(e)}")

print(f"\n{'='*50}")
print(f"Hoàn thành! Đã xóa {deleted_count} file hình ảnh")
print(f"{'='*50}")