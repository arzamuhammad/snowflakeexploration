import os

def download_folder(minio_client, bucket_name, folder_prefix, local_folder):
    objects = minio_client.list_objects(bucket_name, prefix=folder_prefix)

    for obj in objects:
        # Menghilangkan prefix folder dari nama objek untuk mendapatkan nama file
        filename = obj.object_name[len(folder_prefix):]
        local_file_path = os.path.join(local_folder, filename)

        # Membuat direktori jika belum ada
        local_file_directory = os.path.dirname(local_file_path)
        if not os.path.exists(local_file_directory):
            os.makedirs(local_file_directory)

        # Download file
        minio_client.fget_object(bucket_name, obj.object_name, local_file_path)
        # minio_client.fget_object(bucket_name, obj.object_name, local_folder)
        print(f"Downloaded {obj.object_name} to {local_file_path}")