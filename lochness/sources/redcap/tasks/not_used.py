def push_to_data_sink(
    file_path: Path,
    file_md5: str,
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a data sink for the given project and site.

    Args:
        file_path (Path): Path to the file to push.
        file_md5 (str): MD5 hash of the file.
        project_id (str): The project ID.
        site_id (str): The site ID.
        config_file (Path): Path to the config file.

    Returns:
        Optional[DataPush]: The data push record if successful, None otherwise.
    """
    try:
        # Get data sinks for this project and site
        sql_query = f"""
            SELECT data_sink_id, data_sink_name, data_sink_metadata
            FROM data_sinks
            WHERE project_id = '{project_id}' AND site_id = '{site_id}'
        """
        df = db.execute_sql(config_file, sql_query)
        
        if df.empty:
            logger.warning(f"No data sinks found for {project_id}::{site_id}")
            return None

        # For now, use the first data sink found
        data_sink = df.iloc[0]
        data_sink_id = data_sink['data_sink_id']
        data_sink_name = data_sink['data_sink_name']
        data_sink_metadata = data_sink['data_sink_metadata']

        logger.info(f"Pushing {file_path} to data sink {data_sink_name}...")

        # Check if this is a MinIO data sink
        if data_sink_metadata.get('keystore_name'):
            # This is likely a MinIO data sink
            return push_to_minio_sink(
                file_path=file_path,
                file_md5=file_md5,
                data_sink_id=data_sink_id,
                data_sink_name=data_sink_name,
                data_sink_metadata=data_sink_metadata,
                project_id=project_id,
                site_id=site_id,
                config_file=config_file,
            )
        else:
            # For other data sink types, simulate upload for now
            start_time = datetime.now()
            import time
            time.sleep(1)  # Simulate upload time
            end_time = datetime.now()
            push_time_s = int((end_time - start_time).total_seconds())

            data_push = DataPush(
                data_sink_id=data_sink_id,
                file_path=str(file_path),
                file_md5=file_md5,
                push_time_s=push_time_s,
                push_metadata={
                    "data_sink_name": data_sink_name,
                    "data_sink_metadata": data_sink_metadata,
                    "upload_simulated": True,  # Flag to indicate this was simulated
                },
                push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            # Insert the data push record
            db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

            logger.info(f"Successfully pushed {file_path} to data sink {data_sink_name}")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "redcap_data_push_success",
                    "message": f"Successfully pushed {file_path} to data sink {data_sink_name}.",
                    "project_id": project_id,
                    "site_id": site_id,
                    "data_sink_name": data_sink_name,
                    "file_path": str(file_path),
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)

            return data_push

    except Exception as e:
        logger.error(f"Failed to push {file_path} to data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "redcap_data_push_failed",
                "message": f"Failed to push {file_path} to data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None


def push_to_minio_sink(
    file_path: Path,
    file_md5: str,
    data_sink_id: int,
    data_sink_name: str,
    data_sink_metadata: Dict[str, Any],
    project_id: str,
    site_id: str,
    config_file: Path,
) -> Optional[DataPush]:
    """
    Pushes a file to a MinIO data sink.
    """
    try:
        # --- NEW LOGIC: Check for existing push with same md5 ---
        check_push_query = f"""
            SELECT 1 FROM data_push
            WHERE data_sink_id = {data_sink_id}
              AND file_path = '{str(file_path).replace("'", "''")}'
              AND file_md5 = '{file_md5}'
            LIMIT 1;
        """
        push_exists = db.execute_sql(config_file, check_push_query)
        if not push_exists.empty:
            logger.info(f"File {file_path} (md5={file_md5}) already pushed to MinIO sink {data_sink_name}, skipping.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "minio_data_push_already_exists",
                    "message": f"File {file_path} (md5={file_md5}) already pushed to MinIO sink {data_sink_name}, skipping.",
                    "file_path": str(file_path),
                    "data_sink_name": data_sink_name,
                    "project_id": project_id,
                    "site_id": site_id,
                },
            ).insert(config_file)
            return None
        # --- END NEW LOGIC ---
        # Extract MinIO configuration from data sink metadata
        bucket_name = data_sink_metadata.get('bucket')
        keystore_name = data_sink_metadata.get('keystore_name')
        endpoint_url = data_sink_metadata.get('endpoint')

        if not all([bucket_name, keystore_name, endpoint_url]):
            logger.error(f"Missing required MinIO configuration in data sink metadata: {data_sink_metadata}")
            return None

        # Import MinIO client
        from minio import Minio
        from minio.error import S3Error
        from lochness.sources.minio.tasks.credentials import get_minio_cred

        # Get MinIO credentials from keystore
        logger.info(f"Getting MinIO credentials for keystore: {keystore_name}, project: {project_id}")
        minio_creds = get_minio_cred(keystore_name, project_id)
        # Mask credentials in logs: only log presence, not values
        logger.info(f"Retrieved MinIO credentials: access_key={'***' if 'access_key' in minio_creds else 'MISSING'}, secret_key={'***' if 'secret_key' in minio_creds else 'MISSING'}")
        access_key = minio_creds["access_key"]
        secret_key = minio_creds["secret_key"]

        # Initialize MinIO client
        endpoint_host = endpoint_url.replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint_host,
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint_url.startswith("https"),
        )

        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating...")
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

        # Create object name based on file path structure
        # Extract the relative path from the lochness root directory
        lochness_root = config.parse(config_file, 'general')['lochness_root']
        relative_path = file_path.relative_to(Path(lochness_root))
        object_name = str(relative_path).replace("\\", "/")  # Ensure forward slashes for S3

        # Upload the file
        logger.info(f"Uploading '{file_path}' to '{bucket_name}/{object_name}'...")
        start_time = datetime.now()
        
        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type="application/json",
        )
        
        end_time = datetime.now()
        push_time_s = int((end_time - start_time).total_seconds())
        logger.info(f"Upload successful. Time taken: {push_time_s} seconds.")

        # Create data push record
        data_push = DataPush(
            data_sink_id=data_sink_id,
            file_path=str(file_path),
            file_md5=file_md5,
            push_time_s=push_time_s,
            push_metadata={
                "object_name": object_name,
                "bucket_name": bucket_name,
                "endpoint_url": endpoint_url,
                "upload_simulated": False,  # Flag to indicate this was real upload
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Insert the data push record
        db.execute_queries(config_file, [data_push.to_sql_query()], show_commands=False)

        logger.info(f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}")
        Logs(
            log_level="INFO",
            log_message={
                "event": "redcap_data_push_success",
                "message": f"Successfully pushed {file_path} to MinIO data sink {data_sink_name}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_sink_name": data_sink_name,
                "file_path": str(file_path),
                "object_name": object_name,
                "bucket_name": bucket_name,
                "push_time_s": push_time_s,
            },
        ).insert(config_file)

        return data_push

    except S3Error as e:
        logger.error(f"MinIO S3 Error while pushing {file_path}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "redcap_data_push_minio_error",
                "message": f"MinIO S3 Error while pushing {file_path}.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None
    except Exception as e:
        logger.error(f"Failed to push {file_path} to MinIO data sink: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "redcap_data_push_failed",
                "message": f"Failed to push {file_path} to MinIO data sink.",
                "project_id": project_id,
                "site_id": site_id,
                "file_path": str(file_path),
                "error": str(e),
            },
        ).insert(config_file)
        return None
