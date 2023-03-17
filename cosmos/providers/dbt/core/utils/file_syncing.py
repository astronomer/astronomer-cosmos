import fcntl
import logging

logger = logging.getLogger(__name__)


def exclude(src_dir, contents):
    # these are the directories that dbt generates -- we don't care to overwrite them
    return ["target", "logs"]


def has_differences(dcmp):
    # looks for anything that's not in tmp/dbt that's in dags/dbt
    differences = dcmp.left_only + dcmp.diff_files
    if len(differences) > 0:
        logging.info(f"Differences detected: {differences}")
        return True
    return any([has_differences(subdcmp) for subdcmp in dcmp.subdirs.values()])


def is_file_locked(file_path):
    # checks a lock file to see if a lock is being held by another process
    try:
        with open(file_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(f, fcntl.LOCK_UN)
        return False
    except OSError:
        return True
