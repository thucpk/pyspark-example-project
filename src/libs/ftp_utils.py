from ftplib import FTP


def chdir(ftp: FTP, directory: str):
    ch_dir_rec(ftp, directory.split('/'))


def directory_exists(ftp: FTP, directory: str):
    lst_file = []
    ftp.retrlines('LIST', lst_file.append)
    for f in lst_file:
        if f.split()[-1] == directory and f.upper().startswith('D'):
            return True
    return False


def ch_dir_rec(ftp: FTP, descending_path_split: list):
    if len(descending_path_split) == 0:
        return

    next_level_directory = descending_path_split.pop(0)

    if not directory_exists(ftp, next_level_directory):
        ftp.mkd(next_level_directory)
    ftp.cwd(next_level_directory)
    ch_dir_rec(ftp, descending_path_split)
