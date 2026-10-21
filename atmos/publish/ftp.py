"""
Atomic publication over FTPS (plain FTP only with an explicit flag).
Every file is uploaded as <name>.tmp and renamed at the end, JSON last, so a
run that dies mid-upload never leaves mixed state on the server.
"""
import ftplib
import os
import pathlib
from typing import List, Tuple


class PublishError(RuntimeError):
    pass


def _connect(host: str, user: str, password: str, allow_plain: bool):
    try:
        ftp = ftplib.FTP_TLS(); ftp.connect(host, 21, timeout=30)
        ftp.login(user, password); ftp.prot_p(); ftp.set_pasv(True)
        return ftp, "FTPS"
    except Exception as e:
        if not allow_plain:
            raise PublishError(f"FTPS failed and plain FTP not allowed: {e}")
    ftp = ftplib.FTP(); ftp.connect(host, 21, timeout=30)
    ftp.login(user, password); ftp.set_pasv(True)
    return ftp, "FTP (UNENCRYPTED)"


def publish(files: List[Tuple[str, str]], remote_dir: str) -> str:
    """files: [(local_path, remote_name)] in upload order; the last one is renamed last."""
    host = os.environ.get("FTP_HOST") or "ftp.nexplore.pt"   # empty secret must not override the default
    user, pw = os.environ.get("FTP_USER"), os.environ.get("FTP_PASS")
    if not user or not pw:
        return "LOCAL MODE — FTP_USER/FTP_PASS not set"
    allow_plain = os.environ.get("FTP_ALLOW_PLAIN", "0") == "1"
    ftp, mode = _connect(host, user, pw, allow_plain)
    try:
        if remote_dir:
            for part in remote_dir.strip('/').split('/'):
                try:
                    ftp.cwd(part)
                except ftplib.error_perm:
                    ftp.mkd(part); ftp.cwd(part)
        staged = []
        for local, remote in files:
            if not pathlib.Path(local).exists():
                continue
            with open(local, "rb") as f:
                ftp.storbinary(f"STOR {remote}.tmp", f)
            staged.append(remote)
        for remote in staged:
            had_old = True
            try:
                ftp.rename(remote, f"{remote}.bak")
            except ftplib.error_perm:
                had_old = False          # no previous file
            try:
                ftp.rename(f"{remote}.tmp", remote)
            except Exception:
                if had_old:
                    try: ftp.rename(f"{remote}.bak", remote)   # restore previous product
                    except Exception: pass
                raise
            if had_old:
                try: ftp.delete(f"{remote}.bak")
                except Exception: pass
        ftp.quit()
        return f"{mode} → {remote_dir or '/'} · {len(staged)} files"
    except Exception as e:
        try:
            ftp.quit()
        except Exception:
            pass
        raise PublishError(str(e))
