import shlex
import subprocess
from collections.abc import Sequence

import psutil


def run(
    cmd: str | Sequence,  # type: ignore[type-arg]
    split: bool = True,
    print_output: bool = False,
    **kwargs: str,
) -> subprocess.CompletedProcess:  # type: ignore[type-arg]
    if isinstance(cmd, str) and split:
        cmd = shlex.split(cmd)

    result = subprocess.run(
        cmd,
        input="",
        capture_output=True,
        check=False,
        **kwargs,  # type: ignore[call-overload]
    )
    result.stdout = result.stdout.decode("utf-8")
    result.stderr = result.stderr.decode("utf-8")
    # Avoid printing in tests to keep output clean; caller can inspect stdout
    return result  # type: ignore[no-any-return]


class ChildTerminatingPopen(subprocess.Popen):  # type: ignore[type-arg]
    """Extend subprocess.Popen class to automatically kill child processes when terminated."""

    def __init__(self, cmd: Sequence[str], **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)

    def terminate(self) -> None:  # noqa: D401
        """Terminate process and children"""
        try:
            proc = psutil.Process(self.pid)
            procs = [proc] + proc.children(recursive=True)
        except psutil.NoSuchProcess:
            return
        for p in procs:
            try:
                p.terminate()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(procs)
