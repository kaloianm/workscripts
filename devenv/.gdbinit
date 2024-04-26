# This is a script with custom gdb configuration. Symlink or copy it to ~/.gdbinit.

set pagination off
set print static-members off
set print max-depth 2
set print pretty on

add-auto-load-safe-path ~/workspace/mongo*/.gdbinit

python
try:
    import gdbmongo
except ImportError:
    import sys
    if sys.prefix.startswith("/opt/mongodbtoolchain/"):
        import subprocess
        subprocess.run(f"{sys.prefix}/bin/python3 -m pip install gdbmongo", shell=True, check=True)
        import gdbmongo
    else:
        import warnings
        warnings.warn("Not attempting to install gdbmongo into non MongoDB toolchain Python")

if "gdbmongo" in dir():
    gdbmongo.register_printers()
end
