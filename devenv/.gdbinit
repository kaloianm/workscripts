# This is a script with custom gdb configuration. Symlink or copy it to ~/.gdbinit.

set pagination off
set print static-members off
set print max-depth 2
set print pretty on

set history save on
set history size 100
set history filename ~/.gdb_history

add-auto-load-safe-path ~/workspace/mongo*/.gdbinit

# BEGIN - Skip
skip -function "operator new"
skip -function "mongo::Timestamp::Timestamp"
# END - Skip

python
try:
    import gdbmongo
except ImportError:
    import sys
    import gdbmongo

if "gdbmongo" in dir():
    gdbmongo.register_printers()
end
