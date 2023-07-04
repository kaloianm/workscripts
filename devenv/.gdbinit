set pagination off
set print static-members off
set print max-depth 2
set print pretty on

add-auto-load-safe-path ~/workspace/mongo*/.gdbinit

python
import gdbmongo
gdbmongo.register_printers()
end