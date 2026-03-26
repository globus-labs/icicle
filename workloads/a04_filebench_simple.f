# NOTE: If Filebench fails with "Unexpected Process termination Code 3",
#       disable ASLR before running this workload:
#         echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
#         sudo sysctl -w kernel.randomize_va_space=0
#
#       Re-enable ASLR after benchmarking:
#         sudo sysctl -w kernel.randomize_va_space=2

define fileset name="testF", entries=100, filesize=16k, dirwidth=10, prealloc, path="/tmp/filebench"

define process name="readerP", instances=1 {
  thread name="readerT", instances=1 {
    flowop openfile      name="openOP",  filesetname="testF"
    flowop readwholefile name="readOP",  filesetname="testF"
    flowop closefile     name="closeOP"
  }
}

run 60
