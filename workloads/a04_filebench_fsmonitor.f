###########################################################
# Gamma-style read workload for fsmonitor/Icicle
# 50k files, gamma size distribution, mean ~16KB, gamma=1.5
###########################################################

# Tunables
set $dir          = /mnt/exacloud/mdt3
set $nfiles       = 50000
set $meandirwidth = 20
set $filesize     = cvar(type=cvar-gamma,parameters=mean:16384;gamma:1.5)
set $nthreads     = 32

# Fileset definition
define fileset name=gfiles, path=$dir, entries=$nfiles, size=$filesize, dirwidth=$meandirwidth, prealloc=100, paralloc

# Process / threads performing mixed access
define process name=gamma_proc, instances=1 {
    thread name=gamma_thread, memsize=10m, instances=$nthreads {
        flowop openfile      name=open,  filesetname=gfiles
        flowop readwholefile name=read,  filesetname=gfiles
        flowop closefile     name=close
    }
}

# Run long enough to accumulate a lot of ops
run 180
