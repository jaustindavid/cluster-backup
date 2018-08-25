# cluster-backup
a minimal-configuration, agent-based solution for backups: specify source ("server") data locations and desired # copies, and available client ("backup") locations + space.  The clients will independently negotiate with servers to copy as much data as they can, such that at least the specified number of copies exist across backup hosts.

Intended usage: many (potentially small) clients around the network holding parts of a few (potentially very large) filesystems.

Config file:
options: PORT: 5005
server: host1:/path/to/directory
copies: 2
server: host2:/path/to/otherdir
copies: 1
backup: host3:/path/for/backups
size: 100gb
backup: host4:/path/for/backups
size: 1t

Files are stored whole / no mangling, so the smallest backup host must have more space than the largest single file.  A single host stores one copy of a file, so if you want N copies please have (at least) N backup hosts.

When working properly, host3 will store files in a subdir under /path/for/backups, and will limit itself to 100gb consumption.  Host4 stoers in /path/for/backups (as configured), and up to 1tb.  Each unique host will get a copy of the config and would run *one* server.py or client.py (or both, if it has both roles).

The backup clients will only store one copy of a given file in their backup dir.  They will be "greedy" and try to fill up their space, so a single infinitely-large backup host would get one copy of everything.

Clients will do their best to hold complete replicas, but no attempt is made to coordinate or to keep a complete copy on one machine.  Given enough allocated storage to the backup hosts (and enough hosts), each server will get (at least) the required number of copies.

The clients & servers communicate via TCP (always from client -> server), and file data is transmitted via rsync (always client -> server).

Multiple backup lines can exist on a single machine; the client code makes no attempt to ensure integrity within a filesystem.  That is, if you put all your backups on multiple "clients" spanning one drive or FS, please be prepared to deal with a failure in that device.

Backups are "greedy", the "copies" thing is more a minimum.  Below "copies" the backup clients will prefer to pull those files; above "copies" they'll sorta stay balanced, but not guarantees.
