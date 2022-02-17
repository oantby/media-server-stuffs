# Media-Server-Stuffs

These are the utilities I've put together to assist in the lifecycle of my
hobby media server setup. The setup runs roughly as follows:

1. A load balancer runs nginx, proxying to one of `n` data servers
2. The load balancer runs a copy of `heartbeat`, establishing itself as
   the heartbeat _server_, waiting for incoming pings.
3. Each data server runs `heartbeat`, establishing themselves as _clients_,
   reporting their current load, data version, and whatever else I come up with.
4. The data version is updated when `mvie` is used to copy a new file onto
   the **primary** data server. The primary data server reports itself as such
   to the heartbeat server, so the most correct version is known.
5. As of now, all secondary servers do a full sync using a fancied-up `rsync`
   every night. Future updates will result in live data sync when a heartbeat
   client is informed by the heartbeat server that it is out-of-date.

# The Stuffs

## heartbeat

Heartbeat is the meat of the operation. Running as both client and server,
it facilitates heartbeats between the load balancer and the data servers.
It then updates the nginx configuration as necessary to reflect the state
of the servers, and tells it to reload configuration, if changes were made.

Most lines in nginx config are kept and ignored. Lines that match the regex
`/^[\t ]*#(backup_threshold=|down_threshold=)+.*$/` are treated as heartbeat
config lines, and affect the backup and down thresholds of all clients below
them (until another such line sets new threshold(s)). Lines that match the regex
`/^[\t ]*server .*;$/` are treated as client lines, and are searched for in
the list of clients that have been sending pings to establish what changes,
if any, need to be made.

For full usage, see the `usage()` function (or call `heartbeat -h`).

### Force backup / Force down

Any heartbeat client may set the `FORCE_DOWN_BIT` or `FORCE_BACKUP_BIT`
to indicate that the server should be treated as down or backup, respectively.
Note that, while it is nonsensical to set both, a client that does so will
simply be marked as down.

### Thresholds

Clients report their load as (1 minute load / number of cores). If the load
crosses the `backup_threshold`, the client is marked as a backup. If the load
crosses the `down_threshold`, the client is marked as down. It is encouraged
to set these thresholds explicitly in the configuration as 1) the defaults in
the application are naive, 2) how much load you'd like on a client could vary
by the importance of other processes on the client, and 3) not all operating
systems calculate load in the same way (Linux includes processes that are
waiting for I/O in the calculation, MacOS does not; MacOS generally has more
processes, particularly than a non-GUI Linux; etc.).

Example:
```
# this will affect servers .1 and .2
# down_threshold=0.8 backup_threshold=0.4
server 192.168.1.1;
server 192.168.1.2;
# this updates the backup_threshold for .3. down_threshold=0.8 is retained.
# backup_threshold=0.6
server 192.168.1.3;
```

### Data Version

Clients can report a data version in their heartbeats. If a client marked
as primary reports a data version, then any client that reports a different
version is marked as a backup. Note that the presence of multiple primary
clients reporting different versions would result in erratic behavior;
only one client should be marked as a primary.

## mvie

This is my "clever" `mv` that is specific for my `movies` - `mvie`.

`mvie` is very simple, and exists for the purpose of 1) getting my media
into the media directory, which is root-owned, and 2) ensuring that
version numbers and changelog are updated when changes are made, to ensure
secondary clients get marked as backups and only the newest set of files
are served.  The full changelog will serve as a method for an out-of-date
secondary to identify the files it needs to retrieve to get up-to-date in
the future.

See `usage()` or `mvie -h` for usage details.

## verify

Because my method of file transfer doesn't have any form of robust completeness
check and could be subject to internet noise, malicious folks, etc., verify
exists. A future version of `mvie` is expected to, in addition to moving
`file` to `Movies/file`, create `Movies/.file.sha1` holding a checksum of
`file`. This checksum will be copied to secondary servers via nightly rsync
(the method by which video files used to be synced). Then, on some interval,
`verify` runs, finds any newish files that have a checksum available, and
verifies the files against the expected checksums. This avoids me trying to
do a checksum as the file arrives or once it finishes and taking a huge
time penalty in the process.