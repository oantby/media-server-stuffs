/*
This is a setuid binary to copy a given file to the
media server (where everything is root:root), then updating the
data version in the changelog and adding this change to the version history.

see usage for information on changelog, data dir, etc.
*/

#include <iostream>
#include <libgen.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include "sha1.hpp"

using namespace std;

#define CHANGELOG_SIZ 1024
#define CL_DEFAULT "/DataStore/Videos-HQ/.well-known/version.txt"
#define DIR_DEFAULT "/DataStore/Videos-HQ/"

char Dir[1024] = {0};
char changeLog[1024] = {0};
char source[1024] = {0};
char dest[1024] = {0};
bool foreground = false;

extern int optind;

void usage(char **argv) {
	cerr << "Usage: " << argv[0] << " [ -f ] [ -c changelogpath ] [ -d dest_dir ] source dest" << endl;
	cerr << "Program defaults are replaced by environment CHANGELOG and DIR, which are replaced by -c and -d" << endl;
	cerr << "Program defaults (changelog, dir): (" << CL_DEFAULT << ", " << DIR_DEFAULT << ")\n";
	cerr << "-f: run in foreground. By default, drops to background before file move begins\n";
	exit(1);
}

void process_args(int argc, char **argv) {
	int c;
	extern char *optarg;
	
	strcpy(Dir, DIR_DEFAULT);
	strcpy(changeLog, CL_DEFAULT);
	
	if ((optarg = getenv("DIR"))) strcpy(Dir, optarg);
	if ((optarg = getenv("CHANGELOG"))) strcpy(changeLog, optarg);
	
	while ((c = getopt(argc, argv, "c:d:f")) != -1) {
		switch (c) {
			case 'c':
				strcpy(changeLog, optarg);
				break;
			case 'd':
				strcpy(Dir, optarg);
				break;
			case 'f':
				foreground = true;
				break;
			default:
				usage(argv);
				break;
		}
	}
	
	if (optind < argc) {
		strcpy(source, argv[optind++]);
	} else {
		cerr << "Missing source, dest\n" << endl;
		usage(argv);
	}
	
	if (optind < argc) {
		strcat(dest, argv[optind++]);
		if (dest[strlen(dest) - 1] == '/') {
			// we want to get the actual full filename.
			strcat(dest, basename(source));
			cerr << "NOTICE: Appended basename to dest path.  Best to include filename in dest" << endl;
		}
	} else {
		cerr << "Missing dest\n" << endl;
		usage(argv);
	}
}

void update_version() {
	int fd = open(changeLog, O_RDWR | O_CREAT,
		S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	
	if (fd == -1) {
		perror("open(changeLog)");
		return;
	}
	
	// 1: for my setup, fallocate() won't actually work, as ZFS doesn't support
	// it apparently.
	// 2: consideration: as I dropped the mmap() from heartbeat.cpp, there's
	// not really a need to keep this a specific size anymore; we could keep
	// history indefinitely.
	#ifdef __linux__
	if ((errno = fallocate(fd, 0, 0, CHANGELOG_SIZ)) != 0) {
		perror("fallocate()");
	}
	#else
	// on mac, we need to write data into the space, if the file isn't large enough.
	{
		off_t t = lseek(fd, 0, SEEK_END);
		if (t < CHANGELOG_SIZ) {
			char buf[CHANGELOG_SIZ] = {0};
			write(fd, buf, CHANGELOG_SIZ - t);
		}
		lseek(fd, 0, SEEK_SET);
	}
	#endif
	
	// first, read in the current version number.
	uint16_t version, nversion;
	if (read(fd, (void *)&nversion, 2) != 2) {
		cerr << "version read failed" << endl;
		return;
	}
	version = ntohs(nversion);
	
	version++;
	// skip 0, if we overflowed.
	if (!version) version++;
	
	nversion = htons(version);
	
	char buf[CHANGELOG_SIZ];
	read(fd, buf, 1); // throw away a delimiter.
	
	if (read(fd, buf, CHANGELOG_SIZ - 3) != CHANGELOG_SIZ - 3) {
		cerr << "Didn't read full buffer from file" << endl;
		return;
	}
	
	pwrite(fd, (void *)&nversion, 2, 0);
	pwrite(fd, (void *)"\n", 1, 2);
	lseek(fd, 3, SEEK_SET);
	
	char *f, *b = buf;
	int written = 3;
	
	// write in the updated file.
	written += dprintf(fd, "%d\t%s\n", version, dest);
	
	while ((f = strsep(&b, "\n"))) {
		if (written + strlen(f) + 1 > CHANGELOG_SIZ) break;
		
		written += dprintf(fd, "%s\n", f);
	}
	
	cout << "Version updated to " << (int)version << endl;
}

static void write_hash() {
	SHA1 sha(string(Dir) + string(dest));
	char *p = strrchr(dest, '/');
	string hashFile = Dir;
	if (p) {
		p++;
		char c = *p;
		*p = 0;
		hashFile += dest;
		hashFile += '.';
		*p = c;
		hashFile += p;
		hashFile += ".sha1";
	} else {
		hashFile += '.';
		hashFile += dest;
		hashFile += ".sha1";
	}
	
	ofstream ofile(hashFile, ios::out|ios::trunc);
	if (!ofile) {
		cerr << "Failed to open hash file [" << hashFile << "]\n";
		return;
	}
	ofile << sha.hex() << endl;
	ofile.close();
}

int main(int argc, char **argv) {
	process_args(argc, argv);
	
	setuid(geteuid());
	
	if (getuid() != 0) {
		cerr << "I need to be setuid 0" << endl;
		return 1;
	}
	
	// first, we're going to take ownership of the file.
	if (chown(source, 0, 0) == -1) {
		perror("chown");
		return 1;
	}
	
	if (chmod(source, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) == -1) {
		perror("chmod");
		return 1;
	}
	
	if (!foreground) {
		// we now daemonize, so the user doesn't have to remember nohup.
		// this is always done unless explicitly requested in the foreground,
		// as the general implication of this program is that it's going to
		// run for awhile.
		
		// close stdin, stdout, stderr.
		close(0);
		close(1);
		close(2);
		pid_t p;
		if ((p = fork()) > 0) {
			// parent process. leave.
			exit(0);
		} else if (p < 0) {
			exit(1);
		}
		
		// child process. get a new session so we disconnect from terminal.
		setsid();
		
		// now fork again to prevent tty reopening for good measure.
		if ((p = fork()) > 0) {
			// parent. leave.
			exit(0);
		} else if (p < 0) {
			exit(1);
		}
	}
	
	pid_t pid;
	if ((pid = fork()) == 0) {
		char fullDest[2048] = {0};
		strcpy(fullDest, Dir);
		strcat(fullDest, dest);
		execl("/bin/mv", "/bin/mv", source, fullDest, (char *)0);
		perror("exec failed");
		return 1;
	} else if (pid == -1) {
		perror("fork failed");
		return 1;
	}
	
	while (true) {
		int stat;
		pid = wait(&stat);
		if (pid == -1) {
			perror("wait()");
			return 1;
		}
		
		stat = WEXITSTATUS(stat);
		
		if (stat == 0) {
			update_version();
			// it's pretty inefficient that I'm waiting until now to do this
			// todo: probably replace /bin/mv with my own read + write + remove,
			// such that I can calculate the hash in transit.
			write_hash();
		}
		
		return stat;
	}
}
