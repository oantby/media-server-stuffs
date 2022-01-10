#include <iostream>
#include <libgen.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

using namespace std;

#define CHANGELOG_SIZ 1024
#define CL_DEFAULT "/DataStore/Videos-HQ/.well-known/version.txt"
#define DIR_DEFAULT "/DataStore/Videos-HQ/"

// this is a setuid binary to copy the file as root and update the changelog
char Dir[1024] = {0};
char changeLog[1024] = {0};
char source[1024] = {0};
char dest[1024] = {0};

extern int optind;

void usage(char **argv) {
	cerr << "Usage: " << argv[0] << " [ -c changelogpath ] [ -d dest_dir ] source dest" << endl;
	cerr << "Program defaults are replaced by environment CHANGELOG and DIR, which are replaced by -c and -d" << endl;
	cerr << "Program defaults (changelog, dir): (" << CL_DEFAULT << ", " << DIR_DEFAULT << ")\n";
	exit(1);
}

void process_args(int argc, char **argv) {
	int c;
	extern char *optarg;
	
	strcpy(Dir, DIR_DEFAULT);
	strcpy(changeLog, CL_DEFAULT);
	
	if ((optarg = getenv("DIR"))) strcpy(Dir, optarg);
	if ((optarg = getenv("CHANGELOG"))) strcpy(changeLog, optarg);
	
	while ((c = getopt(argc, argv, "c:d:")) != -1) {
		switch (c) {
			case 'c':
				strcpy(changeLog, optarg);
				break;
			case 'd':
				strcpy(Dir, optarg);
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
	uint16_t version;
	if (read(fd, (void *)&version, 2) != 2) {
		cerr << "version read failed" << endl;
		return;
	}
	
	version++;
	// skip 0, if we overflowed.
	if (!version) version++;
	
	char buf[CHANGELOG_SIZ];
	read(fd, buf, 1); // throw away a delimiter.
	
	if (read(fd, buf, CHANGELOG_SIZ - 3) != CHANGELOG_SIZ - 3) {
		cerr << "Didn't read full buffer from file" << endl;
		return;
	}
	
	pwrite(fd, (void *)&version, 2, 0);
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
		}
		
		return stat;
	}
}