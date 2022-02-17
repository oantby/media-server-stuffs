/*

This program serves as a method for secondary nodes to verify the content
of their files.

Background: heartbeat.cpp takes care of the process of bringing files over.
While I have no reason to believe it'd ever mess up in that process, I'd sleep
better at night if I knew the content of the files was verified. This process
runs regularly on all secondary nodes, finds all files in the tree that are
newer than agefile, and verifies their content matches the checksum in hashfile.
On primary nodes, it finds files newer than agefile, hashes their content,
and puts it in hashfile.

Further background: there is currently an rsync process in place that updates
the content of all files over night. Because I don't update the timestamps on
files synced by heartbeat, this includes re-syncing any such files. This is a
wasteful process. By having this script, I can update heartbeat to set
timestamps, allowing rsync to skip over said files, without any risk of loss.
Yes, I know rsync can checksum files, but it's an all-or-nothing, and I'd only
like to checksum those that are newish; checksumming all would be EXTREMELY
resource-intensive. It verifies by size and time right now. heartbeat guarantees
a size match by virtue of allocating the exact size before any data transfer,
so setting the stamp as well would guarantee that rsync never hits files updated
by heartbeat.

Expectation: rsync will still run to keep things looking good and to bring
in the checksum file. This script will check files that are updated and,
if they don't match the expected checksum, delete them from a secondary node,
forcing a re-sync.

*/

#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <utime.h>
#include <queue>
#include <fstream>
#include "log.hpp"
#include "sha1.hpp"

#ifdef __linux__
#define MTIME_FIELD st_mtim
#else
#define MTIME_FIELD st_mtimespec
#endif

#define DEFAULT_DIR "/DataStore/Videos-HQ"

using namespace std;

struct APPL_CONTEXT {
	string ageFile; // a file whose timestamp we compare to files in the tree
	string baseDir;
	time_t maxAge = numeric_limits<time_t>::min();;
	bool update = false; // whether or not to update the ageFile stamp when done.
} Ap;

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [ -npvq ] [ -d basedir ] [ -f agefile | -D timestamp ]\n\n", argv[0]);
	fprintf(stderr, "-d basedir\n\tSet the base dir for files to compare. Default: %s\n", DEFAULT_DIR);
	fprintf(stderr, "-D timestamp\n\tSet the unix timestamp for modification time comparison. "
		"Overridden by -f agefile\n");
	fprintf(stderr, "-f agefile\n\tSet the agefile, the modification time of which "
		"is used for comparison to find newly updated files\n");
	fprintf(stderr, "-n\n\tDo not update the timestamp on agefile after processing\n");
	fprintf(stderr, "-q\n\tBe less verbose. Can be specified multiple times\n");
	fprintf(stderr, "-u\n\tUpdate the modification time on agefile after running\n");
	fprintf(stderr, "-v\n\tBe more verbose. Can be specified multiple times\n");
	fprintf(stderr, "\n");
	exit(1);
}

void parse_args(int argc, char **argv) {
	int c;
	
	Ap.baseDir = DEFAULT_DIR;
	
	while ((c = getopt(argc, argv, "d:D:f:npquvh")) != -1) {
		logger::log(LVL4, "getopt returned [%c]", (char)c);
		switch (c) {
			case 'd':
				Ap.baseDir = optarg;
				break;
			case 'f':
				Ap.ageFile = optarg;
				break;
			case 'D':
				Ap.maxAge = stol(optarg);
				logger::log(LVL2, "Comparing files to mtime [%ld]", Ap.maxAge);
			case 'q':
				logger::log(LVL2, "Decreased log level to %d", logger::lvlDec());
				break;
			case 'u':
				Ap.update = true;
				break;
			case 'v':
				logger::log(LVL2, "Increased log level to %d", logger::lvlInc());
				break;
			case 'h':
			default:
				usage(argv);
				break;
		}
	}
	
}

int main(int argc, char **argv) {
	parse_args(argc, argv);
	
	DIR *dir;
	struct dirent *ent;
	
	queue<string> dirStack;
	
	dirStack.push(Ap.baseDir);
	string curDir;
	
	struct stat st;
	if (Ap.ageFile.size()) {
		if (stat(Ap.ageFile.c_str(), &st) == -1) {
			logger::log(LVL1, "ageFile didn't exist. holding default.");
		} else {
			Ap.maxAge = st.MTIME_FIELD.tv_sec;
			logger::log(LVL2, "Comparing files to ageFile mtime [%ld]", Ap.maxAge);
		}
	}
	
	while (dirStack.size()) {
		dir = opendir(dirStack.front().c_str());
		if (!dir) {
			logger::log(LVL1, "Failed to open [%s] - %s", dirStack.front().c_str(), strerror(errno));
			dirStack.pop();
			continue;
		}
		string curDir = dirStack.front();
		logger::log(LVL2, "Processing directory %s", curDir.c_str());
		while ((ent = readdir(dir))) {
			
			// skip current dir, skip hidden, etc.
			if (*ent->d_name == '.') continue;
			
			if (ent->d_type == DT_DIR) {
				// add to the stack to read.
				logger::log(LVL2, "Adding directory [%s] to stack", (curDir + "/" + ent->d_name).c_str());
				dirStack.push(curDir + "/" + ent->d_name);
				continue;
			} else if (ent->d_type != DT_REG) {
				continue; // the rest don't matter.
			}
			
			if (stat((curDir + "/" + string(".") + string(ent->d_name) + string(".sha1")).c_str(),
				&st) == -1) {
				// this file has no hash. no worries.
				logger::log(LVL4, "File [%s] had no .sha1 file", ent->d_name);
				continue;
			}
			logger::log(LVL2, "File [%s] has .sha1 file", ent->d_name);
			
			stat(ent->d_name, &st);
			if (st.MTIME_FIELD.tv_sec < Ap.maxAge) {
				logger::log(LVL2, "File [%s] older than ageFile; skipping", ent->d_name);
				continue;
			}
			
			logger::log(LVL3, "Processing file [%s]", ent->d_name);
			
			ifstream ifile(curDir + "/" + "." + ent->d_name + ".sha1");
			string hash;
			getline(ifile, hash);
			ifile.close();
			
			SHA1 sha(curDir + "/" + ent->d_name);
			
			if (hash != sha.hex()) {
				logger::log(LVL1, "Checksum mismatch: File [%s] expected [%s] calculated [%s]",
					(curDir + "/" + ent->d_name).c_str(), hash.c_str(), sha.hex().c_str());
			} else {
				logger::log(LVL3, "File [%s] matched expected [%s]",
					(curDir + "/" + ent->d_name).c_str(), hash.c_str());
			}
		}
		
		closedir(dir);
		dirStack.pop();
	}
	
	if (Ap.update && Ap.ageFile.size()) {
		utime(Ap.ageFile.c_str(), NULL);
	}
	
	return 0;
}