#include <iostream>
#include <iomanip>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

using namespace std;

int main() {
	chdir("/var/log/node_updates");
	DIR *dir = opendir(".");
	if (!dir) {
		cerr << "Failed to open dir: " << strerror(errno) << endl;
		return 1;
	}
	struct dirent *ent;
	struct stat finfo;
	time_t now = time(NULL);
	char dbuf[100];
	bool first = true;
	while ((ent = readdir(dir))) {
		if (ent->d_type != DT_REG) continue;

		if (stat(ent->d_name, &finfo) == -1) {
			cerr << "Failed to stat " << ent->d_name << ": " << strerror(errno) << endl;
			continue;
		}
		if (now > (2 * 86400) + finfo.st_mtimespec.tv_sec) {
			strftime(dbuf, sizeof(dbuf), "%Y-%m-%d %H:%M:%S", localtime(&finfo.st_mtimespec.tv_sec));
			if (first) {
				first = false;
				cout << "The following nodes haven't updated in >48 hours" << endl;
			}
			cout << left << setw(30) << ent->d_name << dbuf << endl;
		}
	}
	return 0;
}
