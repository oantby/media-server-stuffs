#include <iostream>
#include <iomanip>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>
#include <signal.h>
#include <sys/signal.h>
#include <sys/time.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <setjmp.h>
#include <fcntl.h>
#include <math.h>
#include <aio.h>
#include <sys/mman.h>
#ifdef __linux__
	#include <sys/sysinfo.h>
#else
	#include <sys/sysctl.h>
#endif
#include <net/if.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <vector>
#include <unordered_set>
#include <set>

using namespace std;

#define DEFAULT_PORT 8001
#define BUF_SIZ 64100
#define CHANGELOG_SIZ 1024
#define CURRENT_VERSION 1

#define OP_HB 0 /* heartbeat */
#define OP_HBREQ 1 /* heartbeat request */
#define OP_DREQ 2 /* request data fragment */
#define OP_DSEND 3 /* send data fragment */
#define OP_OFFLINE 4 /* going offline notification */
#define OP_ONLINE 5 /* back online */
#define OP_UNSUPVERS 6 /* requested version unsupported */
#define OP_ACK 7 /* acknowledge heartbeat */
#define OP_XFER_INIT 8 /* initialize data transfer (and encryption for it) */
#define OP_ACK_XFER 9 /* acknowledge transfer request, and complete encryption for it */
#define OP_DENY_XFER 10 /* transfer request denied */
#define INVALID_MSG 11
#define OP_XFER_INFO 12 /* information on file to be transferred */
#define OP_XFER_DATA 13 /* data chunk from file */
#define OP_XFER_META 14 /* random metadata regarding transfer. */
#define OP_XFER_DREQ 15 /* data chunk request */
#define OP_XFER_INVALID 16 /* last xfer message invalid */

// error messages to go with OP_XFER_INVALID
#define XFER_VERSION_INVALID 0
#define XFER_SLICE_INVALID 1

#define FORCE_DOWN_BIT   0x80
#define FORCE_BACKUP_BIT 0x40
#define PRIMARY_BIT      0x20

#define STAT_NONE 0
#define STAT_IP   1
#define STAT_DONE 2

struct Client {
	bool backup;
	bool down;
	bool weightSet;
	float load;
	uint16_t weight;
	uint16_t version;
	time_t lastHB;
	char addr_string[100];
	in_addr_t addr; /* in network byte order */
	short port; /* in network byte order */
	
	Client() {}
	Client(in_addr_t a, short p) : addr(a), port(p) {}
	void clear() {
		backup = 0;
		down = 0;
		weightSet = 0;
		load = 0;
		weight = 0;
		version = 0;
		lastHB = 0;
		memset(addr_string, 0, sizeof(addr_string));
		addr = 0;
		port = 0;
	}
};

struct {
	short cliport;
	short myport;
	in_addr_t myAddr;
	unsigned logLvl;
	
	// in client checks, if a client has never sent a heartbeat,
	// should we assume it's fine? Will likely be true early, and change to false
	// once servers are consistently running hb.
	bool ignore_absent;
	bool primary;
	
	long ncores;
	
	char configPath[100];
	char pidPath[100];
	char statusDownPath[100];
	char statusBackupPath[100];
	char changelogPath[100];
	char tmpFilePath[100];
	char filesRoot[100];
	char retrievePath[100];
	char retrieveTmp[120];
	// current DATA version - not CURRENT_VERSION.
	// stored in network byte order on all hosts to ensure identical
	// on all hosts.
	uint16_t currentVersion;
	uint16_t retrieveVersion;
	struct sockaddr_in primaryAddr;
	
	// current status of file transfer
	uint_fast8_t retrieveStatus;
	
	uint8_t *retrievedBlockStatus;
	
	uint64_t retrieveSize;
	
	bool (*notifyFunc)(const char *, const char *);
	
	jmp_buf jbuf;
	
	time_t next_req_try;
	
	in_addr_t hbclient;
	
	uint_fast16_t cache_exp;
	
	bool shuttingDown;
	
	int sockfd;
	int xferFD;
	
	struct timeval xferTS;
	
	Client *known_clients;
	int kc_count;
} Ap;

#define LVL1 1
#define LVL2 2
#define LVL3 4
#define LVL4 8
#define log(...) _log(__FILE__, __LINE__, __VA_ARGS__)

#ifndef MSG_CONFIRM
#define MSG_CONFIRM 0
#endif

static void interrupt(int signal);

namespace pushover {bool notify(const char *m, const char *s = NULL);}

void _log(const char *file, int line, unsigned level, const char *msg, ...) {
	if (!(Ap.logLvl & level)) return;
	
	static int_fast8_t logTime = -1;
	if (logTime == -1) {
		// check if we're outputting to journalctl, which timestamps for us.
		logTime = getenv("JOURNAL_STREAM") == NULL;
	}
	if (logTime) {
		struct timeval tv;
		memset(&tv, 0, sizeof(tv));
		gettimeofday(&tv, NULL);
		
		char ds[100];
		strftime(ds, sizeof(ds), "%Y-%m-%dT%H:%M:%S", localtime(&tv.tv_sec));
		fprintf(stderr, "[%s.%d] ", ds, (int)(tv.tv_usec / 10000));
	}
	fprintf(stderr, "[%s:%d] ", file, line);
	
	va_list arglist;
	va_start(arglist, msg);
	vfprintf(stderr, msg, arglist);
	va_end(arglist);
	fputc('\n', stderr);
}

static void ack(in_addr_t addr, short port, bool convert = false) {
	struct sockaddr_in client;
	memset(&client, 0, sizeof(client));
	
	client.sin_family = AF_INET;
	client.sin_port = convert ? htons(port) : port;
	client.sin_addr.s_addr = addr;
	
	char buf[4 + sizeof(Ap.primaryAddr)];
	buf[0] = CURRENT_VERSION;
	buf[1] = OP_ACK;
	
	*(uint16_t *)&buf[2] = htons(Ap.currentVersion);
	memcpy(buf + 4, &Ap.primaryAddr, sizeof(Ap.primaryAddr));
	
	if (sendto(Ap.sockfd, buf, sizeof(buf), MSG_CONFIRM,
		(struct sockaddr *)&client, sizeof(client)) > 0) {
		
		log(LVL2, "Sent ack to %s port %d", inet_ntoa(client.sin_addr),
			ntohs(client.sin_port));
	} else {
		log(LVL1, "ACK send failed: %s", strerror(errno));
	}
}

static void process_ack_v1(struct sockaddr_in client, char *buf, size_t n) {
	if (n < 8) {
		log(LVL1, "ACK had insufficient data [%d bytes]", n);
		return;
	}
	
	if (!Ap.currentVersion) {
		// I haven't read the current version yet or don't even have
		// one.  the latter is a problem that will always result in me
		// being marked down.  the former is weird (i.e. only possible in 1970),
		// but skippable.
		log(LVL1, "I didn't have a current version yet.  Can't compare to ACK");
		return;
	}
	
	uint16_t ackVersion = ntohs(*(uint16_t *)&buf[2]);
	uint16_t myVersion = ntohs(Ap.currentVersion);
	
	if (myVersion == ackVersion || ackVersion == 0 || myVersion == 0) {
		// bueno.
		log(LVL3, "My version [%d] matches ack version [%d]", myVersion, ackVersion);
		return;
	}
	
	Ap.retrieveVersion = myVersion + 1;
	memcpy(&Ap.primaryAddr, buf + 4, sizeof(Ap.primaryAddr));
}

// quick macros for the block status bitfield.
#define RETRIEVEDBIT(a) (Ap.retrievedBlockStatus[a / 8] & (1 << (7 - (a % 8))))
#define SETRETRBIT(a) Ap.retrievedBlockStatus[a / 8] |= (1 << (7 - (a % 8)))

static void file_req() {
	if (!Ap.retrieveVersion || Ap.retrieveVersion == ntohs(Ap.currentVersion)) {
		// already up-to-date; just a routine call. return.
		// do cleanup in the case a previous transfer got interrupted.
		if (Ap.retrievedBlockStatus || Ap.xferFD > 0) {
			log(LVL1, "Mixed transfer statuses (rsync overlap likely). Exiting");
			exit(1);
		}
		return;
	}
	
	static time_t last_info_req = 0;
	static uint_fast8_t reqCount = 0;
	
	time_t now = time(NULL);
	
	
	switch (Ap.retrieveStatus) {
		case STAT_NONE:
			// we know we want to retrieve, but haven't started yet.
			// get the initial version data from the primary.
			if (now < last_info_req + 2) return; // give the primary at least a second to answer.
			// haven't had much luck, and don't want to badger.
			if (Ap.next_req_try && Ap.next_req_try > now) return;
			
			if (now < last_info_req + 10) {
				// if we're sending off requests every couple seconds and not
				// getting responses, keep count so we can give up before too long.
				reqCount++;
			} else {
				reqCount = 1;
			}
			if (reqCount > 5) {
				// time to give up for awhile.
				Ap.next_req_try = now + 60;
				return;
			}
			
			{
				// tell the primary we want the info for this version.
				char buf[4];
				buf[0] = CURRENT_VERSION;
				buf[1] = OP_XFER_INIT;
				
				*(uint16_t *)&buf[2] = htons(Ap.retrieveVersion);
				if (sendto(Ap.sockfd, buf, sizeof(buf), MSG_CONFIRM,
					(struct sockaddr *)&Ap.primaryAddr, sizeof(Ap.primaryAddr)) > 0) {
					
					log(LVL2, "Requested info on version %d from primary [%s]",
						Ap.retrieveVersion, inet_ntoa(Ap.primaryAddr.sin_addr));
				} else {
					log(LVL1, "Failed to request version info: %s", strerror(errno));
				}
			}
			break;
		case STAT_IP:
			// we have received the newest information for the file updated
			// in retrieveVersion, and verified we really need it. choose
			// a missing slice, and request it.
			reqCount = 0; // no longer worried about primary not responding to us.
			
			// we have the file size, and we break the file into 64000-byte blocks
			// (allows extra meta info to still fit in 64KiB block)
			// based on the bitfield Ap.retrievedBlockStatus, find a chunk
			// we still need to get. choose one at random to lower the odds
			// of us accidentally asking for the same block repeatedly due to
			// race condition.
			{
				static size_t request_idx = -1;
				request_idx = (request_idx + 1) % (size_t)ceil((double)Ap.retrieveSize / 64000.0);
				while (RETRIEVEDBIT(request_idx)) {
					// since we're always stepping sequentially, the expectation
					// is that this while is almost never true, so if it is, we
					// check and make sure we're not done with the file.
					bool done = true;
					for (size_t i = 0; i < ceil((double)Ap.retrieveSize / 64000.0); i++) {
						if (!RETRIEVEDBIT(i)) {
							request_idx = i;
							done = false;
							break;
						}
					}
					if (done) {
						Ap.retrieveStatus = STAT_DONE;
						if (Ap.xferTS.tv_sec == 0) {
							futimes(Ap.xferFD, NULL);
						} else {
							struct timeval times[2];
							times[0] = Ap.xferTS;
							times[1] = Ap.xferTS;
							
							futimes(Ap.xferFD, times);
						}
						close(Ap.xferFD);
						Ap.xferFD = -1;
						// nothing left to do on this round.
						return;
					}
				}
				
				// now we request slice request_idx.
				char buf[8];
				buf[0] = CURRENT_VERSION;
				buf[1] = OP_XFER_DREQ;
				*(uint16_t *)&buf[2] = htons(Ap.retrieveVersion); // identifies the file we want.
				*(uint32_t *)&buf[4] = htonl((uint32_t)request_idx);
				
				if (sendto(Ap.sockfd, buf, sizeof(buf), MSG_CONFIRM,
					(struct sockaddr *)&Ap.primaryAddr, sizeof(Ap.primaryAddr)) > 0) {
					
					log(LVL2, "Requested chunk %d from primary [%s]",
						request_idx, inet_ntoa(Ap.primaryAddr.sin_addr));
				} else {
					log(LVL1, "Failed to request chunk from primary: %s", strerror(errno));
				}
			}
			
			break;
		case STAT_DONE:
			// verify the file, move it into place, update our current version,
			// and discard any buffers we made.
			{
				
				struct stat s1, s2;
				memset(&s1, 0, sizeof(s1));
				memset(&s2, 0, sizeof(s2));
				
				if (stat(Ap.retrieveTmp, &s1) == -1) {
					log(LVL1, "Failed to stat temp file: %s", strerror(errno));
					Ap.retrieveStatus = STAT_NONE;
					free(Ap.retrievedBlockStatus);
					Ap.retrievedBlockStatus = NULL;
					return;
				} else if (stat(".", &s2) == -1) {
					log(LVL1, "Failed to stat destination root: %s", strerror(errno));
					Ap.retrieveStatus = STAT_NONE;
					free(Ap.retrievedBlockStatus);
					Ap.retrievedBlockStatus = NULL;
					return;
				}
				
				if (s1.st_dev == s2.st_dev) {
					// same device. we can rename.
					if (rename(Ap.retrieveTmp, Ap.retrievePath) == -1) {
						log(LVL1, "Failed to rename tempfile to final path: %s", strerror(errno));
						Ap.retrieveStatus = STAT_NONE; // force full retry, I guess.
						free(Ap.retrievedBlockStatus);
						Ap.retrievedBlockStatus = NULL;
						return;
					} else {
						log(LVL2, "Rename file [%s] -> [%s]", Ap.retrieveTmp, Ap.retrievePath);
					}
				} else {
					// different device. have to copy & remove.
					// damn.
					log(LVL1, "temp file is on a different device than final path and I'm too lazy to take care of it");
				}
				
				memset(Ap.retrieveTmp, 0, sizeof(Ap.retrieveTmp));
				memset(Ap.retrievePath, 0, sizeof(Ap.retrievePath));
				
				// we're now at retrieveVersion.
				Ap.currentVersion = htons(Ap.retrieveVersion);
				Ap.retrieveVersion = 0;
				Ap.retrieveStatus = STAT_NONE;
				
				free(Ap.retrievedBlockStatus);
				Ap.retrievedBlockStatus = NULL;
				
				// set our timers back to their normal.
				// if we're still not all the way up-to-date, the next heartbeat
				// will tell us so.
				struct itimerval alarmTime;
				memset(&alarmTime, 0, sizeof(alarmTime));
				
				alarmTime.it_interval.tv_sec = 10;
				alarmTime.it_value.tv_sec = 10;
				
				if (setitimer(ITIMER_REAL, &alarmTime, NULL) == -1) {
					log(LVL1, "setitimer failed: %s", strerror(errno));
				}
				
				int fd = open(Ap.changelogPath, O_WRONLY);
				
				if (fd == -1) {
					log(LVL1, "Failed to open changelog file for writing: %s",
						strerror(errno));
					return;
				}
				
				if (pwrite(fd, &Ap.currentVersion, 2, 0) != 2) {
					log(LVL1, "Failed to update version in changelog: %s",
						strerror(errno));
				}
				
				close(fd);
				
			}
			break;
	}
}


#define min(a, b) a < b ? a : b
static void file_process_v1(struct sockaddr_in client, char *buf, size_t n) {
	switch (buf[1]) {
		case OP_XFER_INIT:
			// request for information on a given data version.
			// response to OP_XFER_INIT
			// OP_XFER_INFO structure:
			// 0: heartbeat version
			// 1: OP_XFER_INFO (uint8_t)
			// 2-3: data version
			{
				uint16_t reqVer = ntohs(*(uint16_t *)(buf + 2));
				// find the change associated with this version.
				ifstream cl(Ap.changelogPath);
				string clLine;
				bool found = false;
				getline(cl, clLine); // skip the current version line.
				// read up to 5 version lines from the changelog.
				for (int i = 0; i < 5 && getline(cl, clLine); i++) {
					log(LVL2, "Changelog line version %d", stoi(clLine));
					if (stoi(clLine) == reqVer) {
						found = true;
						break;
					}
				}
				
				cl.close();
				
				if (!found) {
					log(LVL1, "Couldn't find information on version %d", reqVer);
					return;
				}
				size_t pos;
				if ((pos = clLine.find('\t')) == string::npos) {
					return;
				}
				
				string path = clLine.substr(pos + 1);
				
				log(LVL3, "Version [%d] Path [%s]", reqVer, path.c_str());
				
				size_t respLen = path.size() + 26;
				char *resp = (char *)calloc(1, respLen);
				resp[0] = CURRENT_VERSION;
				resp[1] = OP_XFER_INFO;
				// copy requested data version in
				resp[2] = buf[2];
				resp[3] = buf[3];
				// write length of path name.
				*(uint16_t *)&resp[4] = htons((uint16_t)path.size());
				// write path in.
				memcpy(resp + 14, path.c_str(), path.size());
				
				// get stats on the file.
				struct stat info;
				memset(&info, 0, sizeof(info));
				if (stat(path.c_str(), &info) != -1) {
					// write in the file size.
					// note: htonll defined on some but not all of my compilers,
					// so we do it by hand here.
					*(uint32_t *)&resp[6] = htonl((uint32_t)(info.st_size >> 32));
					*(uint32_t *)&resp[10] = htonl((uint32_t)(info.st_size & 0xffffffff));
					
					// write in the modification time. 64-bit seconds, 32-bit microseconds.
					#ifdef __linux__
					*(uint32_t *)&resp[14 + path.size()] = htonl((uint32_t)(info.st_mtim.tv_sec >> 32));
					*(uint32_t *)&resp[18 + path.size()] = htonl((uint32_t)(info.st_mtim.tv_sec & 0xffffffff));
					
					*(uint32_t *)&resp[22 + path.size()] = htonl((uint32_t)(info.st_mtim.tv_nsec / 1000));
					#else
					*(uint32_t *)&resp[14 + path.size()] = htonl((uint32_t)(info.st_mtimespec.tv_sec >> 32));
					*(uint32_t *)&resp[18 + path.size()] = htonl((uint32_t)(info.st_mtimespec.tv_sec & 0xffffffff));
					
					*(uint32_t *)&resp[22 + path.size()] = htonl((uint32_t)(info.st_mtimespec.tv_nsec / 1000));
					#endif
					
					if (sendto(Ap.sockfd, resp, respLen, MSG_CONFIRM,
						(struct sockaddr *)&client, sizeof(client)) > 0) {
						log(LVL2, "Sent file info");
					} else {
						log(LVL1, "Failed to send file info: %s", strerror(errno));
					}
				} else {
					log(LVL1, "Failed to stat file: %s", path.c_str());
				}
				
				free(resp);
				
				log(LVL3, "Finished processing init");
			}
			break;
		case OP_XFER_INFO:
			// response to OP_XFER_INIT
			// OP_XFER_INFO structure:
			// 0: heartbeat version
			// 1: OP_XFER_INFO (uint8_t)
			// 2-3: data version
			// 4-5: filename length (uint16_t)
			// 6-13: file length (uint64_t)
			// 14-(14+fnlength-1): filename (char *)
			{
				// if we've already got a transfer in process, we need to drop
				// this request so we can keep working on the current one.
				log(LVL3, "(bool)retrievedBlockStatus=%d, retrieveStatus=%d",
					(bool)Ap.retrievedBlockStatus, Ap.retrieveStatus);
				if (Ap.retrievedBlockStatus || Ap.retrieveStatus != STAT_NONE) return;
				
				uint16_t infoVersion = ntohs(*(uint16_t *)(buf + 2));
				
				if (infoVersion != Ap.retrieveVersion) {
					// we're going to ignore this one.
					log(LVL3, "");
					return;
				}
				
				uint16_t fnlen = ntohs(*(uint16_t *)(buf + 4));
				// no ntohll or similar for 64 bits.  nonstandard betoh64 unavailable.
				uint64_t flen = ((uint64_t)ntohl(*(uint32_t *)(buf + 6)) << 32)
					+ ntohl(*(uint32_t *)(buf + 10));
				
				log(LVL3, "Copying %d bytes into retrievePath", fnlen);
				log(LVL3, "File length: %ld", flen);
				
				memset(Ap.retrievePath, 0, sizeof(Ap.retrievePath));
				memcpy(Ap.retrievePath, (buf + 14), fnlen);
				
				snprintf(Ap.retrieveTmp, sizeof(Ap.retrieveTmp), "%s/.xfer_%d_XXXXXXX",
					Ap.tmpFilePath, Ap.retrieveVersion);
				Ap.xferFD = mkstemp(Ap.retrieveTmp);
				if (Ap.xferFD == -1) {
					log(LVL1, "Failed to open test file [%s]: %s", Ap.retrieveTmp, strerror(errno));
					return;
				}
				
				// make the file mode 644
				fchmod(Ap.xferFD, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
				
				// save off the timestamp to update when we're done writing.
				memset(&Ap.xferTS, 0, sizeof(Ap.xferTS));
				if (n >= (uint32_t)(22 + fnlen)) {
					// sender is updated enough to send a timestamp.
					Ap.xferTS.tv_sec = ((uint64_t)ntohl(*(uint32_t *)(buf + 14 + fnlen)) << 32)
						+ ntohl(*(uint32_t *)(buf + 18 + fnlen));
					Ap.xferTS.tv_usec = ntohl(*(uint32_t *)(buf + 22 + fnlen));
				}
				
				// we're going to allocate the whole size of the file off the
				// bat so we can fill it in as we go.
				uint64_t written = 0;
				ssize_t writeN;
				char emptyBuf[1 << 15] = {0};
				while (written < flen) {
					writeN = write(Ap.xferFD, emptyBuf,
						min(sizeof(emptyBuf), flen - written));
					if (writeN == -1) {
						log(LVL1, "Failed to write to tempfile: %s", strerror(errno));
						close(Ap.xferFD);
						Ap.xferFD = -1;
						return;
					}
					written += writeN;
				}
				
				log(LVL2, "Wrote %ld 0 bytes into tempfile", written);
				
				Ap.retrieveStatus = STAT_IP;
				Ap.retrieveSize = flen;
				
				Ap.next_req_try = 0; // drop any extra delay that was set up.
				
				if (Ap.retrieveSize == 0) {
					Ap.retrieveStatus = STAT_DONE;
					if (Ap.xferTS.tv_sec == 0) {
						futimes(Ap.xferFD, NULL);
					} else {
						struct timeval times[2];
						times[0] = Ap.xferTS;
						times[1] = Ap.xferTS;
						
						futimes(Ap.xferFD, times);
					}
					close(Ap.xferFD);
					Ap.xferFD = -1;
					log(LVL1, "File size was 0; marking done");
					return;
				}
				
				// allocate a buffer for the yes/no on each 64000 block being retrieved.
				Ap.retrievedBlockStatus = (uint8_t *)calloc(1, ceil((double)flen / 64000.0 / 8.0));
				// since we now know we can get data, we set the alarms to
				// every .01 seconds.
				struct itimerval alarmTime;
				memset(&alarmTime, 0, sizeof(alarmTime));
				
				alarmTime.it_interval.tv_sec = 0;
				alarmTime.it_interval.tv_usec = 10000;
				alarmTime.it_value.tv_sec = 0;
				alarmTime.it_value.tv_usec = 10000;
				
				if (setitimer(ITIMER_REAL, &alarmTime, NULL) == -1) {
					log(LVL1, "setitimer failed: %s", strerror(errno));
				}
			}
			break;
		case OP_XFER_DREQ:
			// format:
			// 0: heartbeat version (1)
			// 1: OP_XFER_DREQ
			// 2-3: data version requested.
			// 4-7: slice index requested.
			{
				// world's smallest cache, so we don't look up the file repeatedly
				// if we keep getting requests for the same one.
				static char path[100] = {0};
				static uint16_t version = 0;
				static int fd = -1;
				
				uint16_t reqVer = ntohs(*(uint16_t *)(buf + 2));
				
				if (reqVer != version) {
					if (fd > 0) {
						log(LVL2, "Switching fd");
						close(fd);
						fd = -1;
					}
					
					// find the change associated with this version.
					ifstream cl(Ap.changelogPath);
					string clLine;
					bool found = false;
					getline(cl, clLine); // skip the current version line.
					// read up to 5 version lines from the changelog.
					for (int i = 0; i < 5 && getline(cl, clLine); i++) {
						if (stoi(clLine) == reqVer) {
							found = true;
							break;
						}
					}
					
					*path = 0;
					if (found) {
						size_t pos;
						if ((pos = clLine.find('\t')) != string::npos) {
							strcpy(path, clLine.substr(pos + 1).c_str());
						}
					}
					
					version = reqVer;
					if (*path) {
						errno = 0;
						fd = open(path, O_RDONLY);
						if (fd == -1) {
							int err = errno;
							log(LVL1, "Failed to open [%s]: [%s]", path, strerror(err));
							if (err == ENOENT || err == EACCES) {
								// no matter how long they wait, we're not going to
								// be able to get them this file without intervention.
								// send invalid version.
								char resp[3];
								resp[0] = CURRENT_VERSION;
								resp[1] = OP_XFER_INVALID;
								resp[2] = XFER_VERSION_INVALID;
								sendto(Ap.sockfd, resp, sizeof(resp), MSG_CONFIRM,
									(struct sockaddr *)&client, sizeof(client));
							}
							// drop this one. nothing more we can do.
							return;
						}
					}
				}
				
				if (!*path) {
					// couldn't find information associated with requested version.
					char resp[3];
					resp[0] = CURRENT_VERSION;
					resp[1] = OP_XFER_INVALID;
					resp[2] = XFER_VERSION_INVALID;
					sendto(Ap.sockfd, resp, sizeof(resp), MSG_CONFIRM,
						(struct sockaddr *)&client, sizeof(client));
					return;
				}
				
				if (fd == -1) {
					// we shouldn't have gotten here, tbh.
					version = 0;
					memset(path, 0, sizeof(path));
					char resp[3];
					resp[0] = CURRENT_VERSION;
					resp[1] = OP_XFER_INVALID;
					resp[2] = XFER_VERSION_INVALID;
					sendto(Ap.sockfd, resp, sizeof(resp), MSG_CONFIRM,
						(struct sockaddr *)&client, sizeof(client));
					return;
				}
				
				// we're going to reuse buf, since it has enough room
				// for a whole 64000-byte block.
				buf[1] = OP_XFER_DATA;
				int read;
				uint32_t slice = ntohl(*(uint32_t *)(buf + 4));
				if ((read = pread(fd, buf + 8, 64000, (off_t)64000 * (off_t)slice)) > 0) {
					if (sendto(Ap.sockfd, buf, read + 8, MSG_CONFIRM,
						(struct sockaddr *)&client, sizeof(client)) > 0) {
						
						log(LVL2, "Sent slice [%d] [%d bytes] from path [%s]", slice, read, path);
					} else {
						log(LVL1, "Failed to send slice from path [%s]: %s",
							path, strerror(errno));
					}
				} else {
					// that's an invalid slice.
					char resp[3];
					resp[0] = CURRENT_VERSION;
					resp[1] = OP_XFER_INVALID;
					resp[2] = XFER_SLICE_INVALID;
					sendto(Ap.sockfd, resp, sizeof(resp), MSG_CONFIRM,
						(struct sockaddr *)&client, sizeof(client));
				}
				
			}
			
			break;
		case OP_XFER_DATA:
			{
				if (Ap.xferFD < 1) {
					log(LVL2, "Got data packet, but no file open");
					return;
				}
				uint16_t reqVer = ntohs(*(uint16_t *)(buf + 2));
				if (reqVer != Ap.retrieveVersion) {
					log(LVL2, "Received packet for wrong version. ignoring.");
					return;
				}
				
				uint64_t slice_id = ntohl(*(uint32_t *)(buf + 4));
				if (slice_id > Ap.retrieveSize / 64000) {
					log(LVL2, "Got slice ID %d, but max id = %d", slice_id, Ap.retrieveSize / 64000);
					return;
				}
				
				static int64_t slices_ip[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
				static struct aiocb aio_callbacks[8];
				
				int t;
				int_fast8_t idx = -1;
				// we'll loop through all to see if they're done.
				// the first available slot is grabbed for idx.
				for (int_fast8_t i = 0; i < 8; i++) {
					if (slices_ip[i] != -1) {
						// check if this one's done, and close it out if so.
						t = aio_error(aio_callbacks + i);
						if (t == 0) {
							// this one is done. close out.
							SETRETRBIT(slices_ip[i]);
							aio_return(aio_callbacks + i);
							slices_ip[i] = -1;
							free((void *)aio_callbacks[i].aio_buf);
							idx = i;
						} else if (t != EINPROGRESS) {
							// an error occurred.
							log(LVL1, "Write of slice [%d] failed: %s",
								slices_ip[i], strerror(t));
							aio_return(aio_callbacks + i);
							slices_ip[i] = -1;
							free((void *)aio_callbacks[i].aio_buf);
							idx = i;
						}
					}
					if (idx == -1 && slices_ip[i] == -1) idx = i;
				}
				if (idx == -1) {
					// there's not one available to use. we need to wait for one.
					log(LVL2, "No aio handles available. Waiting for one.");
					aio_suspend((const struct aiocb *const *)aio_callbacks, 8, NULL);
					log(LVL2, "Finished waiting for a handle.");
					// we've already looped through them for cleaning once.
					// now we just care about moving on. find the first done
					// one and set it as our winner.
					for (int_fast8_t i = 0; i < 8; i++) {
						t = aio_error(aio_callbacks + i);
						if (t == 0) {
							aio_return(aio_callbacks + i);
							SETRETRBIT(slices_ip[i]);
							slices_ip[i] = -1;
							free((void *)aio_callbacks[i].aio_buf);
							idx = i;
							break;
						} else if (t != EINPROGRESS) {
							log(LVL1, "Write of slice [%d] failed: %s",
								slices_ip[i], strerror(t));
							aio_return(aio_callbacks + i);
							slices_ip[i] = -1;
							free((void *)aio_callbacks[i].aio_buf);
							idx = i;
							break;
						}
					}
					if (idx == -1) {
						// this should not be possible, as aio_suspend promised
						// us one of these was finished.
						log(LVL1, "aio_suspend returned, but no callbacks reported done.");
						exit(1);
					}
				}
				
				if (RETRIEVEDBIT(slice_id)) {
					log(LVL3, "Already had slice %d", slice_id);
					return;
				}
				
				// make sure we're not already working on writing this slice.
				for (int_fast8_t i = 0; i < 8; i++) {
					if (slices_ip[i] == (int64_t)slice_id) {
						log(LVL3, "Already writing slice [%d]", slice_id);
						return;
					}
				}
				
				// prepare the aio handle.
				memset(&aio_callbacks[idx], 0, sizeof(aio_callbacks[idx]));
				
				aio_callbacks[idx].aio_buf = malloc(n - 8);
				memcpy((void *)aio_callbacks[idx].aio_buf, buf + 8, n - 8);
				aio_callbacks[idx].aio_nbytes = n - 8;
				aio_callbacks[idx].aio_fildes = Ap.xferFD;
				aio_callbacks[idx].aio_offset = (off_t)slice_id * (off_t)64000;
				
				if (aio_write(&aio_callbacks[idx]) != 0) {
					log(LVL1, "Failed to initiate aio_write: %s", strerror(errno));
					free((void *)aio_callbacks[idx].aio_buf);
				} else {
					// our own bookkeeping.
					slices_ip[idx] = slice_id;
					log(LVL3, "Started write for slice [%d] in slot [%d]", slice_id, idx);
				}
				
			}
			break;
	}
}

void read_heartbeat_v1(struct sockaddr_in client, char *buf, size_t n) {
	log(LVL2, "Received v1 heartbeat from [%s:%d] - Current load: %.02f",
		inet_ntoa(client.sin_addr), ntohs(client.sin_port),
		((float)(ntohs(*(uint16_t *)(buf + 2))) / 100));
	
	int idx;
	if (!Ap.known_clients) {
		Ap.known_clients = (Client *)calloc(1, sizeof(Client));
		Ap.kc_count = 1;
		idx = 0;
	} else {
		for (idx = 0; idx < Ap.kc_count; idx++) {
			if (Ap.known_clients[idx].addr == client.sin_addr.s_addr) break;
		}
		if (idx == Ap.kc_count) {
			Ap.kc_count++;
			Ap.known_clients = (Client *)realloc(Ap.known_clients, sizeof(Client) * Ap.kc_count);
			Ap.known_clients[idx].clear();
			log(LVL2, "Increased known_clients count to %d", Ap.kc_count);
		}
	}
	
	Ap.known_clients[idx].addr = client.sin_addr.s_addr;
	Ap.known_clients[idx].port = client.sin_port;
	Ap.known_clients[idx].load = ((float)(ntohs(*(uint16_t *)(buf + 2))) / 100);
	strcpy(Ap.known_clients[idx].addr_string, inet_ntoa(client.sin_addr));
	Ap.known_clients[idx].lastHB = time(NULL);
	Ap.known_clients[idx].down = (uint8_t)buf[4] & FORCE_DOWN_BIT;
	Ap.known_clients[idx].backup = (uint8_t)buf[4] & FORCE_BACKUP_BIT;
	Ap.known_clients[idx].version = ntohs(*(uint16_t *)(buf + 5));
	if ((uint8_t)buf[4] & PRIMARY_BIT) {
		Ap.currentVersion = Ap.known_clients[idx].version;
		memcpy(&Ap.primaryAddr, &client, sizeof(Ap.primaryAddr));
	}
	
}

static void heartbeat(in_addr_t addr, short port, bool convert = false) {
	struct sockaddr_in client;
	memset(&client, 0, sizeof(client));
	
	static time_t last_version_check = 0;
	
	time_t now = time(NULL);
	
	client.sin_family = AF_INET;
	client.sin_port = convert ? htons(port) : port;
	client.sin_addr.s_addr = addr;
	
	char buf[7];
	buf[0] = CURRENT_VERSION;
	buf[1] = OP_HB;
	buf[4] = 0;
	
	if (*Ap.statusDownPath) {
		struct stat dummy;
		if (stat(Ap.statusDownPath, &dummy) == 0) {
			*(uint8_t *)&buf[4] |= FORCE_DOWN_BIT;
		}
	}
	if (Ap.shuttingDown) {
		*(uint8_t *)&buf[4] |= FORCE_DOWN_BIT;
	}
	
	if (*Ap.statusBackupPath) {
		struct stat dummy;
		if (stat(Ap.statusBackupPath, &dummy) == 0) {
			*(uint8_t *)&buf[4] |= FORCE_BACKUP_BIT;
		}
	}
	
	if (Ap.primary) {
		*(uint8_t *)&buf[4] |= PRIMARY_BIT;
	}
	
	// set data version
	if (last_version_check + Ap.cache_exp > (uint_fast32_t)now) {
		// use cached version, to minimize disk use.
		*(uint16_t *)&buf[5] = Ap.currentVersion;
	} else if (*Ap.changelogPath) {
		// prevent memory failures due to disappearing log etc.
		int fd = open(Ap.changelogPath, O_RDONLY);
		if (fd == -1) {
			log(LVL1, "Failed to open changelog: %s", strerror(errno));
			*(uint16_t *)&buf[5] = 0;
		} else {
			if (read(fd, &Ap.currentVersion, 2) != 2) {
				*(uint16_t *)&buf[5] = 0;
				log(LVL1, "Didn't read 2-byte version");
			} else {
				*(uint16_t *)&buf[5] = Ap.currentVersion;
				last_version_check = now;
			}
			close(fd);
		}
	} else {
		*(uint16_t *)&buf[5] = 0;
	}
	
	double avg;
	if (getloadavg(&avg, 1) == 1) {
		*(uint16_t *)(buf + 2) = htons((uint16_t)((float)(avg / Ap.ncores) * 100));
	}
	if (sendto(Ap.sockfd, buf, sizeof(buf), MSG_CONFIRM, (struct sockaddr *)&client, sizeof(client)) > 0) {
		log(LVL2, "Sent heartbeat to %s port %d", inet_ntoa(client.sin_addr), ntohs(client.sin_port));
	} else {
		log(LVL1, "Heartbeat send failed: %s", strerror(errno));
	}
}

static void printStatus() {
	time_t now = time(NULL);
	
	log(LVL1, "Log Level: %d (LVL%d)", Ap.logLvl,
		Ap.logLvl & LVL4 ? 4 : (Ap.logLvl & LVL3 ? 3 :
		(Ap.logLvl & LVL2 ? 2 : (Ap.logLvl & LVL1 ? 1 : 0))));
	log(LVL1, "Received HB clients: %d", Ap.kc_count);
	for (int i = 0; i < Ap.kc_count; i++) {
		Client *c = Ap.known_clients + i;
		log(LVL2, "\tClient: %s:%d [down=%d, backup=%d, load=%.02f, version=%d, "
			"last_hb=%ld (%ld seconds ago)]", c->addr_string,
			ntohs(c->port), c->down, c->backup, c->load, c->version, c->lastHB,
			now - c->lastHB);
	}
	
	extern char **environ;
	log(LVL3, "Environment:");
	for (char **p = environ; *p; p++) {
		log(LVL3, "\t%s", *p);
	}
	
	struct in_addr my_addr;
	my_addr.s_addr = Ap.myAddr;
	log(LVL1, "Bind address: %s:%d", inet_ntoa(my_addr), Ap.myport);
}

static void interrupt(int signal) {
	static time_t lastHBSent = 0;
	if (signal == SIGALRM) {
		// under normal circumstance, we get an alarm every 10 seconds.
		// while doing file transfers, we get them MUCH faster.
		// we want to ensure we're still waiting at least 5 seconds
		// between heartbeats.
		time_t now = time(NULL);
		if (now > (lastHBSent + 5) && Ap.hbclient) {
			lastHBSent = now;
			heartbeat(Ap.hbclient, Ap.cliport);
		}
		
		// send off the next file request if there is one active.
		file_req();
	}
	if (signal == SIGBUS) longjmp(Ap.jbuf, 1);
	if (signal == SIGUSR1) printStatus();
	if (signal == SIGTERM) {
		Ap.shuttingDown = true;
		log(LVL1, "Shutting down. Sending heartbeat.");
		if (*(uint32_t *)&Ap.hbclient) heartbeat(Ap.hbclient, Ap.cliport);
		exit(0);
	}
}

void usage(char **argv) {
	cerr << "Usage: " << argv[0] << " [ -qvri ] [ -a address ] [ -p port ] "
		"[ -e cache_exp ] [ -P pidpath ] [ -f confpath ] [ -s downpath ] -d ip[:port] "
		"[ -b backup_path ]\n\n"
		"-d ip[:port]\n\tSet destination ip [and port]\n"
		"-D dir\n\tSet starting directory for media files\n"
		"-q\n\tQuiet.  reduces logging level by 1\n"
		"-v\n\tVerbose.  Increases logging level by 1\n"
		"-r\n\tSet this node as the pRimary (-p and -P were already used)\n"
		"-i\n\tIgnore clients from confpath that have not sent a heartbeat. "
		"Meaningless without -f confpath\n"
		"-a address\n\tSet the bind address.  Default is to let the OS decide\n"
		"-c changelogpath\n\tSet path for media version/changelog\n"
		"-e cache_exp\n\tSet data version cache expiration in seconds (default: 60)\n"
		"-p port\n\tSet bind port.  Default is 8001\n"
		"-P pidpath\n\tSet the path to nginx PID for HUP. "
		"Meaningless without -f confpath\n"
		"-f confpath\n\tSet nginx config path to update based on client messages\n"
		"-s downpath\n\tSet path that, if it exists, this node will be marked down\n"
		"-b backup_path\n\tSet path that, if it exists, means this node is marked backup\n"
		"-t tmppath\n\tWhere to store files being transferred in (same drive "
		"as storage ensures fastest performance)\n\n";
	exit(1);
}

void process_args(int argc, char **argv) {
	int c;
	extern char *optarg;
	
	Ap.logLvl = LVL1;
	Ap.myport = DEFAULT_PORT;
	Ap.cliport = htons(DEFAULT_PORT);
	Ap.cache_exp = 60;
	strcpy(Ap.tmpFilePath, "/tmp");
	
	while ((c = getopt(argc, argv, "vqhia:c:e:d:D:f:n:p:P:s:t:r")) != -1) {
		log(LVL4, "getopt returned [%c]", (char)c);
		switch (c) {
			case 'a':
				// set bind address
				Ap.myAddr = inet_addr(optarg);
				break;
			case 'b':
				strcpy(Ap.statusBackupPath, optarg);
				break;
			case 'c':
				strcpy(Ap.changelogPath, optarg);
				break;
			case 'd':
				{
					char *c = strchr(optarg, ':');
					if (c) {
						*c = 0;
						Ap.cliport = htons(atol(c + 1));
					}
					Ap.hbclient = inet_addr(optarg);
					log(LVL2, "Set hbclient to %d.%d.%d.%d", *(uint8_t *)&Ap.hbclient,
						*(((uint8_t *)&Ap.hbclient) + 1), *(((uint8_t *)&Ap.hbclient) + 2),
						*(((uint8_t *)&Ap.hbclient) + 3));
				}
				break;
			case 'D':
				strcpy(Ap.filesRoot, optarg);
				break;
			case 'e':
				Ap.cache_exp = atol(optarg);
				break;
			case 'n':
				switch (*optarg) {
					case 'p':
						Ap.notifyFunc = pushover::notify;
						break;
					default:
						log(LVL1, "Invalid notification mechanism [%s]", optarg);
						usage(argv);
						break;
				}
				break;
			case 'p':
				Ap.myport = atol(optarg);
				break;
			case 'i':
				Ap.ignore_absent = true;
				break;
			case 'v':
				Ap.logLvl <<= 1;
				Ap.logLvl++;
				log(LVL3, "Log level set to %d", Ap.logLvl);
				break;
			case 'q':
				Ap.logLvl >>= 1;
				log(LVL3, "Log level set to %d", Ap.logLvl);
				break;
			case 'r':
				Ap.primary = true;
				break;
			case 'f':
				strcpy(Ap.configPath, optarg);
				break;
			case 'P':
				strcpy(Ap.pidPath, optarg);
				break;
			case 's':
				strcpy(Ap.statusDownPath, optarg);
				break;
			case 't':
				strcpy(Ap.tmpFilePath, optarg);
				break;
			default:
				usage(argv);
				break;
		}
	}
	
	if (*Ap.filesRoot) {
		chdir(Ap.filesRoot);
	}
}

void replaceMe(int argc, char **argv) {
	close(Ap.sockfd);
	execvp(argv[0], argv);
	
	// should be unreachable, but just in case.
	exit(1);
}

// instead of a standard split, consumes all consecutive delimiters,
// adding only non-empty fields,
// so awk_split("hello  world ", {' '}).size() == 2.
vector<string> awk_split(const string in, const unordered_set<char> delim) {
	vector<string> r;
	string rv;
	
	for (size_t i = 0; i < in.size(); i++) {
		if (!delim.count(in[i])) {
			rv.push_back(in[i]);
		} else if (rv.size()) {
			r.push_back(rv);
			rv.clear();
		}
	}
	
	if (rv.size()) r.push_back(rv);
	return r;
}

void checkClients() {
	// we get the list of clients from nginx config, check last heartbeat status
	// on each, determine if we need to make any changes, then HUP nginx if so.
	if (!*Ap.configPath) return;
	
	static time_t lastCheck = 0;
	
	time_t now = time(NULL);
	
	float down_threshold = 10;
	float backup_threshold = 2;
	
	if (lastCheck + 5 > now) return; // make sure we wait at least 5s between checks
	lastCheck = now;
	
	// one of the very few things we'll try to do in proper c++ fashion.
	ifstream confFile(Ap.configPath);
	if (!confFile) {
		log(LVL1, "Couldn't open config file");
		return;
	}
	
	char tempConf[] = "/tmp/nginxconfXXXXXX";
	int confFD = mkstemp(tempConf);
	if (confFD == -1) {
		log(LVL1, "Couldn't create temporary config file: %s", strerror(errno));
		return;
	}
	close(confFD);
	chmod(tempConf, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	ofstream confOut(tempConf);
	log(LVL3, "Checking clients from config");
	
	string inLine, input;
	bool anyChange = false;
	while (getline(confFile, inLine)) {
		input = inLine;
		size_t i;
		for (i = 0; i < input.size() && (input[i] == '\t' || input[i] == ' '); i++) {}
		input = input.substr(i);
		
		if (input.empty()) {
			confOut << inLine << '\n';
			continue;
		}
		if (input[0] == '#') {
			// check for threshold changes.
			size_t pos = input.find("backup_threshold=");
			if (pos != string::npos) {
				pos += 17;
				backup_threshold = stod(input.substr(pos));
				log(LVL2, "Changed backup threshold to %.02f", backup_threshold);
			}
			pos = input.find("down_threshold=");
			if (pos != string::npos) {
				pos += 15;
				down_threshold = stod(input.substr(pos));
				log(LVL2, "Changed down threshold to %.02f", down_threshold);
			}
		}
		
		if (!input.starts_with("server ") || input.back() != ';') {
			confOut << inLine << '\n';
			continue;
		}
		
		input.pop_back(); // don't need the semicolon.
		
		vector<string> parts = awk_split(input, {' ', '\t'});
		
		bool found = false, changed = false;
		for (i = 0; (int)i < Ap.kc_count; i++) {
			if (Ap.known_clients[i].addr_string == parts[1]) {
				found = true;
				break;
			}
		}
		
		log(LVL2, "Checking on client %s: %s", parts[1].c_str(), found ? "found" : "not found");
		if (!found && !Ap.ignore_absent) {
			// we add this in, and act like it just got a heartbeat,
			// such that it will soon be noted as having an OLD heartbeat.
			Ap.kc_count++;
			Ap.known_clients = (Client *)realloc(Ap.known_clients, sizeof(Client) * Ap.kc_count);
			Ap.known_clients[Ap.kc_count - 1].clear();
			strcpy(Ap.known_clients[Ap.kc_count - 1].addr_string, parts[1].c_str());
			Ap.known_clients[Ap.kc_count - 1].addr = inet_addr(parts[1].c_str());
			Ap.known_clients[Ap.kc_count - 1].lastHB = time(NULL);
			log(LVL2, "Added not-yet-seen client %s from config to known_clients", parts[1].c_str());
			// leave line as it was.
			confOut << inLine << '\n';
		} else if (found) {
			// check what we know on this client.
			bool down = false, backup = false, weightSet = false;
			uint16_t weight;
			size_t idx = i;
			for (i = 2; i < parts.size(); i++) {
				if (parts[i] == "backup") {
					backup = true;
				} else if (parts[i] == "down") {
					down = true;
				} else if (parts[i].starts_with("weight=")) {
					weightSet = true;
					weight = stoi(parts[i].substr(7));
				}
			}
			
			if (Ap.known_clients[idx].load > backup_threshold
				|| (Ap.currentVersion && Ap.known_clients[idx].version != Ap.currentVersion)
				) {
				Ap.known_clients[idx].backup = true;
			}
			
			// if the last heartbeat from this server was >20 seconds ago,
			// we mark it as down.
			if (Ap.known_clients[idx].lastHB + 20 < now
				|| Ap.known_clients[idx].load > down_threshold
				|| Ap.known_clients[idx].down) {
				Ap.known_clients[idx].down = true;
				Ap.known_clients[idx].backup = false; // no point in being both down and backup.
			}
			
			if (Ap.known_clients[idx].weightSet != weightSet
				|| (weightSet && Ap.known_clients[idx].weight != weight)
				|| Ap.known_clients[idx].down != down
				|| Ap.known_clients[idx].backup != backup) {
				
				changed = true;
			}
			
			if (changed) {
				anyChange = true;
				log(LVL1, "Updated %s to down=%d, backup=%d, weight=%d",
					Ap.known_clients[idx].addr_string,
					Ap.known_clients[idx].down,
					Ap.known_clients[idx].backup,
					Ap.known_clients[idx].weightSet ?
					Ap.known_clients[idx].weight : 1);
				
				if (Ap.notifyFunc) {
					char temp[200];
					snprintf(temp, sizeof(temp), "Updated %s to down=%d, backup=%d, weight=%d",
					Ap.known_clients[idx].addr_string,
					Ap.known_clients[idx].down,
					Ap.known_clients[idx].backup,
					Ap.known_clients[idx].weightSet ?
					Ap.known_clients[idx].weight : 1);
					(*Ap.notifyFunc)(temp, "Heartbeat Update");
				}
			}
			
			
			for (i = 0; i < inLine.size() && (inLine[i] == ' ' || inLine[i] == '\t'); i++) {
				confOut << inLine[i];
			}
			
			confOut << "server " << parts[1];
			if (Ap.known_clients[idx].down) {
				confOut << " down";
			}
			if (Ap.known_clients[idx].backup) {
				confOut << " backup";
			}
			if (Ap.known_clients[idx].weightSet) {
				confOut << " weight=" << Ap.known_clients[idx].weight;
			}
			confOut << ";\n";
		} else {
			// not found, but ignored.  put line back as it was.
			confOut << inLine << '\n';
		}
		
	}
	
	confFile.close();
	confOut.close();
	
	if (anyChange) {
		rename(tempConf, Ap.configPath);
	} else {
		remove(tempConf);
	}
	
	if (anyChange && *Ap.pidPath) {
		log(LVL1, "HUPing server", tempConf);
		confFile.open(Ap.pidPath);
		if (getline(confFile, input)) {
			try {
				pid_t pid = stoi(input);
				log(LVL1, "Sending HUP to %d", pid);
				if (kill(pid, SIGHUP) == -1) {
					log(LVL1, "Failed to HUP: %s", strerror(errno));
				}
			} catch (...) {
				log(LVL1, "Failed to get server PID");
			}
		}
	}
}

int main(int argc, char **argv) {
	struct sigaction act;
	memset(&act, 0, sizeof(act));
	
	act.sa_handler = interrupt;
	sigemptyset (&act.sa_mask);
	
	sigaction(SIGALRM, &act, NULL);
	sigaction(SIGUSR1, &act, NULL);
	sigaction(SIGTERM, &act, NULL);
	
	act.sa_handler = SIG_IGN;
	
	uint8_t buf[BUF_SIZ];
	struct sockaddr_in my_addr, client_addr;
	time_t now, lastMsg;
	now = lastMsg = time(NULL);
	
	struct itimerval alarmTime;
	memset(&alarmTime, 0, sizeof(alarmTime));
	
	memset(&Ap, 0, sizeof(Ap));
	
	Ap.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (Ap.sockfd < 0) {
		log(LVL1, "Failed to get socket: %s", strerror(errno));
		return 2;
	}
	
	process_args(argc, argv);
	
	alarmTime.it_interval.tv_sec = 10;
	alarmTime.it_value.tv_sec = 10;
	
	if (setitimer(ITIMER_REAL, &alarmTime, NULL) == -1) {
		log(LVL1, "setitimer failed: %s", strerror(errno));
		return 1;
	}
	
	memset(&my_addr, 0, sizeof(my_addr));
	memset(&client_addr, 0, sizeof(client_addr));
	
	my_addr.sin_family = AF_INET; // ipv4
	my_addr.sin_addr.s_addr = Ap.myAddr ? Ap.myAddr : INADDR_ANY;
	my_addr.sin_port = htons(Ap.myport);
	
	if (::bind(Ap.sockfd, (const struct sockaddr *)&my_addr, sizeof(my_addr)) < 0) {
		log(LVL1, "Failed to bind to port: %s", strerror(errno));
		return 2;
	}
	
	
	#ifdef __linux__
		Ap.ncores = get_nprocs();
	#else
		Ap.ncores = sysconf(_SC_NPROCESSORS_ONLN);
	#endif
	if (Ap.ncores == -1) {
		log(LVL1, "Failed to get processor count: %s", strerror(errno));
		return 1;
	}
	
	socklen_t clilen = sizeof(client_addr);
	int n;
	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGALRM);
	sigaddset(&sigset, SIGUSR1);
	
	while (true) {
		checkClients();
		
		errno = 0;
		
		memset(&client_addr, 0, sizeof(client_addr));
		
		if (sigprocmask(SIG_UNBLOCK, &sigset, NULL) == -1) { // ensure sigalrm can interrupt this call
			log(LVL1, "sigprocmask(): %s", strerror(errno));
		}
		
		n = recvfrom(Ap.sockfd, (char *)buf, sizeof(buf), MSG_WAITALL, (struct sockaddr *)&client_addr, &clilen);
		
		if (sigprocmask(SIG_BLOCK, &sigset, NULL) == -1) { // make sigalrm wait until we finish processing this request.
			log(LVL1, "sigprocmask(): %s", strerror(errno));
		}
		
		// check for 127.0.0.0/16
		if ((client_addr.sin_addr.s_addr & 0xffff) == 0x7f) {
			log(LVL1, "Processing admin request");
			if (n > 0) {
				if (buf[0] == 'v') {
					Ap.logLvl <<= 1;
					Ap.logLvl++;
					log(LVL2, "Log level increased to %d", Ap.logLvl);
				} else if (buf[0] == 'q') {
					Ap.logLvl >>= 1;
					log(LVL2, "Log level decreased to %d", Ap.logLvl);
				}
			}
			continue;
		}
		
		if ((now = time(NULL)) > lastMsg + 60) {
			log(LVL1, "No messages in 60s.  I suspect I've lost connection.  Closing out.");
			replaceMe(argc, argv);
		}
		
		if (errno == EINTR) {
			continue;
		}
		
		if (n <= 0) {
			log(LVL2, "recvfrom returned %d", n);
			if (errno) {
				log(LVL1, "recvfrom error: %s", strerror(errno));
				return 2;
			}
			continue;
		}
		
		log(LVL3, "Received %d bytes", n);
		
		if (n < 2) {
			log(LVL2, "Packet too short to mean anything");
			continue;
		}
		
		switch (buf[0]) {
			case 0:
				// unsupported version message. we'll do nothing. yay.
				break;
			case 1:
				switch (buf[1]) {
					case OP_HB:
						read_heartbeat_v1(client_addr, (char *)buf, n);
						ack(client_addr.sin_addr.s_addr, client_addr.sin_port);
						break;
					case OP_HBREQ:
						heartbeat(client_addr.sin_addr.s_addr, client_addr.sin_port);
						break;
					case OP_ACK:
						process_ack_v1(client_addr, (char *)buf, n);
						break;
					case OP_XFER_INIT:
					case OP_XFER_INFO:
					case OP_XFER_DREQ:
					case OP_XFER_DATA:
						file_process_v1(client_addr, (char *)buf, (size_t)n);
						break;
				}
				break;
			default:
				buf[0] = 0;
				buf[1] = OP_UNSUPVERS;
				sendto(Ap.sockfd, buf, 2, MSG_CONFIRM, (struct sockaddr *)&client_addr, clilen);
				log(LVL2, "Sent unsupported version to %s", inet_ntoa(client_addr.sin_addr));
				break;
		}

		lastMsg = time(NULL);
		
	}
	
	
	return 0;
}