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
#define BUF_SIZ 1024
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

#define FORCE_DOWN_BIT   0x80
#define FORCE_BACKUP_BIT 0x40
#define PRIMARY_BIT      0x20

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
	// current DATA version - not CURRENT_VERSION.
	uint16_t currentVersion;
	
	jmp_buf jbuf;
	
	in_addr_t hbclient;
	
	int sockfd;
	
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

void _log(const char *file, int line, unsigned level, const char *msg, ...) {
	if (!(Ap.logLvl & level)) return;
	
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
	
	char buf[2];
	buf[0] = CURRENT_VERSION;
	buf[1] = OP_ACK;
	
	if (sendto(Ap.sockfd, buf, 2, MSG_CONFIRM, (struct sockaddr *)&client, sizeof(client)) > 0) {
		log(LVL2, "Sent ack to %s port %d", inet_ntoa(client.sin_addr), ntohs(client.sin_port));
	} else {
		log(LVL1, "ACK send failed: %s", strerror(errno));
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
	}
	
}

static void heartbeat(in_addr_t addr, short port, bool convert = false) {
	struct sockaddr_in client;
	memset(&client, 0, sizeof(client));
	
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
	if (*Ap.changelogPath) {
		// prevent memory failures due to disappearing log etc.
		int fd = open(Ap.changelogPath, O_RDONLY);
		if (fd == -1) {
			log(LVL1, "Failed to open changelog: %s", strerror(errno));
			*(uint16_t *)&buf[5] = 0;
		} else {
			if (read(fd, buf + 5, 2) != 2) {
				*(uint16_t *)&buf[5] = 0;
				log(LVL1, "Didn't read 2-byte version");
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

static void interrupt(int signal) {
	if (signal == SIGALRM && Ap.hbclient) heartbeat(Ap.hbclient, Ap.cliport);
	if (signal == SIGBUS) longjmp(Ap.jbuf, 1);
}

void usage(char **argv) {
	cerr << "Usage: " << argv[0] << " [ -qvri ] [ -a address ] [ -p port ] "
		"[ -P pidpath ] [ -f confpath ] [ -s downpath ] -d ip[:port]\n\n"
		"-d ip[:port]\n\tSet destination ip [and port]\n"
		"-q\n\tQuiet.  reduces logging level by 1\n"
		"-v\n\tVerbose.  Increases logging level by 1\n"
		"-r\n\tSet this node as the pRimary (-p and -P were already used)\n"
		"-i\n\tIgnore clients from confpath that have not sent a heartbeat. "
		"Meaningless without -f confpath\n"
		"-a address\n\tSet the bind address.  Default is to let the OS decide\n"
		"-c changelogpath\n\tSet path for media version/changelog\n"
		"-p port\n\tSet bind port.  Default is 8001\n"
		"-P pidpath\n\tSet the path to nginx PID for HUP. "
		"Meaningless without -f confpath\n"
		"-f confpath\n\tSet nginx config path to update based on client messages\n"
		"-s downpath\n\tSet path that, if it exists, this node will be marked down\n\n";
	exit(1);
}

void process_args(int argc, char **argv) {
	int c;
	extern char *optarg;
	
	Ap.logLvl = LVL1;
	Ap.myport = DEFAULT_PORT;
	Ap.cliport = htons(DEFAULT_PORT);
	
	while ((c = getopt(argc, argv, "vqhia:c:d:f:p:P:s:r")) != -1) {
		log(LVL4, "getopt returned %d", c);
		switch (c) {
			case 'a':
				// set bind address
				Ap.myAddr = inet_addr(optarg);
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
			default:
				usage(argv);
				break;
		}
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
	
	while (true) {
		
		checkClients();
		
		errno = 0;
		
		memset(&client_addr, 0, sizeof(client_addr));
		sigprocmask(SIG_UNBLOCK, &sigset, NULL); // ensure sigalrm can interrupt this call
		n = recvfrom(Ap.sockfd, (char *)buf, sizeof(buf), MSG_WAITALL, (struct sockaddr *)&client_addr, &clilen);
		sigprocmask(SIG_BLOCK, &sigset, NULL); // make sigalrm wait until we finish processing this request.
		
		if ((now = time(NULL)) > lastMsg + 60) {
			log(LVL1, "No messages in 60s.  I suspect I've lost connection.  Closing out.");
			replaceMe(argc, argv);
		}
		
		if (errno == EINTR) {
			continue;
		}
		
		lastMsg = now;
		
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
					case OP_ACK: /* already updated lastMsg; noop */
						break;
				}
				break;
			default:
				memset(buf, 0, sizeof(buf));
				buf[0] = 0;
				buf[1] = OP_UNSUPVERS;
				sendto(Ap.sockfd, buf, 2, MSG_CONFIRM, (struct sockaddr *)&client_addr, clilen);
				log(LVL2, "Sent unsupported version to %s", inet_ntoa(client_addr.sin_addr));
				break;
		}
	}
	
	
	return 0;
}