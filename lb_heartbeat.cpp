/*
This application runs on the primary load balancer plus one other node.
It facilitates a heartbeat between the two. If the primary ceases its heartbeat,
the backup will claim the IP specified by -a addr and send a gratuitous ARP
to the network to indicate it has taken over. When the primary comes back
online, it will learn from the backup that it has fallen to backup, at which
point it is to notify the backup it is back, reclaim the IP, and send gARP.
*/

#include "log.hpp"
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
#include <fcntl.h>
#include <net/if.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>

using namespace std;

#define REQ 1
#define ACK 2
#define DOWN 3

namespace pushover {bool notify(const char *m, const char *s);}

struct sockaddr_in client_addr;

struct APP_INFO {
	bool primary = false;
	short cliport = htons(8002);
	short myport = 8002;
	in_addr_t hbclient = 0;
	char addr[50] = {0};
	in_addr_t addr_t = 0;
	in_addr_t myAddr = 0;
	int sockfd = 0;
	char interface[20] = {0};
} Ap;

void usage(char **argv) {
	cerr << "Usage: " << argv[0] << " [ -qv ] -a addr { -p | -d destip:port } -i interface" << endl;
	exit(1);
}

void replaceMe(int argc, char **argv) {
	close(Ap.sockfd);
	execvp(argv[0], argv);
	
	// should be unreachable, but just in case.
	exit(1);
}

bool active_c = false;
time_t active_ct = 0;

// checks with ip(8) if the IP is already assigned to this machine.
bool check_active() {
	bool ret = false;
	char buf[512];
	snprintf(buf, sizeof(buf), "ip address show dev %s to %s/32", Ap.interface, Ap.addr);
	FILE *f = popen(buf, "r");
	while (!feof(f)) {
		if (!fgets(buf, sizeof(buf), f)) break;
		if (strstr(buf, Ap.addr)) {
			ret = true;
			break;
		}
	}
	pclose(f);
	return ret;
}

bool set_active() {
	if (check_active()) return true;
	
	logger::log(LVL1, "Setting myself as active");
	if (!Ap.primary) pushover::notify("Backup taking over from LB", "Load Balancer Update");
	
	char buf[512];
	snprintf(buf, sizeof(buf), "ip address add %s/24 dev %s", Ap.addr, Ap.interface);
	FILE *f = popen(buf, "r");
	int ret = pclose(f);
	if (ret != 0) {
		logger::log(LVL1, "Failed to add IP address. Ret [%d]", ret);
		return false;
	}
	return true;
}

bool set_inactive() {
	if (!check_active()) return true;
	
	logger::log(LVL1, "Setting myself as inactive");
	if (!Ap.primary) pushover::notify("Returning IP to primary LB", "Load Balancer Update");
	
	char buf[512];
	snprintf(buf, sizeof(buf), "ip address del %s/24 dev %s", Ap.addr, Ap.interface);
	FILE *f = popen(buf, "r");
	int ret = pclose(f);
	if (ret != 0) {
		logger::log(LVL1, "Failed to delete IP address. Ret [%d]", ret);
		return false;
	}
	return true;
}

void process_args(int argc, char **argv) {
	
	int c;
	extern char *optarg;
	
	while ((c = getopt(argc, argv, "qvpa:b:d:i:P:?")) != -1) {
		if (strchr("abdiP", c)) {
			logger::log(LVL2, "[-%c] = [%s]", (char)c, optarg);
		} else {
			logger::log(LVL2, "[-%c]", (char)c);
		}
		switch (c) {
			case 'q':
				logger::log(LVL3, "Log level decreased to %d", logger::lvlDec());
				break;
			case 'v':
				logger::log(LVL3, "Log level decreased to %d", logger::lvlInc());
				break;
			case 'p':
				Ap.primary = true;
				break;
			case 'a':
				strcpy(Ap.addr, optarg);
				Ap.addr_t = inet_addr(optarg);
				break;
			case 'b':
				Ap.myAddr = inet_addr(optarg);
				break;
			case 'd':
				{
					char *c = strchr(optarg, ':');
					if (c) {
						*c = 0;
						Ap.cliport = htons(atol(c + 1));
					}
					Ap.hbclient = inet_addr(optarg);
					logger::log(LVL2, "Set hbclient to %d.%d.%d.%d", *(uint8_t *)&Ap.hbclient,
						*(((uint8_t *)&Ap.hbclient) + 1), *(((uint8_t *)&Ap.hbclient) + 2),
						*(((uint8_t *)&Ap.hbclient) + 3));
				}
				break;
			case 'i':
				strcpy(Ap.interface, optarg);
				break;
			case 'P':
				Ap.myport = atol(optarg);
				break;
			default:
				usage(argv);
				break;
		}
	}
	
	if (!*Ap.interface || !*Ap.addr || (!Ap.primary && !Ap.hbclient)) {
		usage(argv);
	}
}

void heartbeat() {
	// we put a nice counter in the packet for bookkeeping.
	// yes, this makes it basically a PING.
	static uint16_t seq = 0;
	seq++;
	
	char buf[4];
	buf[0] = REQ;
	buf[1] = check_active();
	*(uint16_t *)(buf + 2) = htons(seq);
	
	struct sockaddr_in client;
	memset(&client, 0, sizeof(client));
	
	client.sin_family = AF_INET;
	client.sin_port = Ap.cliport;
	client.sin_addr.s_addr = Ap.hbclient;
	
	if (sendto(Ap.sockfd, buf, sizeof(buf), MSG_CONFIRM, (struct sockaddr *)&client, sizeof(client)) > 0) {
		logger::log(LVL2, "Sent heartbeat seq [%d] to %s port %d", (int)seq,
			inet_ntoa(client.sin_addr), ntohs(client.sin_port));
	} else {
		logger::log(LVL1, "Heartbeat send failed: %s", strerror(errno));
	}
}

void send_down() {
	// we're sending this off to whatever our last client was.
	if (!*(uint32_t *)&client_addr.sin_addr.s_addr) {
		logger::log(LVL1, "No client addr available. Not sending DOWN");
		return;
	}
	
	char buf;
	buf = DOWN;
	
	if (sendto(Ap.sockfd, &buf, 1, MSG_CONFIRM, (struct sockaddr *)&client_addr, sizeof(client_addr)) > 0) {
		logger::log(LVL2, "Sent DOWN notification to %s port %d",
			inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
	} else {
		logger::log(LVL1, "Failed to send DOWN notification: %s", strerror(errno));
	}
}

void interrupt(int sig) {
	if (sig == SIGTERM) {
		send_down();
		logger::log(LVL1, "Shutting down at system request.");
		exit(0);
	}
	if (!Ap.primary) heartbeat();
	if (Ap.primary) set_active();
}

// process ACK of a heartbeat.
void process_ack(struct sockaddr_in *client, char *buf, int n) {
	if (n != 4) {
		logger::log(LVL1, "Incorrect size [%d] for ACK packet (4 expected)", n);
		return;
	}
	if (Ap.primary) {
		logger::log(LVL1, "Received ACK despite being primary");
		return;
	}
	
	uint16_t seq = ntohs(*(uint16_t *)(buf + 2));
	logger::log(LVL3, "Received ACK for seq [%d]", (int)seq);
	
	// we're the backup, and we got an ack. we should always then be inactive.
	set_inactive();
}


void send_garp() {
	extern int garp (uint8_t *src_ip, char *interface);
	logger::log(LVL1, "Sending garp");
	// we'll send 3 garps, 50 ms between each.
	garp((uint8_t *)&Ap.addr_t, Ap.interface);
	usleep(50000);
	garp((uint8_t *)&Ap.addr_t, Ap.interface);
	usleep(50000);
	garp((uint8_t *)&Ap.addr_t, Ap.interface);
	logger::log(LVL2, "garp sent");
}

void process_down(struct sockaddr_in *client, char *buf, int n) {
	// if we're not the primary, this is an indication from the primary
	// that it's on its way down, and we should take over.
	if (!Ap.primary && !check_active()) {
		set_active();
		send_garp();
	}
}

// process a REQ (heartbeat/request for ack)
void process_req(struct sockaddr_in *client, char *buf, int n) {
	if (n != 4) {
		logger::log(LVL1, "Incorrect size [%d] for REQ packet (4 expected)", n);
		return;
	}
	
	if (!Ap.primary) {
		logger::log(LVL1, "Received REQ despite being backup");
		return;
	}
	
	uint16_t seq = ntohs(*(uint16_t *)(buf + 2));
	logger::log(LVL2, "Processing REQ with seq [%d]", (int)seq);
	
	bool active = buf[1];
	buf[0] = ACK;
	buf[1] = 1;
	
	if (sendto(Ap.sockfd, buf, n, MSG_CONFIRM, (struct sockaddr *)client, sizeof(*client)) > 0) {
		logger::log(LVL2, "Sent ACK seq [%d] to %s port %d", (int)seq,
			inet_ntoa(client->sin_addr), ntohs(client->sin_port));
	} else {
		logger::log(LVL1, "Failed to send ACK: %s", strerror(errno));
	}
	
	if (active) {
		// backup claims to be active. we just told it to cut that out,
		// but for good measure, we need to make sure we're active and tell
		// the network about it.
		set_active();
		send_garp();
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
	
	uint8_t buf[10];
	// client_addr global so send_down can use it.
	struct sockaddr_in my_addr;
	time_t now, lastMsg;
	now = lastMsg = time(NULL);
	
	struct itimerval alarmTime;
	memset(&alarmTime, 0, sizeof(alarmTime));
	
	Ap.sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (Ap.sockfd < 0) {
		logger::log(LVL1, "Failed to get socket: %s", strerror(errno));
		return 2;
	}
	
	process_args(argc, argv);
	
	// run thrice per second, since this is a nearly-no-footprint app and we want
	// near zero downtime. 1 second without a response is deemed down.
	alarmTime.it_interval.tv_usec = 333333;
	alarmTime.it_value.tv_usec = 333333;
	
	// set a timer if we're not primary. primary receives and responds.
	if (setitimer(ITIMER_REAL, &alarmTime, NULL) == -1) {
		logger::log(LVL1, "setitimer failed: %s", strerror(errno));
		return 1;
	}
	
	memset(&my_addr, 0, sizeof(my_addr));
	memset(&client_addr, 0, sizeof(client_addr));
	
	my_addr.sin_family = AF_INET; // ipv4
	my_addr.sin_addr.s_addr = Ap.myAddr ? Ap.myAddr : INADDR_ANY;
	my_addr.sin_port = htons(Ap.myport);
	
	if (::bind(Ap.sockfd, (const struct sockaddr *)&my_addr, sizeof(my_addr)) < 0) {
		logger::log(LVL1, "Failed to bind to port: %s", strerror(errno));
		return 2;
	}
	
	socklen_t clilen = sizeof(client_addr);
	int n;
	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGALRM);
	sigaddset(&sigset, SIGUSR1);
	sigaddset(&sigset, SIGTERM);
	
	while (true) {
		errno = 0;
		
		memset(&client_addr, 0, sizeof(client_addr));
		
		if (sigprocmask(SIG_UNBLOCK, &sigset, NULL) == -1) { // ensure sigalrm can interrupt this call
			logger::log(LVL1, "sigprocmask(): %s", strerror(errno));
		}
		
		n = recvfrom(Ap.sockfd, (char *)buf, sizeof(buf), MSG_WAITALL, (struct sockaddr *)&client_addr, &clilen);
		
		if (sigprocmask(SIG_BLOCK, &sigset, NULL) == -1) { // make sigalrm wait until we finish processing this request.
			logger::log(LVL1, "sigprocmask(): %s", strerror(errno));
		}
		
		// check for 127.0.0.0/16
		if ((client_addr.sin_addr.s_addr & 0xffff) == 0x7f) {
			logger::log(LVL1, "Processing admin request");
			if (n > 0) {
				if (buf[0] == 'v') {
					logger::log(LVL2, "Increased log level to %d", logger::lvlInc());
				} else if (buf[0] == 'q') {
					logger::log(LVL2, "Decreased log level to %d", logger::lvlDec());
				}
			}
			continue;
		}
		
		// expect a message at least once a minute. if we don't get one because the
		// other node is down, that's fine; our state will stay the same
		// and the worst case is that the backup sends an extra garp.
		if ((now = time(NULL)) > lastMsg + 60) {
			logger::log(LVL1, "No messages in 60s.  I suspect I've lost connection.  Closing out.");
			replaceMe(argc, argv);
		}
		
		if (!Ap.primary && now > lastMsg + 1) {
			if (!check_active()) {
				set_active();
				send_garp();
			}
		}
		
		if (errno == EINTR) {
			continue;
		}
		
		if (n <= 0) {
			logger::log(LVL2, "recvfrom returned %d", n);
			if (errno) {
				logger::log(LVL1, "recvfrom error: %s", strerror(errno));
				return 2;
			}
			continue;
		}
		
		logger::log(LVL3, "Received %d bytes", n);
		
		if (n < 2) {
			logger::log(LVL2, "Packet too short to mean anything");
			continue;
		}
		
		switch(buf[0]) {
			case REQ:
				process_req(&client_addr, (char *)buf, n);
				break;
			case ACK:
				process_ack(&client_addr, (char *)buf, n);
				break;
			case DOWN:
				process_down(&client_addr, (char *)buf, n);
				break;
			default:
				logger::log(LVL1, "Unknown OP: %d", (int)buf[0]);
				break;
		}
		
		lastMsg = time(NULL);
		
	}
	
	
	return 0;
}