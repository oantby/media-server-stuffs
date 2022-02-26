#include "log.hpp"
#include <stdio.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>


namespace logger {
	
	unsigned logLvl = 1;
	
	unsigned lvlInc() {
		logLvl = (logLvl << 1) + 1;
		return logLvl;
	}
	
	unsigned lvlDec() {
		logLvl >>= 1;
		return logLvl;
	}
	
	unsigned lvlSet(unsigned l) {
		logLvl = l;
		return logLvl;
	}
	
	void _log(const char *file, int line, unsigned level, const char *msg, ...) {
		if (!(logLvl & level)) return;
		
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
			strftime(ds, sizeof(ds), "%Y-%m-%dT%H%M%S", localtime(&tv.tv_sec));
			fprintf(stderr, "[%s.%d] ", ds, (int)(tv.tv_usec / 10000));
		}
		fprintf(stderr, "[%s:%d] ", file, line);
		
		va_list arglist;
		va_start(arglist, msg);
		vfprintf(stderr, msg, arglist);
		va_end(arglist);
		fputc('\n', stderr);
	}
}