#include "log.hpp"
#include <stdio.h>
#include <stdarg.h>


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
		
		fprintf(stderr, "[%s:%d] ", file, line);
		
		va_list arglist;
		va_start(arglist, msg);
		vfprintf(stderr, msg, arglist);
		va_end(arglist);
		fputc('\n', stderr);
	}
}