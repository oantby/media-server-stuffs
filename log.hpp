#ifndef LOG_HPP
#define LOG_HPP

#include <stdarg.h>

namespace logger {
	
	#define LVL1 1
	#define LVL2 2
	#define LVL3 4
	#define LVL4 8
	
	#define log(...) _log(__FILE__, __LINE__, __VA_ARGS__)
	void _log(const char *file, int line, unsigned level, const char *msg, ...);
	
	unsigned lvlInc();
	unsigned lvlDec();
	unsigned lvlSet(unsigned);
}

#endif