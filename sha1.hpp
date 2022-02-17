#ifndef SHA1_HPP
#define SHA1_HPP

#include <iostream>
using namespace std;

typedef uint32_t sha1_word;

class SHA1 {
	const sha1_word starters[4] = {0x5a827999, 0x6ed9eba1, 0x8f1bbcdc, 0xca62c1d6};
	sha1_word values[5] = {0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0};
	// buffer for a block currently in progress.
	uint8_t lastBlock[64] = {0};
	// length of data currently in lastBlock.
	short lastBlockLen = 0;
	uint64_t blockCounter = 0;
	// flag for whether or not we've appended the length and done our final calculations.
	bool finished = false;
	
	sha1_word ch(sha1_word x, sha1_word y, sha1_word z);
	sha1_word par(sha1_word x, sha1_word y, sha1_word z);
	sha1_word maj(sha1_word x, sha1_word y, sha1_word z);
	void processBlock(const uint8_t *block);
	void finish();
	
	public:
	
	SHA1();
	SHA1(string filename);
	// all writes are done in BYTES, not bits. No partial bytes taken.
	SHA1(const uint8_t *data, size_t bytes);
	void append(const uint8_t *data, size_t bytes);
	
	string raw();
	string hex();
};

#endif