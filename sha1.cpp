// a decent-pace sha1.
// reads messages chunk by chunk without storing the whole
// thing (just the last (incomplete) block).
// yes, there are faster implementations out there, but this one's mine
// and it's pretty dang good, all things considered.

// can process a file or a raw data buffer, and supports appending
// of more data after initialization up until raw() or hex() is called.

#include <iostream>
#include <string>
#include <exception>
#include <fstream>
#include <string.h>
#include <arpa/inet.h>
#include "sha1.hpp"

using namespace std;

sha1_word SHA1::ch(sha1_word x, sha1_word y, sha1_word z) {
	return (x & y) ^ ((~x) & z);
}

sha1_word SHA1::par(sha1_word x, sha1_word y, sha1_word z) {
	return x ^ y ^ z;
}

sha1_word SHA1::maj(sha1_word x, sha1_word y, sha1_word z) {
	return (x & y) ^ (x & z) ^ (y & z);
}

void SHA1::processBlock(const uint8_t *block) {
	#ifdef DEBUG
	for (int i = 0; i < 64; i++) {
		printf("%02x", block[i]);
	}
	printf("\n");
	cout << "Block counter " << blockCounter << endl;
	#endif
	blockCounter++;
	sha1_word a = values[0], b = values[1], c = values[2],
		d = values[3], e = values[4];
	sha1_word W[80];
	// each block is subjected to 80 operations
	for (short t = 0; t < 80; t++) {
		// initialize the message scheduler.
		if (t < 16) {
			uint64_t start_point = 4*t;
			W[t] = ((unsigned char)block[start_point]    << 24)
				+ ((unsigned char)block[start_point + 1] << 16)
				+ ((unsigned char)block[start_point + 2] <<  8)
				+ ((unsigned char)block[start_point + 3]);
		} else {
			W[t] = W[t - 3] ^ W[t - 8] ^ W[t - 14] ^ W[t - 16];
			W[t] = (sha1_word)(W[t] << 1) + (sha1_word)(W[t] >> 31);
		}
		sha1_word temp = (sha1_word)(a << 5) + (sha1_word)(a >> 27);
		// f_t(b, c, d), from the SHA1 description, broken out here:
		if (t < 20) {
			temp += ch(b, c, d);
		} else if (t < 40) {
			temp += par(b, c, d);
		} else if (t < 60) {
			temp += maj(b, c, d);
		} else {
			temp += par(b, c, d);
		}
		temp += e + starters[t/20] + W[t];
		
		e = d;
		d = c;
		c = (sha1_word)(b << 30) + (sha1_word)(b >> 2);
		b = a;
		a = temp;
		#ifdef DEBUG
		cout << "  [t = " << t << "] A=" << a << ", B=" << b << ", C=" << c << ", D=" << d << ", E=" << e << endl;
		#endif
	}
	values[0] += a;
	values[1] += b;
	values[2] += c;
	values[3] += d;
	values[4] += e;
}

void SHA1::finish() {
	// blockCounter contains the number of complete 512-bit blocks
	// are already on. lastBlockLen contains the number of BYTES that
	// are waiting to be calculated. Figure out where we lie in terms
	// of boundaries plus available space, append as necessary,
	// then finish the calculation and mark as done.
	uint64_t msgLen = (lastBlockLen * 8) + (blockCounter * 512);
	
	// we need at least 1 byte for end-of-message marker, plus
	// 8 bytes for the message length. See if we'll need another block.
	// because we ALWAYS process the block if it's full, we know we've
	// got at least one byte available.
	lastBlock[lastBlockLen++] = 0x80;
	// 0 the rest of the current block.
	memset(lastBlock + lastBlockLen, 0, sizeof(lastBlock) - lastBlockLen);
	if (lastBlockLen + 8 > (int)sizeof(lastBlock)) {
		// this block is ready to ship. send it off.
		processBlock(lastBlock);
		// zero the block so our last one is ready.
		memset(lastBlock, 0, sizeof(lastBlock));
	}
	
	// now we should have a block that's ready to have its last 8 bytes filled.
	// for best compatibility, we'll use htonl to get big endian, breaking
	// msgLen into 2 32-bit ints.
	*(uint32_t *)&lastBlock[56] = htonl((uint32_t)(msgLen >> 32));
	*(uint32_t *)&lastBlock[60] = htonl((uint32_t)(msgLen & 0xffffffff));
	
	processBlock(lastBlock);
	
	finished = true;
}

SHA1::SHA1() {}

SHA1::SHA1(const uint8_t *data, size_t bytes) {
	if (bytes) {
		const uint8_t *ptr = data;
		while (bytes >= 64) {
			processBlock(ptr);
			ptr += 64;
			bytes -= 64;
		}
		lastBlockLen = bytes;
		memcpy(lastBlock, ptr, bytes);
	}
}

SHA1::SHA1(string filename) {
	// we process the whole file.
	uint8_t buf[4096]; // we'll read 4k at a time for paging efficiency
	
	ifstream ifile(filename);
	if (!ifile) {
		throw "Failed to open file";
	}
	int offset;
	while (ifile.read((char *)buf, sizeof(buf))) {
		for (offset = 0; offset < (int)sizeof(buf); offset += 64) {
			processBlock(buf + offset);
		}
	}
	
	offset = 0;
	size_t bytes = ifile.gcount();
	while (bytes >= 64) {
		processBlock(buf + offset);
		offset += 64;
		bytes -= 64;
	}
	if (bytes) append(buf + offset, bytes);
	finish();
}

void SHA1::append(const uint8_t *data, size_t bytes) {
	if (finished) {
		throw "Cannot append to finished checksum";
	}
	if (!bytes) return;
	size_t pos = 0;
	while (bytes + lastBlockLen >= sizeof(lastBlock)) {
		memcpy(lastBlock + lastBlockLen, data + pos, sizeof(lastBlock) - lastBlockLen);
		bytes -= sizeof(lastBlock) - lastBlockLen;
		pos += sizeof(lastBlock) - lastBlockLen;
		processBlock(lastBlock);
		memset(lastBlock, 0, sizeof(lastBlock));
		lastBlockLen = 0;
	}
	memcpy(lastBlock + lastBlockLen, data + pos, bytes);
	lastBlockLen += bytes;
}

string SHA1::raw() {
		if (!finished) finish();
		string r;
		
		r.push_back((uint8_t)((values[0] & 0xff000000) >> 24));
		r.push_back((uint8_t)((values[0] & 0x00ff0000) >> 16));
		r.push_back((uint8_t)((values[0] & 0x0000ff00) >>  8));
		r.push_back((uint8_t)((values[0] & 0x000000ff) >>  0));
		r.push_back((uint8_t)((values[1] & 0xff000000) >> 24));
		r.push_back((uint8_t)((values[1] & 0x00ff0000) >> 16));
		r.push_back((uint8_t)((values[1] & 0x0000ff00) >>  8));
		r.push_back((uint8_t)((values[1] & 0x000000ff) >>  0));
		r.push_back((uint8_t)((values[2] & 0xff000000) >> 24));
		r.push_back((uint8_t)((values[2] & 0x00ff0000) >> 16));
		r.push_back((uint8_t)((values[2] & 0x0000ff00) >>  8));
		r.push_back((uint8_t)((values[2] & 0x000000ff) >>  0));
		r.push_back((uint8_t)((values[3] & 0xff000000) >> 24));
		r.push_back((uint8_t)((values[3] & 0x00ff0000) >> 16));
		r.push_back((uint8_t)((values[3] & 0x0000ff00) >>  8));
		r.push_back((uint8_t)((values[3] & 0x000000ff) >>  0));
		r.push_back((uint8_t)((values[4] & 0xff000000) >> 24));
		r.push_back((uint8_t)((values[4] & 0x00ff0000) >> 16));
		r.push_back((uint8_t)((values[4] & 0x0000ff00) >>  8));
		r.push_back((uint8_t)((values[4] & 0x000000ff) >>  0));
		
		return r;
	}

string SHA1::hex() {
		if (!finished) finish();
		string r;
		char hexChars[] = "0123456789abcdef";
		
		r += hexChars[(values[0] & 0xf0000000) >> 28];
		r += hexChars[(values[0] & 0x0f000000) >> 24];
		r += hexChars[(values[0] & 0x00f00000) >> 20];
		r += hexChars[(values[0] & 0x000f0000) >> 16];
		r += hexChars[(values[0] & 0x0000f000) >> 12];
		r += hexChars[(values[0] & 0x00000f00) >>  8];
		r += hexChars[(values[0] & 0x000000f0) >>  4];
		r += hexChars[(values[0] & 0x0000000f) >>  0];
		r += hexChars[(values[1] & 0xf0000000) >> 28];
		r += hexChars[(values[1] & 0x0f000000) >> 24];
		r += hexChars[(values[1] & 0x00f00000) >> 20];
		r += hexChars[(values[1] & 0x000f0000) >> 16];
		r += hexChars[(values[1] & 0x0000f000) >> 12];
		r += hexChars[(values[1] & 0x00000f00) >>  8];
		r += hexChars[(values[1] & 0x000000f0) >>  4];
		r += hexChars[(values[1] & 0x0000000f) >>  0];
		r += hexChars[(values[2] & 0xf0000000) >> 28];
		r += hexChars[(values[2] & 0x0f000000) >> 24];
		r += hexChars[(values[2] & 0x00f00000) >> 20];
		r += hexChars[(values[2] & 0x000f0000) >> 16];
		r += hexChars[(values[2] & 0x0000f000) >> 12];
		r += hexChars[(values[2] & 0x00000f00) >>  8];
		r += hexChars[(values[2] & 0x000000f0) >>  4];
		r += hexChars[(values[2] & 0x0000000f) >>  0];
		r += hexChars[(values[3] & 0xf0000000) >> 28];
		r += hexChars[(values[3] & 0x0f000000) >> 24];
		r += hexChars[(values[3] & 0x00f00000) >> 20];
		r += hexChars[(values[3] & 0x000f0000) >> 16];
		r += hexChars[(values[3] & 0x0000f000) >> 12];
		r += hexChars[(values[3] & 0x00000f00) >>  8];
		r += hexChars[(values[3] & 0x000000f0) >>  4];
		r += hexChars[(values[3] & 0x0000000f) >>  0];
		r += hexChars[(values[4] & 0xf0000000) >> 28];
		r += hexChars[(values[4] & 0x0f000000) >> 24];
		r += hexChars[(values[4] & 0x00f00000) >> 20];
		r += hexChars[(values[4] & 0x000f0000) >> 16];
		r += hexChars[(values[4] & 0x0000f000) >> 12];
		r += hexChars[(values[4] & 0x00000f00) >>  8];
		r += hexChars[(values[4] & 0x000000f0) >>  4];
		r += hexChars[(values[4] & 0x0000000f) >>  0];
		
		return r;
	}

#ifdef TEST
int main() {
	SHA1 s("temp.txt");
	cout << s.hex() << endl;
}
#endif