#include <iostream>
#include <curl/curl.h>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;

namespace pushover {
	bool notify(const char *msg, const char *subject = NULL) {
		CURL *curl = curl_easy_init();
		if (!curl) {
			cerr << "Failed to initialize CURL instance" << endl;
			return false;
		}
		
		json j;
		j["message"] = msg;
		char *p = getenv("PO_USER");
		if (!p) {
			cerr << "PO_USER unset" << endl;
			return false;
		}
		
		j["user"] = p;
		
		p = getenv("PO_TOKEN");
		if (!p) {
			cerr << "PO_TOKEN unset" << endl;
			return false;
		}
		
		j["token"] = p;
		
		j["title"] = subject ? subject : "Heartbeat Notification";
		
		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Content-Type: application/json");
		if (!headers) {
			cerr << "Failed to set content type header" << endl;
		}
		
		curl_easy_setopt(curl, CURLOPT_URL, "https://api.pushover.net/1/messages.json");
		curl_easy_setopt(curl, CURLOPT_POST, 1);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_COPYPOSTFIELDS, j.dump().c_str());
		curl_easy_perform(curl);
		
		curl_easy_cleanup(curl);
		
		curl_slist_free_all(headers);
		
		return true;
	}
};