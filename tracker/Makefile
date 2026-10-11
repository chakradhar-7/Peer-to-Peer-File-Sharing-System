CXX := g++
CXXFLAGS := -std=c++17 -Wall -O2 -pthread -Wno-deprecated-declarations
LDFLAGS := -lcrypto

# macOS: OpenSSL comes from Homebrew (brew install openssl@3), which isn't on the default search paths
ifeq ($(shell uname -s),Darwin)
OPENSSL := $(shell brew --prefix openssl@3)
CXXFLAGS += -I$(OPENSSL)/include
LDFLAGS := -L$(OPENSSL)/lib $(LDFLAGS)
endif

tracker: tracker.cpp dashboard.h
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f tracker
