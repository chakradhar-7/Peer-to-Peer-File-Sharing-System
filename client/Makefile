CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2 -pthread -Wno-deprecated-declarations
LDFLAGS = -lssl -lcrypto

# macOS: OpenSSL comes from Homebrew (brew install openssl@3), which isn't on the default search paths
ifeq ($(shell uname -s),Darwin)
OPENSSL := $(shell brew --prefix openssl@3)
CXXFLAGS += -I$(OPENSSL)/include
LDFLAGS := -L$(OPENSSL)/lib $(LDFLAGS)
endif

TARGET = client
SRC = client.cpp

all: $(TARGET)

$(TARGET): $(SRC) webui.h
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
