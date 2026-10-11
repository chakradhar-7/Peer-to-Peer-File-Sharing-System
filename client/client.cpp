#include<iostream>
#include<string>
#include<cstring>
#include<vector>
#include<utility>
#include<sstream>
#include<cstdio>
#include<cstdlib>
#include<cerrno>
#include<arpa/inet.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<netinet/tcp.h>
#include<sys/socket.h>
#include<sys/uio.h>
#include<unistd.h>
#include<thread>
#include<openssl/sha.h>
#include<openssl/rand.h>
#include<mutex>
#include<map>
#include<set>
#include<deque>
#include<memory>
#include<random>
#include<algorithm>
#include<condition_variable>
#include<atomic>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include<tuple>
#include<csignal>
#include<climits>
#include<poll.h>
#if defined(__linux__)
#include<sys/sendfile.h>
#endif
#include "webui.h"

using namespace std;

string p_sip;
string p_sp;
pair<string, string> tracker1,tracker2;
static bool con=true;

#define PIECE_SIZE 524288
#define PIPELINE 4          // piece requests kept in flight on each peer connection
#define GIVE_UP_AFTER 30    // seconds without a new piece before a download pauses
// largest message accepted from the tracker (a 1GB file's metadata is ~85KB)
#define MAX_MSG (64*1024*1024)
#define HDR_SIZE 6          // message header: type, status, 4-byte length

// first header byte: what the payload is. Same values as the tracker
enum MsgType { MSG_REQ=1, MSG_REPLY=2, MSG_PIECE=3 };

// second header byte of a reply. Same numbers as the tracker
enum Status {
    ST_OK=0,
    ST_BAD_REQUEST=1,   // malformed message or wrong arguments
    ST_NOT_LOGGED_IN=2,
    ST_AUTH_FAILED=3,   // unknown user, wrong password, or expired session
    ST_EXISTS=4,        // user, group or membership already exists
    ST_NOT_FOUND=5,     // group, file, piece or join request doesn't exist
    ST_FORBIDDEN=6,     // not a member or not the owner
    ST_CONFLICT=7,      // a different file with this name is already shared
    ST_UNAVAILABLE=8,   // peer is not sharing right now (logged out)
    ST_UNKNOWN_CMD=9
};

// P2P_TEST_DELAY_MS: wait this long before serving each piece (only for testing)
int test_delay_ms=0;

//login, read by download and heartbeat threads too, so guarded by mauth. The password is
//not kept: a new connection continues the session with the token the tracker gave us
mutex mauth;
static bool gl = false;
static string gu, gtoken;
static string lastUser; // whose files sharedFiles holds

// the connection to the tracker, shared by the shell, the heartbeat and download threads
mutex msock;
int tsock=-1;
atomic<int> preferred(0);  // tracker tried first (0 or 1); flips to the other when a call fails
mutex mtrk;
map<int,int> sockTracker;  // socket -> tracker it is connected to

// one peer of a download
struct PeerInfo{
    string uid,ip,port;
    bool complete=false;    // has the whole file
    vector<char> bits;      // pieces it can serve (for a peer still downloading)
    bool banned=false;      // sent a corrupted piece: not used again for this download
    bool running=false;     // a worker is fetching from it now
    int sock=-1;            // that worker's connection
    int fails=0;            // connection failures in a row
    time_t retry_after=0;   // earliest reconnect after a failure
    int pieces=0;           // pieces it delivered
};

// one download in progress
struct DownloadState{
    string gid,fname,dest,fhash,user;
    size_t size=0;
    int n=0;                    // number of pieces
    vector<string> ph;
    int fd=-1;
    mutex savem;                // one writer of <dest>.part at a time
    mutex m;                    // guards everything below
    condition_variable cv;
    vector<char> have;          // pieces downloaded and verified (also served to other peers)
    vector<int> inflight;       // requests outstanding per piece
    vector<int> avail;          // usable peers that have each piece
    map<string,PeerInfo> peers; // by ip:port
    int done=0;
    int since_save=0;
    bool finished=false;
};

// a file this client serves
struct ShareInfo{
    string path;
    size_t size=0;
    string fhash;
    vector<string> ph;
    shared_ptr<DownloadState> dl;   // set while still downloading: only finished pieces are served
};

// download status
mutex mdown;
map<string,string> downloads; // key = gid:fname, value = status
map<string,shared_ptr<DownloadState>> active; // downloads running now

// shared files for upload
mutex mf;
map<string,ShareInfo> sharedFiles; // gid:fname -> file
atomic<bool> sharing(false);       // pieces are only served while logged in

// what survives a restart of this client: shared files and unfinished downloads, per user
mutex mstatef;
string statePath;                  // p2p_client_<port>.state in the working directory
map<string,string> persisted;      // "<kind> <user> <gid> <fname>" -> line

// the terminal and the web page both run commands: one at a time
mutex mcmd;

int logFd = -1;
mutex logMutex;
void initLogger() {
    logFd = open("cl.log", O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (logFd < 0) {
        perror("log open");
    }
    if (logFd >= 0) cout << "\n";
}

void logmsg(const string &msg) {
    if (logFd < 0) return;
    lock_guard<mutex> lk(logMutex);

    time_t now = time(nullptr);
    char tbuf[64];
    strftime(tbuf, sizeof(tbuf), "[%F %T] ", localtime(&now));

    string line = string(tbuf) + msg + "\n";
    ssize_t n=write(logFd, line.c_str(), line.size());
    if(n<0){}
}

vector<string> splits(const string &buffer) {
    stringstream ss(buffer);
    vector<string> v;
    string w;
    while(ss>>w){
        v.push_back(w);
    }
    return v;
}

// strict number parse: returns false instead of throwing when s isn't a whole number
bool parse_num(const string &s,long long &out){
    if(s.empty())
        return false;
    char *end;
    errno=0;
    out=strtoll(s.c_str(),&end,10);
    return *end=='\0'&&errno==0;
}

bool valid_port(const string &s){
    long long p;
    return parse_num(s,p)&&p>=1&&p<=65535;
}

// send() may write only part of the buffer, so keep going until all of it is out
bool send_all(int sock,const char *data,size_t len){
    size_t sent=0;
    while(sent<len){
        ssize_t n=send(sock,data+sent,len-sent,0);
        if(n<0&&errno==EINTR)
            continue;
        if(n<=0)
            return false;
        sent+=n;
    }
    return true;
}

bool recv_all(int sock,char *data,size_t len){
    size_t got=0;
    while(got<len){
        ssize_t n=recv(sock,data+got,len-got,0);
        if(n<0&&errno==EINTR)
            continue;
        if(n<=0)
            return false;
        got+=n;
    }
    return true;
}

struct Msg{
    uint8_t type=0;
    uint8_t status=0;
    string payload;
};

// every message on every connection: type (1 byte), status (1 byte), payload length
// (4 bytes, network byte order), payload. The length tells the receiver exactly how much
// to read, however TCP splits the bytes up
string header(uint8_t type,uint8_t status,uint32_t len){
    char h[HDR_SIZE];
    h[0]=(char)type;
    h[1]=(char)status;
    uint32_t n=htonl(len);
    memcpy(h+2,&n,4);
    return string(h,HDR_SIZE);
}

bool send_msg(int sock,uint8_t type,uint8_t status,const string &payload){
    string buf=header(type,status,payload.size())+payload;
    return send_all(sock,buf.data(),buf.size());
}

bool recv_msg(int sock,Msg &m,size_t maxlen=MAX_MSG){
    char h[HDR_SIZE];
    if(!recv_all(sock,h,HDR_SIZE))
        return false;
    uint32_t len;
    memcpy(&len,h+2,4);
    len=ntohl(len);
    if(len>maxlen)
        return false;
    m.type=(uint8_t)h[0];
    m.status=(uint8_t)h[1];
    m.payload.assign(len,'\0');
    return len==0||recv_all(sock,&m.payload[0],len);
}

// a stalled peer or tracker makes send/recv fail after sec seconds instead of hanging forever
void set_timeout(int sock,int sec){
    struct timeval tv;
    tv.tv_sec=sec;
    tv.tv_usec=0;
    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    setsockopt(sock,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
}

// send small messages (headers, requests) at once instead of waiting to batch them
void set_nodelay(int sock){
    int one=1;
    setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
}

// connect() to an unreachable host can block for over a minute; give up after sec seconds
bool connect_with_timeout(int sock,const sockaddr_in &sa,int sec){
    int flags=fcntl(sock,F_GETFL,0);
    fcntl(sock,F_SETFL,flags|O_NONBLOCK);
    int r=connect(sock,(const sockaddr*)&sa,sizeof(sa));
    if(r<0&&errno==EINPROGRESS){
        pollfd pfd{sock,POLLOUT,0};
        r=-1;
        if(poll(&pfd,1,sec*1000)==1){
            int err=0;
            socklen_t el=sizeof(err);
            getsockopt(sock,SOL_SOCKET,SO_ERROR,&err,&el);
            if(err==0)
                r=0;
        }
    }
    fcntl(sock,F_SETFL,flags);
    return r==0;
}

// send len bytes of file fd starting at off straight from the page cache to the socket,
// without copying them through this process
bool send_file_range(int sock,int fd,off_t off,size_t len){
#if defined(__APPLE__)
    while(len>0){
        off_t n=len;
        int r=sendfile(fd,sock,off,&n,nullptr,0);
        off+=n;
        len-=n;
        if(r<0&&n==0&&errno!=EINTR)
            return false;
    }
    return true;
#elif defined(__linux__)
    while(len>0){
        ssize_t n=sendfile(sock,fd,&off,len);
        if(n<0&&errno==EINTR)
            continue;
        if(n<=0)
            return false;
        len-=n;
    }
    return true;
#else
    vector<char> buf(len);
    return pread(fd,buf.data(),len,off)==(ssize_t)len&&send_all(sock,buf.data(),len);
#endif
}

string hex_digest(const unsigned char *hash){
    char buf[SHA_DIGEST_LENGTH*2+1];
    for(int i=0;i<SHA_DIGEST_LENGTH;i++){
        snprintf(buf+i*2,3,"%02x",hash[i]);
    }
    return string(buf,SHA_DIGEST_LENGTH*2);
}

// absolute path with symlinks resolved, so two spellings of one file compare equal
string real_path(const string &p){
    char buf[PATH_MAX];
    return realpath(p.c_str(),buf)?string(buf):p;
}

string sha1_str(const char *data, size_t len){
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1((const unsigned char*)data,len,hash);
    return hex_digest(hash);
}

string sha1_file(const string &path){
    int fd=open(path.c_str(),O_RDONLY);
    if(fd<0)
        return "";

    SHA_CTX ctx;
    SHA1_Init(&ctx);
    vector<char> buf(PIECE_SIZE); // heap: macOS threads only get a 512KB stack
    while(true){
        ssize_t n=read(fd,buf.data(),buf.size());
        if(n<=0)
            break;
        SHA1_Update(&ctx,buf.data(),n);
    }
    close(fd);

    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1_Final(hash,&ctx);
    return hex_digest(hash);
}

bool read_file(const string &path,string &data){
    int fd=open(path.c_str(),O_RDONLY);
    if(fd<0)
        return false;
    data.clear();
    char buf[65536];
    ssize_t n;
    while((n=read(fd,buf,sizeof(buf)))>0){
        data.append(buf,n);
    }
    close(fd);
    return true;
}

// replace path with data in one step (write a temporary file, then rename it over)
bool write_file_atomic(const string &path,const string &data){
    string tmp=path+".tmp";
    int fd=open(tmp.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0600);
    if(fd<0)
        return false;
    bool ok=true;
    size_t off=0;
    while(ok&&off<data.size()){
        ssize_t n=write(fd,data.data()+off,data.size()-off);
        if(n<0&&errno==EINTR)
            continue;
        ok=n>0;
        if(ok)
            off+=n;
    }
    close(fd);
    return ok&&rename(tmp.c_str(),path.c_str())==0;
}

//tracker connection

static int connect_tracker(const pair<string,string> &tr) {
    if(tr.first.empty() || tr.second.empty()){
        return -1;
    }
    if(con){
        cout<<"checking trackers availability...\nTrying to connect tracker at"<<tr.first<<":"<<tr.second<<"\n";
    }
    int sock=socket(AF_INET,SOCK_STREAM,0);
    if(sock<0){
        return -1;
    }
    // a hung tracker then counts as down, and send_recv fails over to the other one
    set_timeout(sock,10);
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons((uint16_t)stoi(tr.second));

    if(inet_pton(AF_INET,tr.first.c_str(),&sa.sin_addr)<=0){
        close(sock);
        return -1;
    }
    if(!connect_with_timeout(sock,sa,3)){
        close(sock);
        return -1;
    }
    if(con){
        cout<<"Connected to tracker "<<tr.first<<":"<<tr.second<<"\n";
    }
    return sock;
}

// connect to the preferred tracker, or the other one if it is down
static int connect_backup(){
    int first=preferred;
    for(int i=0;i<2;i++){
        int t=(first+i)%2;
        int s=connect_tracker(t==0?tracker1:tracker2);
        if(s>=0){
            preferred=t;
            lock_guard<mutex> lk(mtrk);
            sockTracker[s]=t;
            return s;
        }
    }
    return -1;
}

// a call on sock failed: close it, and try the other tracker first from now on
static void drop_tracker(int &sock){
    {
        lock_guard<mutex> lk(mtrk);
        auto it=sockTracker.find(sock);
        if(it!=sockTracker.end()){
            preferred=1-it->second;
            sockTracker.erase(it);
        }
    }
    close(sock);
    sock=-1;
}

static void close_tracker(int &sock){
    if(sock<0)
        return;
    {
        lock_guard<mutex> lk(mtrk);
        sockTracker.erase(sock);
    }
    close(sock);
    sock=-1;
}

// one request/reply with the tracker; if it fails, retry once on a fresh connection,
// starting with the other tracker
static bool send_recv(int &sock,const string &req,Msg &r){
    for(int attempt=0;attempt<2;attempt++){
        if(sock<0){
            sock=connect_backup();
        }
        if(sock<0){
            return false;
        }
        if(send_msg(sock,MSG_REQ,0,req)&&recv_msg(sock,r)&&r.type==MSG_REPLY){
            return true;
        }
        drop_tracker(sock);
    }
    return false;
}

bool logged_in(){
    lock_guard<mutex> lk(mauth);
    return gl;
}

// the trackers no longer accept our session token: the user has to log in again
static void session_expired(){
    lock_guard<mutex> lk(mauth);
    if(gl){
        gl=false;
        gtoken.clear();
        sharing=false;
        cout<<"\nSession expired, please login again\n$> "<<flush;
    }
}

// send_recv, and if this connection has no session on the tracker (e.g. after a failover)
// continue the session with its token (the password is never sent again) and retry
static bool autologin(int &sock,const string &payload,int &code,string &text){
    Msg r;
    if(!send_recv(sock,payload,r)){
        return false;
    }
    code=r.status;
    text=r.payload;
    if(code!=ST_NOT_LOGGED_IN){
        return true;
    }
    string u,t;
    {
        lock_guard<mutex> lk(mauth);
        if(!gl){
            return true;
        }
        u=gu;
        t=gtoken;
    }
    if(!send_recv(sock,"resume "+u+" "+t+" "+p_sip+" "+p_sp,r)){
        return false;
    }
    if(r.status!=ST_OK){
        session_expired();
        code=r.status;
        text=r.payload;
        return true;
    }
    if(!send_recv(sock,payload,r)){
        return false;
    }
    code=r.status;
    text=r.payload;
    return true;
}

// a request on the shared tracker connection
static bool tracker_cmd(const string &payload,int &code,string &text){
    lock_guard<mutex> lk(msock);
    return autologin(tsock,payload,code,text);
}

// every 5s: tells the tracker this client is still alive (it drops sessions that go
// silent), and notices a dead tracker even while the user types nothing, failing over
// to the other one and continuing the session there
void heartbeat(){
    while(true){
        sleep(5);
        int code;
        string text;
        tracker_cmd("heartbeat",code,text);
    }
}

//persisted client state (p2p_client_<port>.state): one line per shared file or download
//  share <user> <gid> <fname> <size> <fhash> <n> <hash_1..hash_n> <path>
//  download <user> <gid> <fname> <dest>

void save_state_locked(){
    string s;
    for(auto &kv:persisted){
        s+=kv.second+"\n";
    }
    if(!write_file_atomic(statePath,s)){
        perror("client state file");
    }
}

void load_state(){
    string data;
    if(!read_file(statePath,data))
        return;
    stringstream ss(data);
    string line;
    lock_guard<mutex> lk(mstatef);
    while(getline(ss,line)){
        auto t=splits(line);
        if(t.size()>=5&&(t[0]=="share"||t[0]=="download")){
            persisted[t[0]+" "+t[1]+" "+t[2]+" "+t[3]]=line;
        }
    }
}

void remember(const string &key,const string &line){
    lock_guard<mutex> lk(mstatef);
    persisted[key]=line;
    save_state_locked();
}

void forget(const string &key){
    lock_guard<mutex> lk(mstatef);
    if(persisted.erase(key))
        save_state_locked();
}

void remember_share(const string &user,const string &gid,const string &fname,const ShareInfo &s){
    string line="share "+user+" "+gid+" "+fname+" "+to_string(s.size)+" "+s.fhash+" "+to_string(s.ph.size());
    for(auto &h:s.ph)
        line+=" "+h;
    line+=" "+real_path(s.path);
    remember("share "+user+" "+gid+" "+fname,line);
}

//peer server

size_t piece_len(size_t size,int i){
    size_t off=(size_t)i*PIECE_SIZE;
    return min((size_t)PIECE_SIZE,size-off);
}

// one connection from a downloading peer. It may send many requests, several at a time:
//   GETPIECE <gid> <fname> <index>  -> MSG_PIECE with the piece bytes, or an error reply
//   BITFIELD <gid> <fname>          -> '1'/'0' per piece: which pieces this client can serve
void peer_handler(int csock){
    set_timeout(csock,30);    // a downloader that goes quiet for 30s is gone
    set_nodelay(csock);
    Msg m;
    while(recv_msg(csock,m,4096)){
        auto sp=splits(m.payload);
        if(m.type!=MSG_REQ||sp.size()<3||(sp[0]!="GETPIECE"&&sp[0]!="BITFIELD")){
            send_msg(csock,MSG_REPLY,ST_BAD_REQUEST,"bad request");
            break;
        }
        if(!sharing){
            if(!send_msg(csock,MSG_REPLY,ST_UNAVAILABLE,"not sharing while logged out"))
                break;
            continue;
        }
        ShareInfo info;
        bool found=false;
        {
            // hold the lock only for the lookup, so several pieces can be sent in parallel
            lock_guard<mutex> lk(mf);
            auto it=sharedFiles.find(sp[1]+":"+sp[2]);
            if(it!=sharedFiles.end()){
                info=it->second;
                found=true;
            }
        }
        if(!found){
            if(!send_msg(csock,MSG_REPLY,ST_NOT_FOUND,"file not shared here"))
                break;
            continue;
        }
        int n=(info.size+PIECE_SIZE-1)/PIECE_SIZE;
        if(sp[0]=="BITFIELD"){
            string bits(n,'1');
            if(info.dl){
                lock_guard<mutex> lk(info.dl->m);
                for(int i=0;i<n;i++)
                    bits[i]=info.dl->have[i]?'1':'0';
            }
            if(!send_msg(csock,MSG_REPLY,ST_OK,bits))
                break;
            continue;
        }
        long long index;
        if(sp.size()!=4||!parse_num(sp[3],index)||index<0){
            send_msg(csock,MSG_REPLY,ST_BAD_REQUEST,"bad request");
            break;
        }
        bool avail=index<n;
        if(avail&&info.dl){
            lock_guard<mutex> lk(info.dl->m);
            avail=info.dl->have[index];
        }
        int fd=avail?open(info.path.c_str(),O_RDONLY):-1;
        if(fd<0){
            if(!send_msg(csock,MSG_REPLY,ST_NOT_FOUND,avail?"file not readable":"no such piece"))
                break;
            continue;
        }
        if(test_delay_ms>0)
            usleep(test_delay_ms*1000);
        size_t len=piece_len(info.size,(int)index);
        string h=header(MSG_PIECE,ST_OK,len);
        bool ok=send_all(csock,h.data(),h.size())&&send_file_range(csock,fd,(off_t)index*PIECE_SIZE,len);
        close(fd);
        if(!ok)
            break;
    }
    close(csock);
}

// bind before the shell starts, so a busy or invalid address is reported right away
int open_peer_server(){
    int sock=socket(AF_INET,SOCK_STREAM,0);
    if(sock<0){
        perror("peer socket");
        return -1;
    }
    int opt=1;
    setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons(stoi(p_sp));
    if(inet_pton(AF_INET,p_sip.c_str(),&sa.sin_addr)<=0){
        cerr<<"invalid IP address "<<p_sip<<"\n";
        close(sock);
        return -1;
    }
    if(::bind(sock,(sockaddr*)&sa,sizeof(sa))<0){
        perror("peer bind");
        close(sock);
        return -1;
    }
    if(listen(sock,100)<0){
        perror("peer listen");
        close(sock);
        return -1;
    }
    return sock;
}

void peer_server(int sock){
    while(true){
        sockaddr_in cli;
        socklen_t cl=sizeof(cli);
        int cs=accept(sock,(sockaddr*)&cli,&cl);
        if(cs<0)
            continue;
        thread(peer_handler,cs).detach();
    }
}

//downloads

struct PeerAddr{
    string uid,ip,port;
    bool complete;
};

// parses the tracker's download_file reply:
// <size> <filehash> <pieces> <piecehash...> PEERS <uid> <ip> <port> <C|P> ...
static bool parse_file_reply(const string &text,size_t &size,string &fh,vector<string> &ph,vector<PeerAddr> &peers){
    auto sp=splits(text);
    long long sz,pieces;
    if(sp.size()<4||!parse_num(sp[0],sz)||!parse_num(sp[2],pieces)||sz<0||pieces!=(sz+PIECE_SIZE-1)/PIECE_SIZE||(long long)sp.size()<4+pieces||sp[3+pieces]!="PEERS"){
        return false;
    }
    size=sz;
    fh=sp[1];
    ph.assign(sp.begin()+3,sp.begin()+3+pieces);
    peers.clear();
    for(size_t i=4+pieces;i+3<sp.size();i+=4){
        // never download from ourselves
        if(!valid_port(sp[i+2])||(sp[i+1]==p_sip&&sp[i+2]==p_sp))
            continue;
        peers.push_back({sp[i],sp[i+1],sp[i+2],sp[i+3]=="C"});
    }
    return true;
}

// how many usable peers have each piece (d.m held)
void recount(DownloadState &d){
    fill(d.avail.begin(),d.avail.end(),0);
    for(auto &q:d.peers){
        if(q.second.banned)
            continue;
        for(int i=0;i<d.n;i++){
            if(q.second.complete||q.second.bits[i])
                d.avail[i]++;
        }
    }
}

// add peers the tracker reported, and mark peers that now have the whole file (d.m held)
void merge_peers(DownloadState &d,const vector<PeerAddr> &peers){
    for(auto &a:peers){
        string key=a.ip+":"+a.port;
        auto it=d.peers.find(key);
        if(it==d.peers.end()){
            PeerInfo p;
            p.uid=a.uid;
            p.ip=a.ip;
            p.port=a.port;
            p.complete=a.complete;
            p.bits.assign(d.n,0);
            d.peers[key]=p;
        }
        else if(a.complete){
            it->second.complete=true;
        }
    }
    recount(d);
}

// the next piece to ask peer pk for: one it has and we lack, with the fewest copies among
// our peers (rarest first, random among equals). Once every missing piece is already being
// fetched ("endgame"), a piece may also be asked from a second peer, so one slow peer can't
// hold up the end of the download (d.m held)
int pick_piece(DownloadState &d,const string &pk,const set<int> &mine){
    static thread_local mt19937 rng(random_device{}());
    PeerInfo &p=d.peers[pk];
    bool endgame=true;
    for(int i=0;i<d.n;i++){
        if(!d.have[i]&&d.inflight[i]==0){
            endgame=false;
            break;
        }
    }
    int best=-1,bestavail=INT_MAX,ties=0;
    for(int i=0;i<d.n;i++){
        if(d.have[i]||mine.count(i)||!(p.complete||p.bits[i]))
            continue;
        if(d.inflight[i]>0&&!endgame)
            continue;
        if(d.avail[i]<bestavail){
            best=i;
            bestavail=d.avail[i];
            ties=1;
        }
        else if(d.avail[i]==bestavail&&rng()%(++ties)==0){
            best=i;
        }
    }
    return best;
}

int open_peer(const string &ip,const string &port){
    int s=socket(AF_INET,SOCK_STREAM,0);
    if(s<0)
        return -1;
    // a stalled peer must not hang a worker forever
    set_timeout(s,10);
    set_nodelay(s);
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons(stoi(port));
    if(inet_pton(AF_INET,ip.c_str(),&sa.sin_addr)<=0||!connect_with_timeout(s,sa,3)){
        close(s);
        return -1;
    }
    return s;
}

// which pieces a peer that is still downloading can serve
bool fetch_bitfield(DownloadState &d,const string &pk,int s){
    Msg r;
    if(!send_msg(s,MSG_REQ,0,"BITFIELD "+d.gid+" "+d.fname)||!recv_msg(s,r,d.n+64)){
        return false;
    }
    if(r.status!=ST_OK||(int)r.payload.size()!=d.n){
        return false;
    }
    lock_guard<mutex> lk(d.m);
    auto &p=d.peers[pk];
    int count=0;
    for(int i=0;i<d.n;i++){
        p.bits[i]=r.payload[i]=='1';
        count+=p.bits[i];
    }
    if(count==d.n)
        p.complete=true;
    recount(d);
    return true;
}

void set_status(DownloadState &d,const string &status){
    lock_guard<mutex> lk(mdown);
    downloads[d.gid+":"+d.fname]=status;
}

// <dest>.part remembers which pieces are done, so the download can resume after a restart
void save_part(DownloadState &d){
    string bits;
    {
        lock_guard<mutex> lk(d.m);
        for(char h:d.have)
            bits+=h?'1':'0';
    }
    lock_guard<mutex> lk(d.savem);
    write_file_atomic(d.dest+".part","P2PPART "+d.fhash+" "+to_string(d.size)+"\n"+bits+"\n");
}

// pieces marked done in <dest>.part are hashed again before they are trusted
void load_part(DownloadState &d){
    string data;
    if(!read_file(d.dest+".part",data))
        return;
    stringstream ss(data);
    string head,bits;
    getline(ss,head);
    getline(ss,bits);
    auto h=splits(head);
    if(h.size()!=3||h[0]!="P2PPART"||h[1]!=d.fhash||h[2]!=to_string(d.size)||(int)bits.size()!=d.n)
        return;
    vector<char> buf(PIECE_SIZE);
    int found=0;
    for(int i=0;i<d.n;i++){
        if(bits[i]!='1')
            continue;
        size_t len=piece_len(d.size,i);
        if(pread(d.fd,buf.data(),len,(off_t)i*PIECE_SIZE)==(ssize_t)len&&sha1_str(buf.data(),len)==d.ph[i]){
            lock_guard<mutex> lk(d.m);
            d.have[i]=1;
            d.done++;
            found++;
        }
    }
    if(found>0)
        logmsg("Resuming download of "+d.fname+": "+to_string(found)+"/"+to_string(d.n)+" pieces already here");
}

// fetches pieces from one peer over one connection, keeping up to PIPELINE requests in
// flight, so the peer never waits for our next request
void peer_worker(shared_ptr<DownloadState> d,string pk){
    string ip,port;
    bool complete;
    {
        lock_guard<mutex> lk(d->m);
        auto &p=d->peers[pk];
        ip=p.ip;
        port=p.port;
        complete=p.complete;
    }
    string from=ip+":"+port;
    int s=open_peer(ip,port);
    bool ok=s>=0;
    bool idle=false;
    {
        lock_guard<mutex> lk(d->m);
        d->peers[pk].sock=s;
    }
    if(ok&&!complete)
        ok=fetch_bitfield(*d,pk,s);
    deque<int> out;     // requested on this connection, oldest first
    set<int> outset;
    while(ok){
        vector<int> fresh;
        {
            lock_guard<mutex> lk(d->m);
            if(d->finished)
                break;
            while((int)out.size()<PIPELINE){
                int i=pick_piece(*d,pk,outset);
                if(i<0)
                    break;
                d->inflight[i]++;
                out.push_back(i);
                outset.insert(i);
                fresh.push_back(i);
            }
            complete=d->peers[pk].complete;
        }
        for(int i:fresh){
            if(!send_msg(s,MSG_REQ,0,"GETPIECE "+d->gid+" "+d->fname+" "+to_string(i))){
                ok=false;
                break;
            }
        }
        if(!ok)
            break;
        if(out.empty()){
            // nothing this peer can give us now. A peer that is still downloading may have
            // more pieces soon: look again in a second
            if(complete){
                idle=true;
                break;
            }
            {
                unique_lock<mutex> lk(d->m);
                d->cv.wait_for(lk,chrono::seconds(1),[&]{ return d->finished; });
                if(d->finished)
                    break;
            }
            ok=fetch_bitfield(*d,pk,s);
            continue;
        }
        Msg r;
        if(!recv_msg(s,r,PIECE_SIZE+64)){
            logmsg("Peer "+from+" failed to send full piece "+to_string(out.front()));
            ok=false;
            break;
        }
        int i=out.front();
        out.pop_front();
        outset.erase(i);
        size_t len=piece_len(d->size,i);
        if(r.type!=MSG_PIECE||r.status!=ST_OK){
            logmsg("Peer "+from+" refused piece "+to_string(i)+": "+r.payload);
            lock_guard<mutex> lk(d->m);
            d->inflight[i]--;
            auto &p=d->peers[pk];
            if(r.status==ST_NOT_FOUND&&!p.complete&&p.bits[i]){
                p.bits[i]=0;
                d->avail[i]--;
            }
            // a peer that stopped sharing (logged out, or lost the file) is no use now
            if(r.status==ST_UNAVAILABLE||(r.status==ST_NOT_FOUND&&p.complete))
                ok=false;
            continue;
        }
        if(r.payload.size()!=len||sha1_str(r.payload.data(),len)!=d->ph[i]){
            // a corrupted (or wrong) piece: never use this peer again for this download
            logmsg("Hash mismatch for piece "+to_string(i)+" from "+from+"; no longer using this peer");
            lock_guard<mutex> lk(d->m);
            d->inflight[i]--;
            d->peers[pk].banned=true;
            recount(*d);
            ok=false;
            break;
        }
        bool wrote=pwrite(d->fd,r.payload.data(),len,(off_t)i*PIECE_SIZE)==(ssize_t)len;
        bool savepart=false;
        int done=0;
        {
            lock_guard<mutex> lk(d->m);
            d->inflight[i]--;
            if(wrote&&!d->have[i]){
                d->have[i]=1;
                d->done++;
                d->peers[pk].pieces++;
                savepart=(++d->since_save>=16);
                if(savepart)
                    d->since_save=0;
                if(d->done==d->n)
                    d->finished=true;
            }
            d->peers[pk].fails=0;
            done=d->done;
        }
        d->cv.notify_all();
        if(!wrote){
            logmsg("Write error for piece "+to_string(i));
            continue;
        }
        logmsg("Piece "+to_string(i)+" downloaded from "+from);
        set_status(*d,"[D] "+d->gid+" "+d->fname+" "+to_string(done)+"/"+to_string(d->n));
        if(savepart)
            save_part(*d);
    }
    {
        lock_guard<mutex> lk(d->m);
        // give back requests this connection won't answer now
        for(int i:out)
            d->inflight[i]--;
        auto &p=d->peers[pk];
        p.running=false;
        p.sock=-1;
        if(idle){
            p.retry_after=time(nullptr)+2;
        }
        else if(!ok&&!p.banned){
            // back off: 2, 4, 8 .. 30 seconds before trying this peer again
            p.fails++;
            p.retry_after=time(nullptr)+min(1<<min(p.fails,5),30);
        }
    }
    d->cv.notify_all();
    if(s>=0)
        close(s);
}

// ask the tracker who has the file now: new peers, and peers that finished their copy
void refresh_peers(DownloadState &d){
    int code;
    string text;
    if(!tracker_cmd("download_file "+d.gid+" "+d.fname,code,text)||code!=ST_OK)
        return;
    size_t size;
    string fh;
    vector<string> ph;
    vector<PeerAddr> peers;
    if(!parse_file_reply(text,size,fh,ph,peers)||fh!=d.fhash)
        return;
    lock_guard<mutex> lk(d.m);
    merge_peers(d,peers);
}

// tells the tracker this client has the whole file; true if it was accepted
bool announce_share(const string &gid,const string &fname,const ShareInfo &s,string &text){
    string user;
    {
        lock_guard<mutex> lk(mauth);
        user=gu;
    }
    string cmd="upload_file "+gid+" "+fname+" "+to_string(s.size)+" "+s.fhash+" "+to_string(s.ph.size())+" ";
    for(auto &h:s.ph)
        cmd+=h+" ";
    cmd+=user+" "+p_sip+" "+p_sp;
    int code;
    if(!tracker_cmd(cmd,code,text)){
        text="tracker down";
        return false;
    }
    return code==ST_OK;
}

void finish_download(shared_ptr<DownloadState> d){
    string key=d->gid+":"+d->fname;
    string dkey="download "+d->user+" "+d->gid+" "+d->fname;
    close(d->fd);
    if(d->done<d->n){
        // paused, not failed: the pieces we have stay shared, and it resumes on the next
        // login or with download_file
        save_part(*d);
        logmsg("Giving up on "+d->fname+" for now: "+to_string(d->done)+"/"+to_string(d->n)+" pieces");
        set_status(*d,"[E] "+d->gid+" "+d->fname+" paused at "+to_string(d->done)+"/"+to_string(d->n)+" pieces, no peer has the rest (resumes on next login or download_file)");
    }
    else if(sha1_file(d->dest)==d->fhash){
        logmsg("File download completed and verified successfully: " + d->fname);
        // printed from a background thread: flush, or it waits for the next prompt
        cout<<"File downloaded successfully"<<endl;
        ShareInfo s{d->dest,d->size,d->fhash,d->ph,nullptr};
        {
            lock_guard<mutex> lk(mf);
            sharedFiles[key]=s;
        }
        unlink((d->dest+".part").c_str());
        forget(dkey);
        remember_share(d->user,d->gid,d->fname,s);
        set_status(*d,"[C] "+d->gid+" "+d->fname);
        string text;
        if(announce_share(d->gid,d->fname,s,text)){
            logmsg("Informed tracker: now client is also seeder for file " + d->fname);
        }
        else{
            logmsg("[WARNING] Could not notify tracker that client is now a seeder: "+text);
        }
    }
    else{
        logmsg("Final file hash mismatch for " + d->fname);
        unlink((d->dest+".part").c_str());
        forget(dkey);
        {
            lock_guard<mutex> lk(mf);
            sharedFiles.erase(key);
        }
        set_status(*d,"[E] "+d->gid+" "+d->fname+" final hash mismatch");
    }
    lock_guard<mutex> lk(mdown);
    active.erase(key);
}

// runs one download: a worker per peer, a fresh peer list from the tracker every 5s, and
// a pause if no new piece arrives for GIVE_UP_AFTER seconds
void run_download(shared_ptr<DownloadState> d){
    load_part(*d);
    set_status(*d,"[D] "+d->gid+" "+d->fname+" "+to_string(d->done)+"/"+to_string(d->n));
    save_part(*d);
    vector<thread> workers;
    time_t lastrefresh=time(nullptr);
    time_t lastprogress=time(nullptr);
    int lastdone=-1;
    while(true){
        {
            unique_lock<mutex> lk(d->m);
            if(d->done==d->n)
                break;
            if(d->done!=lastdone){
                lastdone=d->done;
                lastprogress=time(nullptr);
            }
            time_t now=time(nullptr);
            for(auto &kv:d->peers){
                auto &p=kv.second;
                if(!p.running&&!p.banned&&now>=p.retry_after){
                    p.running=true;
                    workers.emplace_back(peer_worker,d,kv.first);
                }
            }
            d->cv.wait_for(lk,chrono::seconds(1));
            if(d->done==d->n)
                break;
        }
        time_t now=time(nullptr);
        // new peers (and peers that finished their copy) appear over time
        if(now-lastrefresh>=5){
            lastrefresh=now;
            refresh_peers(*d);
        }
        if(now-lastprogress>=GIVE_UP_AFTER)
            break;
    }
    {
        lock_guard<mutex> lk(d->m);
        d->finished=true;
        // workers still waiting on a slow or frozen peer stop at once instead of timing out
        for(auto &kv:d->peers){
            if(kv.second.sock>=0)
                shutdown(kv.second.sock,SHUT_RDWR);
        }
    }
    d->cv.notify_all();
    for(auto &t:workers){
        t.join();
    }
    {
        lock_guard<mutex> lk(d->m);
        string from;
        for(auto &kv:d->peers){
            if(kv.second.pieces>0)
                from+=" "+kv.first+"="+to_string(kv.second.pieces);
        }
        logmsg("Pieces of "+d->fname+" by peer:"+from);
    }
    finish_download(d);
}

// begins (or resumes, using <dest>.part) a download in the background
bool start_download(const string &gid,const string &fname,const string &dest,bool verbose,ostream &out){
    string key=gid+":"+fname;
    {
        lock_guard<mutex> lk(mdown);
        if(active.count(key)){
            if(verbose)
                out<<"This file is already being downloaded\n";
            return false;
        }
    }
    {
        // never overwrite a file this client is sharing: peers may be reading it right now
        string target=real_path(dest);
        lock_guard<mutex> lk(mf);
        for(auto &s:sharedFiles){
            // (except this same download resuming into its own unfinished file)
            bool resuming=s.first==key&&s.second.dl;
            if(!resuming&&real_path(s.second.path)==target){
                if(verbose)
                    out<<"Destination is a file you are sharing, choose another path\n";
                return false;
            }
        }
    }

    int code;
    string text;
    if(!tracker_cmd("download_file "+gid+" "+fname,code,text)){
        out<<"tracker down\n";
        return false;
    }
    if(code!=ST_OK){
        out<<"Tracker: "<<text<<"\n";
        return false;
    }
    auto d=make_shared<DownloadState>();
    vector<PeerAddr> peers;
    if(!parse_file_reply(text,d->size,d->fhash,d->ph,peers)){
        out<<"Malformed reply from tracker\n";
        return false;
    }
    if(peers.empty()){
        out<<"No peers have "<<fname<<" right now\n";
        return false;
    }
    d->gid=gid;
    d->fname=fname;
    d->dest=dest;
    d->n=d->ph.size();
    d->have.assign(d->n,0);
    d->inflight.assign(d->n,0);
    d->avail.assign(d->n,0);
    {
        lock_guard<mutex> lk(mauth);
        d->user=gu;
    }
    merge_peers(*d,peers);

    d->fd=open(dest.c_str(),O_CREAT|O_RDWR,0666);
    if(d->fd<0){
        out<<"cannot open destination: "<<strerror(errno)<<"\n";
        return false;
    }
    if(ftruncate(d->fd,(off_t)d->size)<0){
        out<<"cannot size destination: "<<strerror(errno)<<"\n";
        close(d->fd);
        return false;
    }

    {
        lock_guard<mutex> lk(mdown);
        active[key]=d;
        downloads[key]="[D] "+gid+" "+fname+" 0/"+to_string(d->n);
    }
    {
        // serve the pieces we get to other peers right away (partial seeding)
        lock_guard<mutex> lk(mf);
        sharedFiles[key]=ShareInfo{dest,d->size,d->fhash,d->ph,d};
    }
    remember("download "+d->user+" "+gid+" "+fname,"download "+d->user+" "+gid+" "+fname+" "+real_path(dest));
    // the tracker lists us as a partial peer for this file from now on
    tracker_cmd("start_download "+gid+" "+fname,code,text);

    logmsg("Download started for file " + fname + " (" + to_string(d->n) + " pieces)");
    thread(run_download,d).detach();
    out<<(verbose?"Download started in background for ":"Resuming download of ")<<fname<<"\n";
    return true;
}

//commands

void create_user_cmd(const vector<string> &inp,ostream &out) {
    int code;
    string text;
    if(!tracker_cmd(inp[0]+" "+inp[1]+" "+inp[2],code,text)){
        out<<"tracker not reachable\n";
        return;
    }
    out<<"Tracker: "<<text<<"\n";
}

// after logging in: share this user's files again and resume their unfinished downloads
void restore_user_state(const string &user,ostream &out){
    vector<string> shares,dls;
    {
        lock_guard<mutex> lk(mstatef);
        for(auto &kv:persisted){
            auto t=splits(kv.second);
            if(t.size()>=5&&t[1]==user)
                (t[0]=="share"?shares:dls).push_back(kv.second);
        }
    }
    int shared=0,resumed=0;
    for(auto &line:shares){
        //share <user> <gid> <fname> <size> <fhash> <n> <hashes..> <path>
        auto t=splits(line);
        long long size,n;
        string key="share "+user+" "+t[2]+" "+t[3];
        if(t.size()<8||!parse_num(t[4],size)||!parse_num(t[6],n)||(long long)t.size()!=8+n){
            forget(key);
            continue;
        }
        ShareInfo s{t.back(),(size_t)size,t[5],vector<string>(t.begin()+7,t.begin()+7+n),nullptr};
        struct stat st;
        if(stat(s.path.c_str(),&st)<0||(long long)st.st_size!=size){
            // the file is gone or changed: stop offering it
            forget(key);
            continue;
        }
        {
            lock_guard<mutex> lk(mf);
            if(sharedFiles.count(t[2]+":"+t[3]))
                continue;
        }
        string text;
        if(announce_share(t[2],t[3],s,text)){
            lock_guard<mutex> lk(mf);
            sharedFiles[t[2]+":"+t[3]]=s;
            shared++;
        }
        else if(text!="tracker down"){
            forget(key);
        }
    }
    for(auto &line:dls){
        auto t=splits(line);
        if(start_download(t[2],t[3],t[4],false,out))
            resumed++;
    }
    if(shared>0||resumed>0)
        out<<"Re-shared "<<shared<<" files, resumed "<<resumed<<" downloads\n";
}

bool login_cmd(const vector<string> &in,ostream &out){
    int code;
    string text;
    if(!tracker_cmd(in[0]+" "+in[1]+" "+in[2]+" "+p_sip+" "+p_sp,code,text)){
        out<<"tracker not reachable\n";
        return false;
    }
    if(code!=ST_OK){
        out<<"Tracker: "<<text<<"\n";
        return false;
    }
    // reply: <token> <message>
    size_t sp=text.find(' ');
    string token=text.substr(0,sp);
    out<<"Tracker: "<<(sp==string::npos?"":text.substr(sp+1))<<"\n";
    {
        lock_guard<mutex> lk(mauth);
        // files a different user shared from this client must not be served for the new one
        if(lastUser!=in[1]){
            lock_guard<mutex> lk2(mf);
            sharedFiles.clear();
        }
        lastUser=in[1];
        gl=true;
        gu=in[1];
        gtoken=token;
        sharing=true;
    }
    restore_user_state(in[1],out);
    return true;
}

bool logout_cmd(const vector<string> &in,ostream &out){
    int code;
    string text;
    if(!tracker_cmd(in[0],code,text)){
        out<<"tracker not reachable\n";
        return false;
    }
    out<<"Tracker: "<<text<<"\n";
    return code==ST_OK;
}

void upload_file_cmd(const vector<string> &in,ostream &out){
    string gid=in[1];
    string path=in[2];
    int fd=open(path.c_str(),O_RDONLY);
    if(fd<0){
        out<<"cannot open file: "<<strerror(errno)<<"\n";
        return;
    }

    struct stat st;
    if(fstat(fd,&st)<0||!S_ISREG(st.st_mode)){
        out<<"not a regular file\n";
        close(fd);
        return;
    }
    ShareInfo s;
    s.path=path;
    s.size=st.st_size;
    int pieces=(s.size+PIECE_SIZE-1)/PIECE_SIZE;

    // one pass over the file gives both the piece hashes and the whole-file hash
    SHA_CTX ctx;
    SHA1_Init(&ctx);
    vector<char> buf(PIECE_SIZE);
    for(int i=0;i<pieces;i++){
        ssize_t n=read(fd,buf.data(),PIECE_SIZE);
        if(n<=0)
            break;
        s.ph.push_back(sha1_str(buf.data(),n));
        SHA1_Update(&ctx,buf.data(),n);
    }
    close(fd);
    if((int)s.ph.size()!=pieces){
        out<<"error reading file\n";
        return;
    }
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1_Final(hash,&ctx);
    s.fhash=hex_digest(hash);
    string fname = path.substr(path.find_last_of("/\\") == string::npos ? 0 : path.find_last_of("/\\") + 1);

    string text;
    bool ok=announce_share(gid,fname,s,text);
    out<<"Tracker: "<<text<<"\n";
    // only serve the file if the tracker actually accepted it
    if(!ok){
        return;
    }
    {
        lock_guard<mutex> lk(mf);
        sharedFiles[gid+":"+fname]=s;
    }
    string user;
    {
        lock_guard<mutex> lk(mauth);
        user=gu;
    }
    remember_share(user,gid,fname,s);
}

void download_file_cmd(const vector<string> &in,ostream &out){
    string dest=in[3];
    // a directory as destination means "save it in there under the same name"
    struct stat ds;
    if(stat(dest.c_str(),&ds)==0&&S_ISDIR(ds.st_mode)){
        dest+="/"+in[2];
    }
    start_download(in[1],in[2],dest,true,out);
}

void show_downloads_cmd(ostream &out){
    lock_guard<mutex> lk(mdown);
    if(downloads.empty()){
        out<<"No files\n";
        return;
    }
    for(auto &d:downloads)
        out<<d.second<<"\n";
}

void stop_share_cmd(const vector<string> &in,ostream &out){
    int code;
    string text;
    if(!tracker_cmd(in[0]+" "+in[1]+" "+in[2],code,text)){
        out<<"tracker down\n";
        return;
    }
    out<<"Tracker: "<<text<<"\n";
    string user;
    {
        lock_guard<mutex> lk(mauth);
        user=gu;
    }
    {
        lock_guard<mutex> lk(mf);
        sharedFiles.erase(in[1]+":"+in[2]);
    }
    forget("share "+user+" "+in[1]+" "+in[2]);
}

// leaving a group also means no longer serving (or remembering) its files
void forget_group(const string &gid){
    string user;
    {
        lock_guard<mutex> lk(mauth);
        user=gu;
    }
    string prefix=gid+":";
    vector<string> names;
    {
        lock_guard<mutex> lk(mf);
        for(auto it=sharedFiles.begin();it!=sharedFiles.end();){
            if(it->first.compare(0,prefix.size(),prefix)==0){
                names.push_back(it->first.substr(prefix.size()));
                it=sharedFiles.erase(it);
            }
            else{
                ++it;
            }
        }
    }
    for(auto &f:names){
        forget("share "+user+" "+gid+" "+f);
    }
}

// runs one shell command; what it prints goes to out (the terminal, or the web page)
void run_command(const string &line,ostream &out){
    auto in=splits(line);
    if(in.empty()){
        return;
    }
    string cmd=in[0];
    bool userLogin=logged_in();
    if(cmd=="create_user"){
        if(userLogin){
            out<<"Logout to create another user\n";
            return;
        }
        if(in.size()!=3){
            out<<"Use this: create_user <user> <pass>\n";
            return;
        }
        create_user_cmd(in,out);
        return;
    }
    if(cmd!="login" && !userLogin) {
        out<<"User not logged in\n";
        return;
    }
    if(cmd=="login"){
        if(userLogin){
            out<<"user already logged in\n";
            return;
        }
        if(in.size()!=3){
            out<<"Use this: login <user> <password>\n";
            return;
        }
        login_cmd(in,out);
        return;
    }
    if(cmd=="logout"){
        if(in.size()!=1){
            out<<"Use this: logout\n";
            return;
        }
        if(logout_cmd(in,out)){
            sharing=false; // stop serving pieces until the next login
            lock_guard<mutex> lk(mauth);
            gl=false;
            gu.clear();
            gtoken.clear();
        }
        return;
    }
    if(cmd=="create_group"||cmd=="join_group"||cmd == "leave_group"||cmd=="list_requests"||cmd=="accept_request"||cmd=="list_groups"||cmd=="list_files"){
        int code;
        string text;
        if(!tracker_cmd(line,code,text)){
            out<<"tracker is not reachable\n";
        }
        else{
            out<<"Tracker: "<<text<<"\n";
            if(cmd=="leave_group"&&in.size()==2&&code==ST_OK){
                forget_group(in[1]);
            }
        }
        return;
    }
    if(cmd=="upload_file"){
        if(in.size()!=3){
            out<<"use: upload_file <gid> <filepath>\n"; return;
        }
        upload_file_cmd(in,out);
        return;
    }
    if(cmd=="download_file"){
        if(in.size()!=4){
            out<<"use: download_file <gid> <fname> <dest>\n"; return;
        }
        download_file_cmd(in,out);
        return;
    }
    if(cmd=="show_downloads"){
        show_downloads_cmd(out);
        return;
    }
    if(cmd=="stop_share"){
        if(in.size()!=3){
            out<<"use: stop_share <gid> <fname>\n"; return;
        }
        stop_share_cmd(in,out);
        return;
    }
    int code;
    string text;
    if (!tracker_cmd(line,code,text)){
        out<<"tracker is not reachable\n";
    }
    else{
        out<<"Tracker: "<<text<<"\n";
    }
}

void cmmd(){
    while(true){
        cout<<"$> ";
        string line;
        if(!getline(cin,line)){
            break;
        }
        // the web page runs commands too: one at a time
        lock_guard<mutex> lk(mcmd);
        run_command(line,cout);
    }
}

//web UI: optional page at http://127.0.0.1:<web_port>/?t=<token> (see webui.h)

// random per run. Every API call must carry it, so another web page open in the same
// browser can't send commands to this client
string web_token;

struct HttpReq{
    string method,path,query,body;
    map<string,string> headers;   // names in lower case
};

// one HTTP/1.1 request; the server answers it and closes the connection
bool read_http(int s,HttpReq &r){
    string data;
    char buf[8192];
    size_t end;
    while((end=data.find("\r\n\r\n"))==string::npos){
        if(data.size()>65536)
            return false;
        ssize_t n=recv(s,buf,sizeof(buf),0);
        if(n<=0)
            return false;
        data.append(buf,n);
    }
    stringstream ss(data.substr(0,end));
    string line;
    getline(ss,line);
    auto first=splits(line);
    if(first.size()<2)
        return false;
    r.method=first[0];
    size_t q=first[1].find('?');
    r.path=first[1].substr(0,q);
    r.query=q==string::npos?"":first[1].substr(q+1);
    while(getline(ss,line)){
        size_t c=line.find(':');
        if(c==string::npos)
            continue;
        string k=line.substr(0,c),v=line.substr(c+1);
        transform(k.begin(),k.end(),k.begin(),::tolower);
        while(!v.empty()&&(v[0]==' '||v[0]=='\t'))
            v.erase(0,1);
        while(!v.empty()&&(v.back()=='\r'||v.back()==' '))
            v.pop_back();
        r.headers[k]=v;
    }
    long long len=0;
    if(r.headers.count("content-length")&&(!parse_num(r.headers["content-length"],len)||len<0||len>(1<<20)))
        return false;
    r.body=data.substr(end+4);
    while((long long)r.body.size()<len){
        ssize_t n=recv(s,buf,sizeof(buf),0);
        if(n<=0)
            return false;
        r.body.append(buf,n);
    }
    r.body.resize(len);
    return true;
}

void send_http(int s,int code,const string &type,const string &body){
    string reason=code==200?"OK":code==403?"Forbidden":code==404?"Not Found":"Bad Request";
    string all="HTTP/1.1 "+to_string(code)+" "+reason+"\r\nContent-Type: "+type+"\r\nContent-Length: "+to_string(body.size())+
        "\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n"+body;
    send_all(s,all.data(),all.size());
}

string json_str(const string &s){
    string o="\"";
    for(unsigned char c:s){
        if(c=='"'||c=='\\'){
            o+='\\';
            o+=(char)c;
        }
        else if(c<0x20){
            char b[8];
            snprintf(b,sizeof(b),"\\u%04x",c);
            o+=b;
        }
        else{
            o+=(char)c;
        }
    }
    return o+"\"";
}

string json_list(const vector<string> &v){
    string o="[";
    for(size_t i=0;i<v.size();i++){
        o+=(i?",":"")+json_str(v[i]);
    }
    return o+"]";
}

string query_param(const string &query,const string &key){
    stringstream ss(query);
    string kv;
    while(getline(ss,kv,'&')){
        size_t e=kv.find('=');
        if(kv.substr(0,e)!=key)
            continue;
        string v=e==string::npos?"":kv.substr(e+1),out;
        for(size_t i=0;i<v.size();i++){
            if(v[i]=='+'){
                out+=' ';
            }
            else if(v[i]=='%'&&i+2<v.size()){
                out+=(char)strtol(v.substr(i+1,2).c_str(),nullptr,16);
                i+=2;
            }
            else{
                out+=v[i];
            }
        }
        return out;
    }
    return "";
}

// compare in constant time, so response timing reveals nothing about the token
bool same_token(const string &a,const string &b){
    if(a.size()!=b.size())
        return false;
    unsigned char diff=0;
    for(size_t i=0;i<a.size();i++){
        diff|=a[i]^b[i];
    }
    return diff==0;
}

// everything the page shows: login, downloads with per-peer progress, shared files, groups
string web_state(){
    string user;
    bool li;
    {
        lock_guard<mutex> lk(mauth);
        user=gu;
        li=gl;
    }
    auto tr=preferred==0?tracker1:tracker2;
    char cwd[PATH_MAX];
    string here=getcwd(cwd,sizeof(cwd))?cwd:".";
    string j="{\"peer\":"+json_str(p_sip+":"+p_sp)+",\"tracker\":"+json_str(tr.first+":"+tr.second)+",\"cwd\":"+json_str(here)+
        ",\"logged_in\":"+(li?"true":"false")+",\"user\":"+json_str(user)+",\"downloads\":[";
    {
        lock_guard<mutex> lk(mdown);
        bool first=true;
        for(auto &kv:downloads){
            size_t c=kv.first.find(':');
            const string &st=kv.second;
            j+=string(first?"":",")+"{\"group\":"+json_str(kv.first.substr(0,c))+",\"file\":"+json_str(kv.first.substr(c+1))+
                ",\"status\":"+json_str(st)+",\"state\":"+json_str(st.size()>1?st.substr(1,1):"");
            first=false;
            auto it=active.find(kv.first);
            if(it!=active.end()){
                auto &d=*it->second;
                lock_guard<mutex> lk2(d.m);
                j+=",\"done\":"+to_string(d.done)+",\"total\":"+to_string(d.n)+",\"peers\":[";
                bool pf=true;
                for(auto &p:d.peers){
                    j+=string(pf?"":",")+"{\"addr\":"+json_str(p.first)+",\"user\":"+json_str(p.second.uid)+",\"pieces\":"+to_string(p.second.pieces)+
                        ",\"complete\":"+(p.second.complete?"true":"false")+",\"banned\":"+(p.second.banned?"true":"false")+"}";
                    pf=false;
                }
                j+="]";
            }
            j+="}";
        }
    }
    j+="],\"shared\":[";
    {
        lock_guard<mutex> lk(mf);
        bool first=true;
        for(auto &kv:sharedFiles){
            size_t c=kv.first.find(':');
            j+=string(first?"":",")+"{\"group\":"+json_str(kv.first.substr(0,c))+",\"file\":"+json_str(kv.first.substr(c+1))+
                ",\"path\":"+json_str(kv.second.path)+",\"size\":"+to_string(kv.second.size)+",\"partial\":"+(kv.second.dl?"true":"false")+"}";
            first=false;
        }
    }
    vector<string> grps;
    int code;
    string text;
    if(li&&tracker_cmd("list_groups",code,text)&&code==ST_OK&&text!="no groups"){
        grps=splits(text);
    }
    return j+"],\"groups\":"+json_list(grps)+"}";
}

// files of one group, whether we are a member, and (for the owner) its join requests
string web_group(const string &g){
    if(g.empty()||g.find_first_of(" \t\r\n")!=string::npos)
        return "{\"error\":\"bad group id\"}";
    int code;
    string text;
    string j="{\"group\":"+json_str(g);
    if(!tracker_cmd("list_files "+g,code,text))
        return j+",\"error\":\"tracker not reachable\"}";
    vector<string> files,reqs;
    if(code==ST_OK&&text!="No files in the group")
        files=splits(text);
    j+=string(",\"member\":")+(code==ST_OK?"true":"false");
    if(code!=ST_OK&&code!=ST_FORBIDDEN)
        j+=",\"error\":"+json_str(text);
    bool owner=tracker_cmd("list_requests "+g,code,text)&&code==ST_OK;
    if(owner&&text!="no pending requests")
        reqs=splits(text);
    return j+",\"files\":"+json_list(files)+",\"owner\":"+(owner?"true":"false")+",\"requests\":"+json_list(reqs)+"}";
}

void web_handler(int s){
    set_timeout(s,10);
    HttpReq r;
    if(!read_http(s,r)){
        close(s);
        return;
    }
    const string js="application/json";
    if(r.method=="GET"&&r.path=="/"){
        send_http(s,200,"text/html; charset=utf-8",WEB_PAGE);
    }
    else if(r.path.compare(0,5,"/api/")!=0){
        send_http(s,404,"text/plain","not found");
    }
    else if(!same_token(r.headers["x-p2p-token"],web_token)){
        send_http(s,403,js,"{\"error\":\"Open the link the client printed (it contains the access token)\"}");
    }
    else if(r.method=="GET"&&r.path=="/api/state"){
        send_http(s,200,js,web_state());
    }
    else if(r.method=="GET"&&r.path=="/api/group"){
        send_http(s,200,js,web_group(query_param(r.query,"g")));
    }
    else if(r.method=="POST"&&r.path=="/api/cmd"){
        // one command line, exactly as typed in the terminal
        string line=r.body.substr(0,r.body.find_first_of("\r\n"));
        ostringstream out;
        {
            lock_guard<mutex> lk(mcmd);
            run_command(line,out);
        }
        send_http(s,200,js,"{\"output\":"+json_str(out.str())+"}");
    }
    else{
        send_http(s,404,js,"{\"error\":\"not found\"}");
    }
    close(s);
}

// only on 127.0.0.1: the page can run any command, so other machines must not reach it
int open_web_server(int port){
    int sock=socket(AF_INET,SOCK_STREAM,0);
    if(sock<0){
        perror("web socket");
        return -1;
    }
    int opt=1;
    setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons(port);
    sa.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    if(::bind(sock,(sockaddr*)&sa,sizeof(sa))<0||listen(sock,16)<0){
        perror("web port");
        close(sock);
        return -1;
    }
    return sock;
}

void web_server(int sock){
    while(true){
        int cs=accept(sock,nullptr,nullptr);
        if(cs<0)
            continue;
        thread(web_handler,cs).detach();
    }
}

string random_token(){
    unsigned char b[16];
    if(RAND_bytes(b,sizeof(b))!=1){
        random_device rd;
        for(auto &c:b)
            c=(unsigned char)rd();
    }
    char out[33];
    for(int i=0;i<16;i++){
        snprintf(out+i*2,3,"%02x",b[i]);
    }
    return string(out,32);
}

void tnpdetails(char const *argv[]) {
    string self(argv[1]);
    size_t c=self.find(':');
    if(c==string::npos||!valid_port(self.substr(c+1))){
        cerr<<"use this: ./client <IP:PORT> <tracker_info.txt>\n";
        exit(1);
    }
    p_sip=self.substr(0,c);
    p_sp=self.substr(c+1);
    int fd=open(argv[2],O_RDONLY);
    if(fd<0){
        perror("not able to open tracker details file");
        exit(1);
    }
    char buff[4096];
    ssize_t n=read(fd,buff,sizeof(buff)-1);
    if(n<0){
        perror("reading error from tracker file");
        close(fd);
        exit(1);
    }
    buff[n]='\0';
    close(fd);
    int ln=0;
    char ip[256],port[256];
    char* spr;
    char* lpr=strtok_r(buff,"\n",&spr);
    while(lpr && ln<2){
        if(sscanf(lpr,"%255s %255s",ip,port)==2&&valid_port(port)){
            if(ln==0){
                tracker1={ip,port};
            }
            else{
                tracker2={ip,port};
            }
            ln++;
        }
        lpr=strtok_r(NULL,"\n",&spr);
    }
    if(ln==0){
        cerr<<"no valid tracker address in "<<argv[2]<<"\n";
        exit(1);
    }
}

int main(int argc, char const *argv[]) {
    if(argc<3) {
        cerr<<"use this: ./client <IP:PORT> <tracker_info.txt> [web_port]\n";
        return 1;
    }
    if(argc>=4&&!valid_port(argv[3])){
        cerr<<"web_port must be a port number\n";
        return 1;
    }
    // writing to a socket whose peer has gone must return an error, not kill the client
    signal(SIGPIPE,SIG_IGN);
    const char *delay=getenv("P2P_TEST_DELAY_MS");
    long long dms;
    if(delay&&parse_num(delay,dms)&&dms>0)
        test_delay_ms=(int)dms;
    initLogger();

    cout<<"\n";
    tnpdetails(argv);
    statePath="p2p_client_"+p_sp+".state";
    load_state();
    int psock=open_peer_server();
    if(psock<0){
        return 1;
    }
    thread(peer_server,psock).detach();
    // optional web UI on 127.0.0.1:<web_port>
    if(argc>=4){
        int wsock=open_web_server(stoi(argv[3]));
        if(wsock<0){
            return 1;
        }
        web_token=random_token();
        thread(web_server,wsock).detach();
        cout<<"Web UI: http://127.0.0.1:"<<argv[3]<<"/?t="<<web_token<<"\n";
    }
    int s=connect_backup();
    con=false;
    close_tracker(s);
    thread(heartbeat).detach();
    cmmd();
    {
        lock_guard<mutex> lk(msock);
        close_tracker(tsock);
    }
    return 0;
}
