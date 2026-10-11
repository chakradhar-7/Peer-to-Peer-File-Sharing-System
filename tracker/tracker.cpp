#include<iostream>
#include<cstdio>
#include<string>
#include<cstring>
#include<vector>
#include<map>
#include<set>
#include<deque>
#include<memory>
#include<utility>
#include<sstream>
#include<algorithm>
#include<sys/socket.h>
#include<thread>
#include<arpa/inet.h>
#include<errno.h>
#include<mutex>
#include<condition_variable>
#include<unistd.h>
#include<fcntl.h>
#include<poll.h>
#include<chrono>
#include<netinet/in.h>
#include<cstdlib>
#include<atomic>
#include<csignal>
#include<random>
#include<ctime>
#include<sys/time.h>
#include<openssl/evp.h>
#include<openssl/rand.h>
#include<openssl/sha.h>
#include<openssl/crypto.h>
#include "dashboard.h"
using namespace std;

#define PIECE_SIZE 524288
// largest message accepted: upload_file for a 1GB file is ~85KB, a full snapshot a few MB
#define MAX_MSG (64*1024*1024)
#define HDR_SIZE 6              // message header: type, status, 4-byte length
#define WORKERS 8               // threads that handle requests; a connection itself costs no thread
#define HEARTBEAT_TIMEOUT 15    // a client that sends heartbeats and then goes quiet is gone
#define SYNC_TIMEOUT 8          // the other tracker pings every 2s
// after the link to the other tracker drops, "logged in through it" is trusted this many
// seconds: its clients fail over here (heartbeats) well within it, and ones that crashed expire
#define REMOTE_GRACE 20
#define PBKDF2_ROUNDS 20000     // makes guessing passwords from a stolen state file slow
#define MAX_TOKENS 16           // session tokens kept per user
#define COMPACT_AFTER 20000     // state file lines before it is rewritten as one checkpoint

// first header byte: what the payload is
enum MsgType { MSG_REQ=1, MSG_REPLY=2, MSG_PIECE=3 };

// second header byte of a reply. The client uses the same numbers
enum Status {
    ST_OK=0,
    ST_BAD_REQUEST=1,   // malformed message or wrong arguments
    ST_NOT_LOGGED_IN=2,
    ST_AUTH_FAILED=3,   // unknown user, wrong password, or expired session
    ST_EXISTS=4,        // user, group or membership already exists
    ST_NOT_FOUND=5,     // group, file or join request doesn't exist
    ST_FORBIDDEN=6,     // not a member, not the owner, or a bad sync secret
    ST_CONFLICT=7,      // a different file with this name is already shared
    ST_UNAVAILABLE=8,   // (peer to peer) file not being shared right now
    ST_UNKNOWN_CMD=9
};

static atomic<bool> trackerRunning(true);

// tracker config
string tracker_ip, tracker_port;
string p_tracker_ip, p_tracker_port;
int tracker_no; // 1 or 2
string sync_secret; // shared by both trackers (TRACKER_SECRET), so clients can't send sync messages

vector<string> splits(string s) {
    vector<string> fs;
    string word;
    stringstream ss(s);
    while(ss>>word){
        fs.push_back(word);
    }
    return fs;
}

string join(const vector<string> &w,size_t from=0){
    string s;
    for(size_t i=from;i<w.size();i++){
        if(i>from)
            s+=" ";
        s+=w[i];
    }
    return s;
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

//network helpers

// send() may write only part of the buffer, so keep going until all of it is out. On a
// non-blocking socket, wait (up to 10s) whenever its send buffer is full
bool send_all(int sock,const char *data,size_t len){
    size_t sent=0;
    while(sent<len){
        ssize_t n=send(sock,data+sent,len-sent,0);
        if(n>0){
            sent+=n;
            continue;
        }
        if(n<0&&errno==EINTR)
            continue;
        if(n<0&&(errno==EAGAIN||errno==EWOULDBLOCK)){
            pollfd p{sock,POLLOUT,0};
            if(poll(&p,1,10000)==1)
                continue;
        }
        return false;
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

// is a whole message at the front of buf? 1: yes (moved into m), 0: not yet, -1: garbage
int take_msg(string &buf,Msg &m){
    if(buf.size()<HDR_SIZE)
        return 0;
    uint32_t len;
    memcpy(&len,buf.data()+2,4);
    len=ntohl(len);
    if(len>MAX_MSG)
        return -1;
    if(buf.size()<HDR_SIZE+(size_t)len)
        return 0;
    m.type=(uint8_t)buf[0];
    m.status=(uint8_t)buf[1];
    m.payload=buf.substr(HDR_SIZE,len);
    buf.erase(0,HDR_SIZE+len);
    return 1;
}

// a silent peer makes send/recv fail after sec seconds instead of blocking forever
void set_timeout(int sock,int sec){
    struct timeval tv;
    tv.tv_sec=sec;
    tv.tv_usec=0;
    setsockopt(sock,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    setsockopt(sock,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
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

void reply(int socketfd,int code,const string &mssg) {
    if(!send_msg(socketfd,MSG_REPLY,code,mssg)){
        cout<<"Unable to send reply to client"<<"\n";
    }
}

//crypto helpers

string to_hex(const unsigned char *d,size_t n){
    static const char *digits="0123456789abcdef";
    string s;
    for(size_t i=0;i<n;i++){
        s+=digits[d[i]>>4];
        s+=digits[d[i]&15];
    }
    return s;
}

string random_hex(int bytes){
    vector<unsigned char> b(bytes);
    if(RAND_bytes(b.data(),bytes)!=1){
        random_device rd;
        for(auto &c:b)
            c=(unsigned char)rd();
    }
    return to_hex(b.data(),b.size());
}

// salted, deliberately slow hash: the state file never holds a password, and a stolen
// copy can't be checked against guesses quickly
string password_hash(const string &pass,const string &salt){
    unsigned char out[32];
    PKCS5_PBKDF2_HMAC(pass.c_str(),pass.size(),(const unsigned char*)salt.data(),salt.size(),PBKDF2_ROUNDS,EVP_sha256(),sizeof(out),out);
    return to_hex(out,sizeof(out));
}

// session tokens are random, so a fast hash is enough to avoid storing them as they are
string token_hash(const string &token){
    unsigned char h[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char*)token.data(),token.size(),h);
    return to_hex(h,SHA256_DIGEST_LENGTH);
}

// compare in constant time, so response timing reveals nothing about a secret
bool same_secret(const string &a,const string &b){
    return a.size()==b.size()&&CRYPTO_memcmp(a.data(),b.data(),a.size())==0;
}

//data

struct userdetails {
public:
    string username,salt,pwhash,peerserverip,peerserverport;
    int origin=1;           // tracker the account was created on: decides conflicts
    vector<string> tokens;  // hashes of this user's session tokens
    int sessions=0;         // logged-in connections to this tracker
    bool remote=false;      // logged in through the other tracker
    userdetails(){}
    userdetails(string n,string s,string h,int o){
        username=n;
        salt=s;
        pwhash=h;
        origin=o;
    }
};

map<string,userdetails> login;

class group {
public:
    string grpid,grpowner;
    vector<string> grpmem,req; // the owner is always grpmem[0]
    int origin=1;              // tracker the group was created on: decides conflicts
    group(){}
    group(string gid,string p,int o){
        grpid=gid;
        grpowner=p;
        grpmem.push_back(p);
        origin=o;
    }
};

map<string,group> groups;

struct fileinfo{
public:
    string fname;
    size_t fsize=0;
    vector<string>phashes;
    string fhash;
    int origin=1;                                // tracker it was first uploaded through
    map<string,pair<string,string>> seeders;     // uid->(pip,pport): have the whole file
    map<string,pair<string,string>> leechers;    // uid->(pip,pport): downloading, serve some pieces
};

map<string,map<string,fileinfo>> group_files; //grpid->fname->finfo

// one lock for all tracker state (users, groups, files, the sync bookkeeping and the state
// file). Commands are tiny, and a single lock means changes are queued for the other tracker
// and written to disk in exactly the order they were applied here
mutex mstate;

//synchronization bookkeeping (see README, "Tracker synchronization")
string my_epoch;                       // random id of this tracker's state (kept across restarts)
long long my_seq=0;                    // number of the last change made here
deque<pair<long long,string>> outbox;  // changes made here the other tracker hasn't confirmed
condition_variable outbox_cv;
string peer_epoch;                     // the other tracker's state whose changes we apply
long long peer_applied=0;              // last change of it applied here
set<string> peer_data_from;            // the other tracker's states whose data we hold
long long sync_conn=0;                 // newest incoming sync link; older ones stop
bool peer_link=false;                  // that link is up
time_t peer_link_lost=0;               // when it went down
atomic<bool> out_link(false);          // our sync_sender's link is up (for the dashboard)
atomic<int> open_conns(0);             // connections the event loop holds (for the dashboard)

//persistence: tracker<N>.state is a checkpoint followed by one line per change since
string db_path;
int dbfd=-1;
long long db_lines=0;

// listed as a seeder: a session here, or a recent one through the other tracker (mstate held)
bool is_online(const userdetails &u){
    if(u.sessions>0)
        return true;
    return u.remote&&(peer_link||time(nullptr)-peer_link_lost<REMOTE_GRACE);
}

// online/offline notices describe live sessions, which don't survive a restart
bool durable(const vector<string> &op){
    return !op.empty()&&op[0]!="online"&&op[0]!="offline"&&op[0]!="noop";
}

void db_checkpoint();

void write_fd(int fd,const string &s){
    size_t off=0;
    while(off<s.size()){
        ssize_t n=write(fd,s.data()+off,s.size()-off);
        if(n<0&&errno==EINTR)
            continue;
        if(n<=0){
            perror("state file write");
            return;
        }
        off+=n;
    }
}

// append one line to the state file (mstate held)
void db_write(const string &line){
    if(dbfd<0)
        return;
    write_fd(dbfd,line+"\n");
    if(++db_lines>=COMPACT_AFTER)
        db_checkpoint();
}

// queue a change made here for the other tracker, and record it on disk (mstate held)
void publish(const string &op){
    outbox.push_back({++my_seq,op});
    db_write("L "+to_string(my_seq)+" "+op);
    outbox_cv.notify_all();
}

// drop user as a seeder or downloader in gid; if the group itself was deleted, drop all of
// its files
void remove_member_files(const string &gid,const string &user,bool groupdeleted){
    auto g_it=group_files.find(gid);
    if(g_it==group_files.end()){
        return;
    }
    if(groupdeleted){
        group_files.erase(g_it);
        return;
    }
    auto &files=g_it->second;
    vector<string> er;
    for(auto &f:files){
        f.second.seeders.erase(user);
        f.second.leechers.erase(user);
        if(f.second.seeders.empty()&&f.second.leechers.empty()){
            er.push_back(f.first);
        }
    }
    for(auto &fname:er){
        files.erase(fname);
    }
    if(files.empty()){
        group_files.erase(g_it);
    }
}

// create the file record, or settle a clash with a different file under the same name made
// while the trackers were cut off: the version uploaded through tracker 1 wins and copies of
// the other version are dropped. Returns null if this version loses (mstate held)
fileinfo* put_file(const string &gid,const string &fname,long long fsize,const string &fhash,const vector<string> &ph,long long origin){
    auto &files=group_files[gid];
    auto it=files.find(fname);
    if(it==files.end()){
        fileinfo f;
        f.fname=fname;
        f.fsize=(size_t)fsize;
        f.fhash=fhash;
        f.phashes=ph;
        f.origin=(int)origin;
        return &(files[fname]=f);
    }
    fileinfo &f=it->second;
    if(f.fhash==fhash){
        return &f;
    }
    if(origin<f.origin){
        f.fsize=(size_t)fsize;
        f.fhash=fhash;
        f.phashes=ph;
        f.origin=(int)origin;
        f.seeders.clear();
        f.leechers.clear();
        return &f;
    }
    return nullptr;
}

// the only place shared state changes: used for commands here, for changes received from
// the other tracker, for snapshots and when replaying the state file (mstate held). Changes
// from the other tracker were checked there, so this only guards against the two trackers'
// states differing
void apply_op(const vector<string> &op){
    if(op.empty()){
        return;
    }
    const string &cmd=op[0];
    long long origin;
    if(cmd=="create_user"&&op.size()==5&&parse_num(op[4],origin)){
        auto it=login.find(op[1]);
        if(it==login.end()){
            login[op[1]]=userdetails(op[1],op[2],op[3],(int)origin);
        }
        // the same name was created on both trackers while they were cut off from each
        // other: the account made through tracker 1 wins, on both trackers
        else if(it->second.pwhash!=op[3]&&origin<it->second.origin){
            it->second.salt=op[2];
            it->second.pwhash=op[3];
            it->second.origin=(int)origin;
            it->second.tokens.clear();
        }
    }
    else if(cmd=="add_token"&&op.size()==3){
        auto it=login.find(op[1]);
        if(it!=login.end()){
            auto &t=it->second.tokens;
            if(find(t.begin(),t.end(),op[2])==t.end()){
                t.push_back(op[2]);
                if(t.size()>MAX_TOKENS)
                    t.erase(t.begin());
            }
        }
    }
    else if(cmd=="revoke_token"&&op.size()==3){
        auto it=login.find(op[1]);
        if(it!=login.end()){
            auto &t=it->second.tokens;
            t.erase(remove(t.begin(),t.end(),op[2]),t.end());
        }
    }
    else if(cmd=="online"&&op.size()==4){
        auto it=login.find(op[1]);
        if(it!=login.end()){
            it->second.remote=true;
            it->second.peerserverip=op[2];
            it->second.peerserverport=op[3];
        }
    }
    else if(cmd=="offline"&&op.size()==2){
        auto it=login.find(op[1]);
        if(it!=login.end())
            it->second.remote=false;
    }
    else if(cmd=="create_group"&&op.size()==4&&parse_num(op[3],origin)){
        auto it=groups.find(op[1]);
        if(it==groups.end()){
            groups[op[1]]=group(op[1],op[2],(int)origin);
        }
        else if(it->second.grpowner!=op[2]&&origin!=it->second.origin){
            // created on both trackers while cut off: tracker 1's owner wins, and the other
            // creator stays a member, so both trackers end up with the same group
            auto &g=it->second;
            string winner=origin<g.origin?op[2]:g.grpowner;
            string loser=origin<g.origin?g.grpowner:op[2];
            g.grpowner=winner;
            g.origin=min(g.origin,(int)origin);
            g.grpmem.erase(remove(g.grpmem.begin(),g.grpmem.end(),winner),g.grpmem.end());
            g.grpmem.insert(g.grpmem.begin(),winner);
            if(find(g.grpmem.begin(),g.grpmem.end(),loser)==g.grpmem.end())
                g.grpmem.push_back(loser);
            g.req.erase(remove(g.req.begin(),g.req.end(),winner),g.req.end());
            g.req.erase(remove(g.req.begin(),g.req.end(),loser),g.req.end());
        }
    }
    else if(cmd=="add_member"&&op.size()==3){ // only appears in snapshots
        auto it=groups.find(op[1]);
        if(it!=groups.end()){
            auto &g=it->second;
            if(find(g.grpmem.begin(),g.grpmem.end(),op[2])==g.grpmem.end())
                g.grpmem.push_back(op[2]);
        }
    }
    else if(cmd=="join_group"&&op.size()==3){
        auto it=groups.find(op[1]);
        if(it!=groups.end()){
            auto &g=it->second;
            if(find(g.grpmem.begin(),g.grpmem.end(),op[2])==g.grpmem.end()&&find(g.req.begin(),g.req.end(),op[2])==g.req.end())
                g.req.push_back(op[2]);
        }
    }
    else if(cmd=="accept_request"&&op.size()==3){
        auto it=groups.find(op[1]);
        if(it!=groups.end()){
            auto &g=it->second;
            auto rit=find(g.req.begin(),g.req.end(),op[2]);
            if(rit!=g.req.end()){
                g.req.erase(rit);
                if(find(g.grpmem.begin(),g.grpmem.end(),op[2])==g.grpmem.end())
                    g.grpmem.push_back(op[2]);
            }
        }
    }
    else if(cmd=="leave_group"&&op.size()==3){
        string gid=op[1];
        string user=op[2];
        auto it=groups.find(gid);
        if(it==groups.end()){
            return;
        }
        auto &g=it->second;
        auto mit=find(g.grpmem.begin(),g.grpmem.end(),user);
        if(mit==g.grpmem.end()){
            return;
        }
        g.grpmem.erase(mit);
        bool deleted=false;
        if(g.grpowner==user){
            if(!g.grpmem.empty()){
                g.grpowner=g.grpmem.front();
            }
            else{
                groups.erase(it);
                deleted=true;
            }
        }
        // remove users files from seeders (or all files, if the group is gone)
        remove_member_files(gid,user,deleted);
    }
    //upload_file gid fname fsize fhash pcount phash1..phashn uid pip pport origin
    else if(cmd=="upload_file"&&op.size()>=10){
        long long fsize,n;
        if(!parse_num(op[3],fsize)||!parse_num(op[5],n)||fsize<0||n<0||(long long)op.size()!=10+n||!parse_num(op[9+n],origin)||groups.find(op[1])==groups.end()){
            return;
        }
        fileinfo *f=put_file(op[1],op[2],fsize,op[4],vector<string>(op.begin()+6,op.begin()+6+n),origin);
        if(f){
            string uid=op[6+n];
            f->seeders[uid]={op[7+n],op[8+n]};
            f->leechers.erase(uid);
        }
    }
    //file_meta gid fname fsize fhash pcount phash1..phashn origin (only in snapshots)
    else if(cmd=="file_meta"&&op.size()>=7){
        long long fsize,n;
        if(!parse_num(op[3],fsize)||!parse_num(op[5],n)||fsize<0||n<0||(long long)op.size()!=7+n||!parse_num(op[6+n],origin)||groups.find(op[1])==groups.end()){
            return;
        }
        put_file(op[1],op[2],fsize,op[4],vector<string>(op.begin()+6,op.begin()+6+n),origin);
    }
    //seeder|leech gid fname uid pip pport
    else if((cmd=="seeder"||cmd=="leech")&&op.size()==6){
        auto g_it=group_files.find(op[1]);
        if(g_it==group_files.end()){
            return;
        }
        auto f_it=g_it->second.find(op[2]);
        if(f_it==g_it->second.end()){
            return;
        }
        auto &f=f_it->second;
        if(cmd=="seeder"){
            f.seeders[op[3]]={op[4],op[5]};
            f.leechers.erase(op[3]);
        }
        else if(f.seeders.count(op[3])==0){
            f.leechers[op[3]]={op[4],op[5]};
        }
    }
    else if(cmd=="stop_share"&&op.size()==4){
        auto g_it=group_files.find(op[1]);
        if(g_it==group_files.end()){
            return;
        }
        auto f_it=g_it->second.find(op[2]);
        if(f_it==g_it->second.end()){
            return;
        }
        f_it->second.seeders.erase(op[3]);
        f_it->second.leechers.erase(op[3]);
        if(f_it->second.seeders.empty()&&f_it->second.leechers.empty()){
            g_it->second.erase(f_it);
        }
        if(g_it->second.empty()){
            group_files.erase(g_it);
        }
    }
}

// make a change here and queue the same change for the other tracker (mstate held)
void commit(const vector<string> &op){
    apply_op(op);
    publish(join(op));
}

// forget who is logged in through the other tracker (mstate held)
void clear_remote(){
    for(auto &u:login){
        u.second.remote=false;
    }
}

// this tracker's whole state, written as changes that rebuild it (mstate held)
string build_snapshot(bool presence){
    string s;
    for(auto &u:login){
        s+="create_user "+u.first+" "+u.second.salt+" "+u.second.pwhash+" "+to_string(u.second.origin)+"\n";
        for(auto &t:u.second.tokens){
            s+="add_token "+u.first+" "+t+"\n";
        }
    }
    for(auto &g:groups){
        s+="create_group "+g.first+" "+g.second.grpowner+" "+to_string(g.second.origin)+"\n";
        for(auto &m:g.second.grpmem){
            if(m!=g.second.grpowner)
                s+="add_member "+g.first+" "+m+"\n";
        }
        for(auto &r:g.second.req){
            s+="join_group "+g.first+" "+r+"\n";
        }
    }
    for(auto &gf:group_files){
        for(auto &f:gf.second){
            s+="file_meta "+gf.first+" "+f.first+" "+to_string(f.second.fsize)+" "+f.second.fhash+" "+to_string(f.second.phashes.size());
            for(auto &h:f.second.phashes){
                s+=" "+h;
            }
            s+=" "+to_string(f.second.origin)+"\n";
            for(auto &sd:f.second.seeders){
                s+="seeder "+gf.first+" "+f.first+" "+sd.first+" "+sd.second.first+" "+sd.second.second+"\n";
            }
            for(auto &l:f.second.leechers){
                s+="leech "+gf.first+" "+f.first+" "+l.first+" "+l.second.first+" "+l.second.second+"\n";
            }
        }
    }
    // users with a session here are online for the other tracker too
    if(presence){
        for(auto &u:login){
            if(u.second.sessions>0)
                s+="online "+u.first+" "+u.second.peerserverip+" "+u.second.peerserverport+"\n";
        }
    }
    return s;
}

// PRESENCE <uid> <ip> <port> ...: users that have a session here right now (mstate held)
string build_presence(){
    string s="PRESENCE";
    for(auto &u:login){
        if(u.second.sessions>0)
            s+=" "+u.first+" "+u.second.peerserverip+" "+u.second.peerserverport;
    }
    return s;
}

// merge a snapshot from the other tracker into this one (mstate held)
void apply_snapshot(const string &body){
    clear_remote();
    stringstream ss(body);
    string line;
    while(getline(ss,line)){
        apply_op(splits(line));
    }
}

// rewrite the state file as one checkpoint: the whole state plus the sync bookkeeping, so a
// restarted tracker continues as the same run (mstate held)
void db_checkpoint(){
    if(db_path.empty()){
        return;
    }
    string s="E "+my_epoch+" "+to_string(my_seq)+"\n";
    if(!peer_epoch.empty())
        s+="P "+peer_epoch+" "+to_string(peer_applied)+"\n";
    if(!peer_data_from.empty()){
        s+="F";
        for(auto &e:peer_data_from)
            s+=" "+e;
        s+="\n";
    }
    stringstream ss(build_snapshot(false));
    string line;
    while(getline(ss,line)){
        s+="C "+line+"\n";
    }
    for(auto &o:outbox){
        s+="O "+to_string(o.first)+" "+o.second+"\n";
    }
    string tmp=db_path+".tmp";
    int fd=open(tmp.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0600);
    if(fd<0){
        perror("state file");
        return;
    }
    write_fd(fd,s);
    fsync(fd);
    close(fd);
    if(rename(tmp.c_str(),db_path.c_str())<0){
        perror("state file rename");
        return;
    }
    if(dbfd>=0)
        close(dbfd);
    dbfd=open(db_path.c_str(),O_WRONLY|O_APPEND);
    db_lines=0;
}

// rebuild state from the state file after a restart; false if there is none (mstate held).
// Line kinds: E epoch seq, P peer_epoch applied, F epochs.., C change (checkpointed state),
// O seq change (unconfirmed), L seq change (made here), R epoch seq change (from the other
// tracker), A seq (the other tracker confirmed up to seq)
bool db_load(){
    int fd=open(db_path.c_str(),O_RDONLY);
    if(fd<0){
        return false;
    }
    string data;
    char buf[65536];
    ssize_t n;
    while((n=read(fd,buf,sizeof(buf)))>0){
        data.append(buf,n);
    }
    close(fd);
    stringstream ss(data);
    string line;
    while(getline(ss,line)){
        if(line.size()<2)
            continue;
        char kind=line[0];
        vector<string> t=splits(line.substr(2));
        long long a;
        if(kind=='E'&&t.size()==2&&parse_num(t[1],a)){
            my_epoch=t[0];
            my_seq=a;
        }
        else if(kind=='P'&&t.size()==2&&parse_num(t[1],a)){
            peer_epoch=t[0];
            peer_applied=a;
        }
        else if(kind=='F'){
            peer_data_from.insert(t.begin(),t.end());
        }
        else if(kind=='C'){
            apply_op(t);
        }
        else if((kind=='O'||kind=='L')&&t.size()>=2&&parse_num(t[0],a)){
            vector<string> op(t.begin()+1,t.end());
            if(kind=='L'&&durable(op))
                apply_op(op);
            // sessions don't survive a restart, so their online/offline notices become no-ops
            outbox.push_back({a,durable(op)?join(op):"noop"});
            my_seq=max(my_seq,a);
        }
        else if(kind=='R'&&t.size()>=3&&parse_num(t[1],a)){
            vector<string> op(t.begin()+2,t.end());
            if(durable(op))
                apply_op(op);
            peer_epoch=t[0];
            peer_applied=a;
            peer_data_from.insert(t[0]);
        }
        else if(kind=='A'&&t.size()==1&&parse_num(t[0],a)){
            while(!outbox.empty()&&outbox.front().first<=a){
                outbox.pop_front();
            }
        }
    }
    return !my_epoch.empty();
}

// one client connection, or the incoming link from the other tracker
struct Conn{
    int fd=-1;
    string inbuf;            // bytes received but not yet a whole message
    string user;             // logged-in user on this connection
    string tokhash;          // its session token (revoked on logout)
    bool heartbeating=false;
    bool sync=false;         // incoming link from the other tracker
    string sync_epoch;
    long long sync_id=0;
    bool busy=false;         // a worker is handling one of its messages
    bool closing=false;      // close once the current message is done
    time_t last=0;           // when data last arrived
};

//commands

// a new session for u at this peer address (mstate held)
void start_session(userdetails &u,const string &psip,const string &psp){
    u.sessions++;
    u.peerserverip=psip;
    u.peerserverport=psp;
    // the other tracker lists this user's files while it has a session here
    publish("online "+u.username+" "+psip+" "+psp);
}

// one logged-in connection of user ended (logout, disconnect, or missed heartbeats)
void end_session(const string &user){
    lock_guard<mutex>lk(mstate);
    auto it=login.find(user);
    if(it==login.end()){
        return;
    }
    if(it->second.sessions>0){
        it->second.sessions--;
    }
    if(it->second.sessions==0){
        publish("offline "+user);
    }
}

void create_user(vector<string> ip,int socketfd) {
    string name=ip[1];
    string pass=ip[2];
    string salt=random_hex(16);
    string hash=password_hash(pass,salt); // slow on purpose, so computed without the lock
    bool created=false;
    {
        lock_guard<mutex>lk(mstate);
        if(login.find(name)==login.end()){
            commit({"create_user",name,salt,hash,to_string(tracker_no)});
            created=true;
        }
    }
    // reply after unlocking, so a slow client never blocks the others
    if(created){
        reply(socketfd,ST_OK,"user created successfully");
    }
    else{
        reply(socketfd,ST_EXISTS,"user already exists");
    }
}

// login <u> <password> <ip> <port>: reply "<token> user login successfull". The client uses
// the token, not the password, to continue its session on a new connection
void loginuser(Conn &c,vector<string> ip) {
    string name=ip[1];
    string pass=ip[2];
    string psip=ip[3];
    string psp=ip[4];
    string salt,hash;
    {
        lock_guard<mutex>lk(mstate);
        auto it=login.find(name);
        if(it!=login.end()){
            salt=it->second.salt;
            hash=it->second.pwhash;
        }
    }
    if(salt.empty()){
        reply(c.fd,ST_AUTH_FAILED,"Login failed");
        return;
    }
    if(!same_secret(password_hash(pass,salt),hash)){
        reply(c.fd,ST_AUTH_FAILED,"Invalid credentials");
        return;
    }
    string token=random_hex(16);
    string th=token_hash(token);
    {
        lock_guard<mutex>lk(mstate);
        start_session(login[name],psip,psp);
        commit({"add_token",name,th});
        // logging in again on the same connection replaces its old session
        if(!c.tokhash.empty())
            commit({"revoke_token",c.user,c.tokhash});
    }
    if(!c.user.empty()){
        end_session(c.user);
    }
    c.user=name;
    c.tokhash=th;
    // logged-in clients send heartbeats: from now on, 15s of silence means it is gone
    c.heartbeating=true;
    reply(c.fd,ST_OK,token+" user login successfull");
}

// resume <u> <token> <ip> <port>: continue a session on a new connection (e.g. after the
// client failed over from the other tracker) without sending the password again
void resume_session(Conn &c,vector<string> ip){
    string name=ip[1];
    string th=token_hash(ip[2]);
    bool ok=false;
    {
        lock_guard<mutex>lk(mstate);
        auto it=login.find(name);
        if(it!=login.end()){
            for(auto &t:it->second.tokens){
                if(same_secret(t,th))
                    ok=true;
            }
            if(ok){
                start_session(it->second,ip[3],ip[4]);
            }
        }
    }
    if(!ok){
        reply(c.fd,ST_AUTH_FAILED,"session expired, please login again");
        return;
    }
    if(!c.user.empty()){
        end_session(c.user);
    }
    c.user=name;
    c.tokhash=th;
    c.heartbeating=true;
    reply(c.fd,ST_OK,"session resumed");
}

void create_group(vector<string> ip,int socketfd,string user) {
    string gid=ip[1];
    bool created=false;
    {
        lock_guard<mutex>lk(mstate);
        if(groups.find(gid)==groups.end()){
            commit({"create_group",gid,user,to_string(tracker_no)});
            created=true;
        }
    }
    if(created){
        reply(socketfd,ST_OK,"group created successfully");
    }
    else{
        reply(socketfd,ST_EXISTS,"group already exists");
    }
}

void join_group(vector<string> ip,int socketfd,string user) {
    string gid=ip[1];
    int code=ST_OK;
    string replytrack="join request to the group is sent";
    {
        lock_guard<mutex>lk(mstate);
        auto it=groups.find(gid);
        if(it==groups.end()){
            code=ST_NOT_FOUND;
            replytrack="group doesnt exist";
        }
        else if(find(it->second.grpmem.begin(),it->second.grpmem.end(),user)!=it->second.grpmem.end()){
            code=ST_EXISTS;
            replytrack="user is already present in the group";
        }
        else if(find(it->second.req.begin(),it->second.req.end(),user)==it->second.req.end()){
            commit({"join_group",gid,user});
        }
    }
    reply(socketfd,code,replytrack);
}

void leave_group(vector<string> ip,int socketfd,string user) {
    string gid=ip[1];
    int code=ST_OK;
    string replytrack;
    {
        lock_guard<mutex>lk(mstate);
        auto it=groups.find(gid);
        if(it==groups.end()){
            code=ST_NOT_FOUND;
            replytrack="group doesnt exist";
        }
        else if(find(it->second.grpmem.begin(),it->second.grpmem.end(),user)==it->second.grpmem.end()){
            code=ST_FORBIDDEN;
            replytrack="user is not a member of the group";
        }
        else{
            bool last=(it->second.grpmem.size()==1);
            commit({"leave_group",gid,user});
            if(last)
                replytrack="User is removed and no member exists in the group, so deleting the group";
            else
                replytrack="user removed from the group (file is removed if no seeder)";
        }
    }
    reply(socketfd,code,replytrack);
}

void list_groups(int socketfd) {
    string replytrack="";
    {
        lock_guard<mutex>lk(mstate);
        for (auto &g:groups) {
            replytrack+=g.first+" ";
        }
    }
    if(replytrack.empty()){
        reply(socketfd,ST_OK,"no groups");
        return;
    }
    replytrack.pop_back();
    reply(socketfd,ST_OK,replytrack);
}

void list_requests(vector<string> ip,int socketfd,string user) {
    string gid=ip[1];
    int code=ST_OK;
    string replytrack="";
    {
        lock_guard<mutex>lk(mstate);
        auto it=groups.find(gid);
        if(it==groups.end()){
            code=ST_NOT_FOUND;
            replytrack="group doesnt exist";
        }
        else if(it->second.grpowner!=user){
            code=ST_FORBIDDEN;
            replytrack="you are not the owner of the group";
        }
        else{
            for(auto &r:it->second.req) {
                replytrack+=r+" ";
            }
        }
    }
    if(code==ST_OK){
        if(replytrack.empty())
            replytrack="no pending requests";
        else
            replytrack.pop_back();
    }
    reply(socketfd,code,replytrack);
}

void accept_request(vector<string> ip,int socketfd,string user) {
    string gid=ip[1];
    string aid=ip[2];
    int code=ST_OK;
    string replytrack="user added to group";
    {
        lock_guard<mutex>lk(mstate);
        auto it=groups.find(gid);
        if(it==groups.end()){
            code=ST_NOT_FOUND;
            replytrack="group doesnt exist";
        }
        else if(it->second.grpowner!=user){
            code=ST_FORBIDDEN;
            replytrack="you are not the owner of the group";
        }
        else if(find(it->second.req.begin(),it->second.req.end(),aid)==it->second.req.end()){
            code=ST_NOT_FOUND;
            replytrack="not present in the requests";
        }
        else{
            commit({"accept_request",gid,aid});
        }
    }
    reply(socketfd,code,replytrack);
}

void logout(Conn &c){
    end_session(c.user);
    {
        lock_guard<mutex>lk(mstate);
        // an explicit logout means the user's client stopped sharing, even if it had
        // logged in through the other tracker before failing over here
        login[c.user].remote=false;
        if(!c.tokhash.empty())
            commit({"revoke_token",c.user,c.tokhash});
    }
    c.user.clear();
    c.tokhash.clear();
    reply(c.fd,ST_OK,"logout successfull");
}

// group must exist and user must be a member; otherwise sets the error reply (mstate held)
bool check_member(const string &gid,const string &user,int &code,string &msg){
    auto g=groups.find(gid);
    if(g==groups.end()){
        code=ST_NOT_FOUND;
        msg="Group doesnt exist";
        return false;
    }
    if(find(g->second.grpmem.begin(),g->second.grpmem.end(),user)==g->second.grpmem.end()){
        code=ST_FORBIDDEN;
        msg="not a member of this group";
        return false;
    }
    return true;
}

//upload_file gid fname fsize fhash pcount phash1..phashn uid pip pport
void upload_file(vector<string>ip,int socketfd, string user){
    // the message must hold exactly piecescnt hashes plus uid, ip and port, and the piece
    // count must match the file size
    long long fsize,piecescnt;
    if(ip.size()<9||!parse_num(ip[3],fsize)||!parse_num(ip[5],piecescnt)||fsize<0||piecescnt!=(fsize+PIECE_SIZE-1)/PIECE_SIZE||(long long)ip.size()!=9+piecescnt){
        reply(socketfd,ST_BAD_REQUEST,"wrong metadata");
        return;
    }
    string gid=ip[1];
    string fname=ip[2];
    string fhash=ip[4];
    // the uploader is whoever is logged in on this connection, not the uid written in the message
    ip[6+piecescnt]=user;
    ip.push_back(to_string(tracker_no));

    int code=ST_OK;
    string replytrack="file uploaded!";
    {
        lock_guard<mutex>lk(mstate);
        if(check_member(gid,user,code,replytrack)){
            auto gf=group_files.find(gid);
            // same name but different content would make this user a seeder of a file it doesn't have
            if(gf!=group_files.end()&&gf->second.count(fname)&&gf->second[fname].fhash!=fhash){
                code=ST_CONFLICT;
                replytrack="a different file with this name is already shared in the group";
            }
            else{
                commit(ip);
            }
        }
    }
    reply(socketfd,code,replytrack);
}

// start_download gid fname: the client began downloading. It serves the pieces it already
// has, so it is listed as a partial peer until it finishes (or stops sharing)
void start_download(vector<string> ip,int socketfd,string user){
    if(ip.size()!=3){
        reply(socketfd,ST_BAD_REQUEST,"invalid command");
        return;
    }
    string gid=ip[1];
    string fname=ip[2];
    int code=ST_OK;
    string msg="download registered";
    {
        lock_guard<mutex>lk(mstate);
        if(check_member(gid,user,code,msg)){
            auto gf=group_files.find(gid);
            if(gf==group_files.end()||gf->second.find(fname)==gf->second.end()){
                code=ST_NOT_FOUND;
                msg="File not found in this group";
            }
            else if(gf->second[fname].seeders.count(user)==0){
                auto &u=login[user];
                commit({"leech",gid,fname,user,u.peerserverip,u.peerserverport});
            }
        }
    }
    reply(socketfd,code,msg);
}

void list_files(vector<string>ip,int socketfd,string user){
    if(ip.size()!=2){
        reply(socketfd,ST_BAD_REQUEST,"invalid command");
        return;
    }
    string gid=ip[1];
    int code=ST_OK;
    string msg="";
    {
        lock_guard<mutex>lk(mstate);
        if(check_member(gid,user,code,msg)){
            auto it=group_files.find(gid);
            if(it!=group_files.end()){
                for(auto &f:it->second){
                    msg+=f.first+" ";
                }
            }
        }
    }
    if(code==ST_OK){
        if(msg.empty())
            msg="No files in the group";
        else
            msg.pop_back();
    }
    reply(socketfd,code,msg);
}

// reply: <fsize> <fhash> <pcount> <phash1..phashn> PEERS <uid> <ip> <port> <C|P> ...
// C = has the whole file, P = still downloading (serves the pieces it has)
void download_file(vector<string>ip,int socketfd,string user){
    if(ip.size()!=3){
        reply(socketfd,ST_BAD_REQUEST,"invalid command");
        return;
    }
    string gid=ip[1];
    string fname=ip[2];
    int code=ST_OK;
    string out="";
    {
        lock_guard<mutex>lk(mstate);
        if(check_member(gid,user,code,out)){
            auto gf=group_files.find(gid);
            if(gf==group_files.end()||gf->second.find(fname)==gf->second.end()){
                code=ST_NOT_FOUND;
                out="File not found in this group";
            }
            else{
                fileinfo &f=gf->second[fname];
                stringstream ss;
                ss<<f.fsize<<" "<<f.fhash<<" "<<f.phashes.size();
                for(auto &h:f.phashes){
                    ss<<" "<<h;
                }
                ss<<" PEERS";
                // only peers online right now, at the address they last logged in from
                for(int kind=0;kind<2;kind++){
                    for(auto &s:kind==0?f.seeders:f.leechers) {
                        auto it=login.find(s.first);
                        if(it==login.end()||!is_online(it->second))
                            continue;
                        string pip=it->second.peerserverip.empty()?s.second.first:it->second.peerserverip;
                        string pport=it->second.peerserverport.empty()?s.second.second:it->second.peerserverport;
                        ss<<" "<<s.first<<" "<<pip<<" "<<pport<<" "<<(kind==0?"C":"P");
                    }
                }
                out=ss.str();
            }
        }
    }
    reply(socketfd,code,out);
}

void stop_share(vector<string>ip,int socketfd,string user){
    if(ip.size()!=3){
        reply(socketfd,ST_BAD_REQUEST,"Invalid command");
        return;
    }
    string gid=ip[1];
    string fname=ip[2];
    int code=ST_OK;
    string replytrack="sharing this file is stopped";
    {
        lock_guard<mutex>lk(mstate);
        if(check_member(gid,user,code,replytrack)){
            auto gf=group_files.find(gid);
            if(gf==group_files.end()||gf->second.find(fname)==gf->second.end()){
                code=ST_NOT_FOUND;
                replytrack="File not found in this group";
            }
            else if(gf->second[fname].seeders.count(user)==0&&gf->second[fname].leechers.count(user)==0){
                code=ST_NOT_FOUND;
                replytrack="you are not sharing this file";
            }
            else{
                commit({"stop_share",gid,fname,user});
            }
        }
    }
    reply(socketfd,code,replytrack);
}

//tracker-to-tracker synchronization

int connect_peer(){
    if(p_tracker_ip.empty()||p_tracker_port.empty()){
        return -1;
    }
    int sock=socket(AF_INET,SOCK_STREAM,0);
    if(sock<0){
        return -1;
    }
    struct sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons((uint16_t)stoi(p_tracker_port));
    if(inet_pton(AF_INET,p_tracker_ip.c_str(),&sa.sin_addr)<=0||!connect_with_timeout(sock,sa,3)){
        close(sock);
        return -1;
    }
    return sock;
}

// SNAPSHOT_REQ <secret>: a tracker that starts without a state file copies this tracker's
// whole state. Reply: SNAPSHOT <epoch> <seq>, then one change per line
void send_snapshot(int sock,const vector<string> &ip){
    if(ip.size()!=2||!same_secret(ip[1],sync_secret)){
        reply(sock,ST_FORBIDDEN,"bad sync secret");
        return;
    }
    string snap;
    {
        lock_guard<mutex>lk(mstate);
        snap="SNAPSHOT "+my_epoch+" "+to_string(my_seq)+"\n"+build_snapshot(true);
    }
    reply(sock,ST_OK,snap);
}

// SYNC_HELLO <secret> <epoch>: the other tracker's sync_sender opens its change stream.
// Reply: RESUME <last change applied> <our epoch>, or NEW 0 <our epoch>
void sync_hello(Conn &c,const vector<string> &hello){
    if(hello.size()!=3||!same_secret(hello[1],sync_secret)){
        reply(c.fd,ST_FORBIDDEN,"bad sync secret");
        c.closing=true;
        return;
    }
    string epoch=hello[2];
    string answer;
    {
        lock_guard<mutex>lk(mstate);
        c.sync_id=++sync_conn;
        if(epoch==peer_epoch){
            // same state as before: continue after the last change we applied
            answer="RESUME "+to_string(peer_applied);
        }
        else{
            // the other tracker lost its state: the sessions it had are gone too
            clear_remote();
            peer_epoch=epoch;
            peer_applied=0;
            db_write("P "+epoch+" 0");
            answer="NEW 0";
        }
        answer+=" "+my_epoch;
        peer_link=true;
    }
    c.sync=true;
    c.sync_epoch=epoch;
    reply(c.fd,ST_OK,answer);
}

// one message on the incoming sync link: SNAPSHOT / PRESENCE / OP <seq> <change> / PING,
// each acknowledged so the sender knows what arrived
void handle_sync(Conn &c,const string &msg){
    size_t nl=msg.find('\n');
    vector<string> head=splits(msg.substr(0,nl));
    string ack;
    {
        lock_guard<mutex>lk(mstate);
        if(c.sync_id!=sync_conn||head.empty()){
            c.closing=true;
            return;
        }
        if(head[0]=="PING"){
            ack="PONG";
        }
        else if(head[0]=="PRESENCE"){
            clear_remote();
            for(size_t i=1;i+2<head.size();i+=3){
                apply_op({"online",head[i],head[i+1],head[i+2]});
            }
            ack="OK";
        }
        else if(head[0]=="SNAPSHOT"&&head.size()==3){
            long long seq;
            if(head[1]!=c.sync_epoch||!parse_num(head[2],seq)){
                c.closing=true;
                return;
            }
            apply_snapshot(nl==string::npos?"":msg.substr(nl+1));
            peer_applied=seq;
            peer_data_from.insert(c.sync_epoch);
            db_checkpoint();
            ack="ACK "+head[2];
        }
        else if(head[0]=="OP"&&head.size()>=3){
            long long seq;
            // a gap means something was lost: drop the link, the sender resumes after peer_applied
            if(!parse_num(head[1],seq)||seq>peer_applied+1){
                c.closing=true;
                return;
            }
            if(seq==peer_applied+1){
                vector<string> op(head.begin()+2,head.end());
                apply_op(op);
                peer_applied=seq;
                peer_data_from.insert(c.sync_epoch);
                db_write("R "+c.sync_epoch+" "+head[1]+" "+join(op));
            }
            // seq<=peer_applied: resent after a reconnect, already applied
            ack="ACK "+head[1];
        }
        else{
            c.closing=true;
            return;
        }
    }
    reply(c.fd,ST_OK,ack);
}

// sends this tracker's changes to the other tracker in order, each one until it is
// confirmed, and reconnects whenever the link breaks
void sync_sender(){
    while(trackerRunning){
        int sock=connect_peer();
        if(sock<0){
            sleep(1);
            continue;
        }
        set_timeout(sock,5);
        // reply: RESUME|NEW <last change of ours it applied> <its epoch>
        Msg r;
        vector<string> w;
        long long applied=0;
        if(!send_msg(sock,MSG_REQ,0,"SYNC_HELLO "+sync_secret+" "+my_epoch)||!recv_msg(sock,r)){
            close(sock);
            sleep(1);
            continue;
        }
        w=splits(r.payload);
        if(r.status!=ST_OK||w.size()!=3||!parse_num(w[1],applied)){
            close(sock);
            sleep(1);
            continue;
        }
        string first,presence;
        long long next;
        {
            lock_guard<mutex>lk(mstate);
            // outbox still holds every change since this tracker's state began
            bool complete=outbox.empty()?my_seq==0:outbox.front().first==1;
            // everything it hasn't applied is still in outbox
            bool canresume=outbox.empty()?applied==my_seq:applied+1>=outbox.front().first;
            // all data we got from the other tracker came from the state we're talking to
            bool onlyfromit=true;
            for(auto &e:peer_data_from){
                if(e!=w[2])
                    onlyfromit=false;
            }
            if(w[0]=="RESUME"&&canresume){
                next=applied+1;
            }
            else if(w[0]=="NEW"&&complete&&onlyfromit){
                // it already holds everything else we have, so our own changes are enough
                next=1;
            }
            else{
                // it may be missing data we hold (e.g. it lost its state while we were cut off): send all
                first="SNAPSHOT "+my_epoch+" "+to_string(my_seq)+"\n"+build_snapshot(true);
                next=my_seq+1;
            }
            if(!outbox.empty()&&outbox.front().first<next){
                while(!outbox.empty()&&outbox.front().first<next){
                    outbox.pop_front();
                }
                db_write("A "+to_string(next-1));
            }
            presence=build_presence();
        }
        bool ok=true;
        if(!first.empty()){
            ok=send_msg(sock,MSG_REQ,0,first)&&recv_msg(sock,r)&&r.status==ST_OK&&r.payload=="ACK "+to_string(next-1);
        }
        if(ok){
            ok=send_msg(sock,MSG_REQ,0,presence)&&recv_msg(sock,r)&&r.status==ST_OK;
        }
        out_link=ok;
        while(ok&&trackerRunning){
            long long seq=0;
            string op;
            {
                unique_lock<mutex>lk(mstate);
                outbox_cv.wait_for(lk,chrono::seconds(2),[]{ return !outbox.empty(); });
                if(!outbox.empty()){
                    seq=outbox.front().first;
                    op=outbox.front().second;
                }
            }
            if(seq==0){
                // nothing to send: ping, so both sides notice a dead link
                ok=send_msg(sock,MSG_REQ,0,"PING")&&recv_msg(sock,r)&&r.status==ST_OK;
                continue;
            }
            ok=send_msg(sock,MSG_REQ,0,"OP "+to_string(seq)+" "+op)&&recv_msg(sock,r)&&r.status==ST_OK&&r.payload=="ACK "+to_string(seq);
            if(ok){
                lock_guard<mutex>lk(mstate);
                while(!outbox.empty()&&outbox.front().first<=seq){
                    outbox.pop_front();
                }
                db_write("A "+to_string(seq));
            }
        }
        out_link=false;
        close(sock);
        sleep(1);
    }
}

// a tracker that starts without a state file first copies the other tracker's whole state
void bootstrap(){
    int sock=connect_peer();
    if(sock<0){
        cout<<"Other tracker not reachable, starting with empty state\n";
        return;
    }
    set_timeout(sock,10);
    Msg r;
    bool ok=false;
    if(send_msg(sock,MSG_REQ,0,"SNAPSHOT_REQ "+sync_secret)&&recv_msg(sock,r)&&r.status==ST_OK){
        // SNAPSHOT <epoch> <seq>\n<changes...>
        size_t nl=r.payload.find('\n');
        vector<string> head=splits(r.payload.substr(0,nl));
        long long seq;
        if(head.size()==3&&head[0]=="SNAPSHOT"&&parse_num(head[2],seq)){
            lock_guard<mutex>lk(mstate);
            apply_snapshot(nl==string::npos?"":r.payload.substr(nl+1));
            peer_epoch=head[1];
            peer_applied=seq;
            peer_data_from.insert(head[1]);
            // its sync link connects within a second or two; trust its logins until then
            peer_link_lost=time(nullptr);
            cout<<"Copied state from the other tracker: "<<login.size()<<" users, "<<groups.size()<<" groups\n";
            ok=true;
        }
    }
    if(!ok){
        cout<<"Could not copy state from the other tracker, starting with empty state\n";
    }
    close(sock);
}

//request handling

void handle_msg(Conn &c,Msg &m){
    if(m.type!=MSG_REQ){
        reply(c.fd,ST_BAD_REQUEST,"expected a request");
        c.closing=true;
        return;
    }
    if(c.sync){
        handle_sync(c,m.payload);
        return;
    }
    vector<string> ip=splits(m.payload);
    if(ip.empty()){
        reply(c.fd,ST_BAD_REQUEST,"invalid command");
        return;
    }
    int socketfd=c.fd;
    string user=c.user;
    string cmd=ip[0];
    //the other tracker
    if(cmd=="SYNC_HELLO"){
        sync_hello(c,ip);
    }
    else if(cmd=="SNAPSHOT_REQ"){
        send_snapshot(socketfd,ip);
        c.closing=true;
    }
    else if(cmd=="heartbeat"){
        // from now on, 15s of silence means this client is gone
        c.heartbeating=true;
        if(user.empty())
            reply(socketfd,ST_NOT_LOGGED_IN,"please login first");
        else
            reply(socketfd,ST_OK,"alive");
    }
    else if(cmd=="create_user"){
        if(ip.size()!=3){
            reply(socketfd,ST_BAD_REQUEST,"Incorrect usage");
            return;
        }
        create_user(ip,socketfd);
    }
    else if(cmd=="login"||cmd=="resume"){
        if(ip.size()!=5){
            reply(socketfd,ST_BAD_REQUEST,"Incorrect usage");
            return;
        }
        if(cmd=="login")
            loginuser(c,ip);
        else
            resume_session(c,ip);
    }
    else if(cmd=="list_groups"){
        if(ip.size()!=1){
            reply(socketfd,ST_BAD_REQUEST,"Incorrect usage");
            return;
        }
        list_groups(socketfd);
    }
    else if(cmd=="logout"){
        if(ip.size()!=1){
            reply(socketfd,ST_BAD_REQUEST,"Incorrect usage");
            return;
        }
        if(user.empty()){
            reply(socketfd,ST_NOT_LOGGED_IN,"No active session");
            return;
        }
        logout(c);
    }
    else{
        static const set<string> needlogin={"create_group","join_group","leave_group","list_requests","accept_request","upload_file","list_files","download_file","start_download","stop_share"};
        if(needlogin.count(cmd)==0){
            reply(socketfd,ST_UNKNOWN_CMD,"command not valid");
            return;
        }
        if(user.empty()){
            reply(socketfd,ST_NOT_LOGGED_IN,"please login first");
            return;
        }
        static const map<string,size_t> argc={{"create_group",2},{"join_group",2},{"leave_group",2},{"list_requests",2},{"accept_request",3}};
        auto a=argc.find(cmd);
        if(a!=argc.end()&&ip.size()!=a->second){
            reply(socketfd,ST_BAD_REQUEST,"Incorrect usage");
            return;
        }
        if(cmd=="create_group") create_group(ip,socketfd,user);
        else if(cmd=="join_group") join_group(ip,socketfd,user);
        else if(cmd=="leave_group") leave_group(ip,socketfd,user);
        else if(cmd=="list_requests") list_requests(ip,socketfd,user);
        else if(cmd=="accept_request") accept_request(ip,socketfd,user);
        else if(cmd=="upload_file") upload_file(ip,socketfd,user);
        else if(cmd=="list_files") list_files(ip,socketfd,user);
        else if(cmd=="download_file") download_file(ip,socketfd,user);
        else if(cmd=="start_download") start_download(ip,socketfd,user);
        else if(cmd=="stop_share") stop_share(ip,socketfd,user);
    }
}

//event loop: one thread polls every connection, a fixed pool of workers handles messages

mutex mjobs;
condition_variable jobs_cv;
deque<pair<shared_ptr<Conn>,Msg>> jobs;   // messages waiting for a worker
mutex mconn;
deque<shared_ptr<Conn>> returned;         // connections a worker has finished with
int wakefd[2]={-1,-1};                    // a byte here wakes the poll loop

void wake_loop(){
    char b=1;
    ssize_t n=write(wakefd[1],&b,1);
    (void)n;
}

void worker_thread(){
    while(true){
        pair<shared_ptr<Conn>,Msg> job;
        {
            unique_lock<mutex>lk(mjobs);
            jobs_cv.wait(lk,[]{ return !jobs.empty(); });
            job=std::move(jobs.front());
            jobs.pop_front();
        }
        try{
            handle_msg(*job.first,job.second);
        }
        catch(const exception &e){
            // a malformed request must only drop this connection, never the tracker
            cout<<"dropping connection after bad request: "<<e.what()<<"\n";
            job.first->closing=true;
        }
        {
            lock_guard<mutex>lk(mconn);
            returned.push_back(job.first);
        }
        wake_loop();
    }
}

// hand c's next whole message (if any) to a worker; false if c sent garbage
bool dispatch(const shared_ptr<Conn> &c){
    Msg m;
    int r=take_msg(c->inbuf,m);
    if(r<0)
        return false;
    if(r==1){
        c->busy=true;
        {
            lock_guard<mutex>lk(mjobs);
            jobs.push_back({c,std::move(m)});
        }
        jobs_cv.notify_one();
    }
    return true;
}

void close_conn(map<int,shared_ptr<Conn>> &conns,int fd){
    auto c=conns[fd];
    // client quit, crashed, or stopped sending heartbeats: it can't serve files any more
    if(!c->user.empty()){
        end_session(c->user);
    }
    if(c->sync){
        lock_guard<mutex>lk(mstate);
        // from now on we can't confirm who is online through the other tracker; its
        // logins stay trusted for REMOTE_GRACE seconds while its clients fail over here
        if(c->sync_id==sync_conn){
            peer_link=false;
            peer_link_lost=time(nullptr);
        }
    }
    close(fd);
    conns.erase(fd);
}

//server thread
void trackerserverthread() {
    int server_fd;
    struct sockaddr_in address;
    int opt=1;

    if((server_fd=socket(AF_INET,SOCK_STREAM,0))<0){
        perror("Socket Failed");
        exit(EXIT_FAILURE);
    }
    // socket options can't be OR'd together; SO_REUSEADDR alone allows quick restarts
    if(setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt))){
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family=AF_INET;
    address.sin_addr.s_addr=INADDR_ANY;
    uint16_t PORT=stoi(tracker_port);
    address.sin_port=htons(PORT);

    if(::bind(server_fd,(struct sockaddr *)&address,sizeof(address))<0){
        perror("Binding Failed");
        exit(EXIT_FAILURE);
    }
    if(listen(server_fd,10000)<0){
        perror("listen");
        exit(EXIT_FAILURE);
    }
    fcntl(server_fd,F_SETFL,fcntl(server_fd,F_GETFL,0)|O_NONBLOCK);
    for(int i=0;i<WORKERS;i++){
        thread(worker_thread).detach();
    }
    cout<<"Tracker is listening on "<<tracker_ip<<":"<<tracker_port<<"\n";

    map<int,shared_ptr<Conn>> conns;
    while(trackerRunning){
        // wait for: a new connection, data on an idle connection, or a worker finishing
        vector<pollfd> pfds;
        pfds.push_back({server_fd,POLLIN,0});
        pfds.push_back({wakefd[0],POLLIN,0});
        for(auto &kv:conns){
            if(!kv.second->busy)
                pfds.push_back({kv.first,POLLIN,0});
        }
        if(poll(pfds.data(),pfds.size(),1000)<0&&errno!=EINTR){
            perror("poll");
            break;
        }
        if(!trackerRunning)
            break;

        if(pfds[1].revents&POLLIN){
            char buf[256];
            while(read(wakefd[0],buf,sizeof(buf))>0){}
        }
        deque<shared_ptr<Conn>> back;
        {
            lock_guard<mutex>lk(mconn);
            back.swap(returned);
        }
        for(auto &c:back){
            c->busy=false;
            if(!dispatch(c)||(c->closing&&!c->busy))
                close_conn(conns,c->fd);
        }

        if(pfds[0].revents&POLLIN){
            while(true){
                int fd=accept(server_fd,nullptr,nullptr);
                if(fd<0)
                    break;
                fcntl(fd,F_SETFL,fcntl(fd,F_GETFL,0)|O_NONBLOCK);
                auto c=make_shared<Conn>();
                c->fd=fd;
                c->last=time(nullptr);
                conns[fd]=c;
            }
        }

        for(size_t i=2;i<pfds.size();i++){
            if(pfds[i].revents==0)
                continue;
            auto it=conns.find(pfds[i].fd);
            if(it==conns.end()||it->second->busy)
                continue;
            auto c=it->second;
            bool open=true;
            char buf[65536];
            while(true){
                ssize_t n=read(c->fd,buf,sizeof(buf));
                if(n>0){
                    c->inbuf.append(buf,n);
                    if(c->inbuf.size()>MAX_MSG+HDR_SIZE){
                        open=false;
                        break;
                    }
                    continue;
                }
                if(n<0&&errno==EINTR)
                    continue;
                if(n==0||(errno!=EAGAIN&&errno!=EWOULDBLOCK))
                    open=false;
                break;
            }
            c->last=time(nullptr);
            // the peer closed its side: still answer a request that arrived before that
            if(!open)
                c->closing=true;
            if(!dispatch(c)||(c->closing&&!c->busy))
                close_conn(conns,c->fd);
        }

        // connections that went silent: heartbeating clients, and the sync link
        time_t now=time(nullptr);
        vector<int> idle;
        for(auto &kv:conns){
            auto &c=kv.second;
            if(c->busy)
                continue;
            if((c->heartbeating&&now-c->last>HEARTBEAT_TIMEOUT)||(c->sync&&now-c->last>SYNC_TIMEOUT))
                idle.push_back(kv.first);
        }
        for(int fd:idle){
            close_conn(conns,fd);
        }
        open_conns=conns.size();
    }
    close(server_fd);
    cout << "Tracker stopped.\n";
}

//dashboard: optional read-only page at http://127.0.0.1:<web_port>/ (see dashboard.h)

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

// everything the dashboard shows, as JSON
string dashboard_state(){
    lock_guard<mutex>lk(mstate);
    time_t now=time(nullptr);
    long long down=peer_link||peer_link_lost==0?0:(long long)(now-peer_link_lost);
    string j="{\"tracker\":"+to_string(tracker_no)+",\"addr\":"+json_str(tracker_ip+":"+tracker_port)+
        ",\"peer\":"+json_str(p_tracker_ip.empty()?"":p_tracker_ip+":"+p_tracker_port)+
        ",\"sending\":"+(out_link?"true":"false")+",\"receiving\":"+(peer_link?"true":"false")+",\"link_down_for\":"+to_string(down)+
        ",\"changes\":"+to_string(my_seq)+",\"unconfirmed\":"+to_string(outbox.size())+",\"peer_applied\":"+to_string(peer_applied)+
        ",\"connections\":"+to_string(open_conns.load())+",\"users\":[";
    bool first=true;
    for(auto &u:login){
        j+=string(first?"":",")+"{\"name\":"+json_str(u.first)+",\"online\":"+(is_online(u.second)?"true":"false")+
            ",\"here\":"+(u.second.sessions>0?"true":"false")+",\"sessions\":"+to_string(u.second.sessions)+
            ",\"addr\":"+json_str(u.second.peerserverip+":"+u.second.peerserverport)+"}";
        first=false;
    }
    j+="],\"groups\":[";
    first=true;
    for(auto &g:groups){
        j+=string(first?"":",")+"{\"id\":"+json_str(g.first)+",\"owner\":"+json_str(g.second.grpowner)+
            ",\"members\":"+json_list(g.second.grpmem)+",\"requests\":"+json_list(g.second.req)+"}";
        first=false;
    }
    j+="],\"files\":[";
    first=true;
    auto peers=[](const map<string,pair<string,string>> &m){
        string o="[";
        for(auto &p:m){
            auto it=login.find(p.first);
            bool on=it!=login.end()&&is_online(it->second);
            o+=string(o.size()>1?",":"")+"{\"user\":"+json_str(p.first)+",\"online\":"+(on?"true":"false")+"}";
        }
        return o+"]";
    };
    for(auto &gf:group_files){
        for(auto &f:gf.second){
            j+=string(first?"":",")+"{\"group\":"+json_str(gf.first)+",\"name\":"+json_str(f.first)+
                ",\"size\":"+to_string(f.second.fsize)+",\"pieces\":"+to_string(f.second.phashes.size())+
                ",\"seeders\":"+peers(f.second.seeders)+",\"partial\":"+peers(f.second.leechers)+"}";
            first=false;
        }
    }
    return j+"]}";
}

void send_http(int s,int code,const string &type,const string &body){
    string reason=code==200?"OK":code==404?"Not Found":"Bad Request";
    string all="HTTP/1.1 "+to_string(code)+" "+reason+"\r\nContent-Type: "+type+"\r\nContent-Length: "+to_string(body.size())+
        "\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n"+body;
    send_all(s,all.data(),all.size());
}

// one GET request per connection: the page, or its data
void dashboard_handler(int s){
    set_timeout(s,10);
    string data;
    char buf[4096];
    while(data.find("\r\n\r\n")==string::npos&&data.size()<65536){
        ssize_t n=recv(s,buf,sizeof(buf),0);
        if(n<=0){
            close(s);
            return;
        }
        data.append(buf,n);
    }
    vector<string> first=splits(data.substr(0,data.find("\r\n")));
    string path=first.size()>=2?first[1].substr(0,first[1].find('?')):"";
    if(first.empty()||first[0]!="GET")
        send_http(s,400,"text/plain","only GET");
    else if(path=="/")
        send_http(s,200,"text/html; charset=utf-8",DASHBOARD_PAGE);
    else if(path=="/api/state")
        send_http(s,200,"application/json",dashboard_state());
    else
        send_http(s,404,"text/plain","not found");
    close(s);
}

// only on 127.0.0.1: the dashboard shows user names and addresses
void dashboard_server(int port){
    int sock=socket(AF_INET,SOCK_STREAM,0);
    int opt=1;
    setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons(port);
    sa.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    if(sock<0||::bind(sock,(sockaddr*)&sa,sizeof(sa))<0||listen(sock,16)<0){
        perror("dashboard port");
        return;
    }
    cout<<"Dashboard: http://127.0.0.1:"<<port<<"/"<<endl;
    while(trackerRunning){
        int cs=accept(sock,nullptr,nullptr);
        if(cs<0)
            continue;
        thread(dashboard_handler,cs).detach();
    }
}

void trackerdetails(char const *argv[]) {
    long long no;
    if(!parse_num(argv[2],no)||(no!=1&&no!=2)){
        cerr<<"tracker_no must be 1 or 2\n";
        exit(1);
    }
    tracker_no=(int)no;

    int fd=open(argv[1],O_RDONLY);
    if(fd<0) {
        perror("Could not open tracker details");
        exit(1);
    }
    char buff[4096];
    ssize_t readbytes=read(fd,buff,sizeof(buff)-1);
    if(readbytes<0) {
        perror("Error reading the tracker file");
        close(fd);
        exit(1);
    }
    buff[readbytes]='\0';
    close(fd);

    vector<pair<string, string>> ts;
    char *line=strtok(buff,"\n");
    while(line!=NULL){
        char ip[256],port[256];
        if(sscanf(line,"%255s %255s",ip,port)==2) {
            ts.push_back({ip,port});
        }
        line=strtok(NULL, "\n");
    }
    if(tracker_no==1&&ts.size()>=1) {
        tracker_ip=ts[0].first;
        tracker_port=ts[0].second;
        if(ts.size()>=2){
            p_tracker_ip=ts[1].first;
            p_tracker_port=ts[1].second;
        }
    }
    else if(tracker_no==2&&ts.size()>=2){
        tracker_ip=ts[1].first;
        tracker_port=ts[1].second;
        p_tracker_ip=ts[0].first;
        p_tracker_port=ts[0].second;
    }
    else{
        cerr<<"Tracker information invalid"<<endl;
        exit(1);
    }
    if(!valid_port(tracker_port)||(!p_tracker_port.empty()&&!valid_port(p_tracker_port))){
        cerr<<"Invalid port in tracker information file"<<endl;
        exit(1);
    }
    cout<<tracker_ip<<" "<<tracker_port<<endl;
}

void tracker_console() {
    string line;
    while(true){
        cout<<"$> ";
        if(!getline(cin,line)) break;
        if(line=="quit") {
            cout<<"Shutting down tracker...\n";
            trackerRunning=false;
            wake_loop();
            break;
        }
        else {
            cout<<"Unknown command: "<<line<<"\n";
        }
    }
}


int main(int argc,char const *argv[]) {
    if(argc<3){
        cerr<<"use: ./tracker <tracker_info.txt> <tracker_no> [web_port]\n";
        return 1;
    }
    if(argc>=4&&!valid_port(argv[3])){
        cerr<<"web_port must be a port number\n";
        return 1;
    }
    // writing to a socket whose peer has gone must return an error, not kill the tracker
    signal(SIGPIPE,SIG_IGN);
    trackerdetails(argv);
    const char *secret=getenv("TRACKER_SECRET");
    sync_secret=secret?secret:"aos-p2p-tracker-sync";
    if(pipe(wakefd)<0){
        perror("pipe");
        return 1;
    }
    fcntl(wakefd[0],F_SETFL,O_NONBLOCK);
    fcntl(wakefd[1],F_SETFL,O_NONBLOCK);

    // state survives restarts in tracker<N>.state (in the working directory)
    db_path="tracker"+to_string(tracker_no)+".state";
    bool loaded;
    {
        lock_guard<mutex>lk(mstate);
        loaded=db_load();
        if(loaded)
            cout<<"Recovered state from "<<db_path<<": "<<login.size()<<" users, "<<groups.size()<<" groups\n";
        else
            my_epoch=random_hex(12);
    }
    if(!loaded&&!p_tracker_ip.empty()){
        bootstrap();
    }
    {
        lock_guard<mutex>lk(mstate);
        db_checkpoint();
    }
    thread serverThread(trackerserverthread);
    if(!p_tracker_ip.empty()){
        thread(sync_sender).detach();
    }
    // optional read-only dashboard on 127.0.0.1:<web_port>
    if(argc>=4){
        thread(dashboard_server,stoi(argv[3])).detach();
    }
    tracker_console();
    serverThread.join();
    return 0;
}
