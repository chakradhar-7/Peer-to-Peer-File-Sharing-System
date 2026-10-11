// p2pmsg: test tool. Sends requests to a tracker or a client and prints each reply as
// "<status> <text>" (or "<status> <N bytes>" for piece data).
//
//   p2pmsg <port> <request>...       all requests go on one connection, in order
//                                    "raw:<text>" sends <text> as plain bytes, without a header
//   p2pmsg <port> --hold <n> <secs>  opens n idle connections and keeps them open for secs
#include<cstdio>
#include<cstdlib>
#include<cstring>
#include<string>
#include<vector>
#include<unistd.h>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<sys/socket.h>
#include<sys/time.h>
using namespace std;

static int conn(int port){
    int s=socket(AF_INET,SOCK_STREAM,0);
    if(s<0)
        return -1;
    struct timeval tv;
    tv.tv_sec=5;
    tv.tv_usec=0;
    setsockopt(s,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    sockaddr_in sa{};
    sa.sin_family=AF_INET;
    sa.sin_port=htons(port);
    inet_pton(AF_INET,"127.0.0.1",&sa.sin_addr);
    if(connect(s,(sockaddr*)&sa,sizeof(sa))<0){
        close(s);
        return -1;
    }
    return s;
}

static bool recv_all(int s,char *b,size_t n){
    size_t got=0;
    while(got<n){
        ssize_t r=recv(s,b+got,n-got,0);
        if(r<=0)
            return false;
        got+=r;
    }
    return true;
}

int main(int argc,char **argv){
    if(argc<3){
        fprintf(stderr,"usage: p2pmsg <port> <request>... | p2pmsg <port> --hold <n> <secs>\n");
        return 2;
    }
    int port=atoi(argv[1]);
    if(string(argv[2])=="--hold"&&argc==5){
        vector<int> held;
        for(int i=0;i<atoi(argv[3]);i++){
            int s=conn(port);
            if(s>=0)
                held.push_back(s);
        }
        printf("holding %zu connections\n",held.size());
        fflush(stdout);
        sleep(atoi(argv[4]));
        return 0;
    }
    int s=conn(port);
    if(s<0){
        printf("connect failed\n");
        return 1;
    }
    for(int i=2;i<argc;i++){
        string a=argv[i],out;
        if(a.rfind("raw:",0)==0){
            out=a.substr(4);
        }
        else{
            // header: type 1 (request), status 0, payload length
            char h[6];
            h[0]=1;
            h[1]=0;
            uint32_t n=htonl(a.size());
            memcpy(h+2,&n,4);
            out=string(h,6)+a;
        }
        if(send(s,out.data(),out.size(),0)<0){
            printf("closed\n");
            return 0;
        }
        char h[6];
        if(!recv_all(s,h,6)){
            printf("closed\n");
            return 0;
        }
        uint32_t n;
        memcpy(&n,h+2,4);
        n=ntohl(n);
        string p(n,'\0');
        if(n>0&&!recv_all(s,&p[0],n)){
            printf("closed\n");
            return 0;
        }
        if(h[0]==3)
            printf("%d <%u bytes>\n",(unsigned char)h[1],n);
        else
            printf("%d %s\n",(unsigned char)h[1],p.c_str());
        fflush(stdout);
    }
    close(s);
    return 0;
}
