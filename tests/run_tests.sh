#!/usr/bin/env bash
# End-to-end tests: starts two trackers and up to three clients on this machine, breaks
# things on purpose (kills, freezes, corrupted files, garbage messages) and checks results.
#
#   tests/run_tests.sh            builds tracker, client and tests/p2pmsg, then runs everything
#   KEEP=1 tests/run_tests.sh     keeps the working directory (logs, state files) afterwards
#
#   tests/run_tests.sh liveness   runs one section (basic swarm restart trackers liveness misc web)
#
# Needs bash, make, g++, curl and OpenSSL. Takes about two minutes.
# Exit status is the number of failed checks.

set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TR=$ROOT/tracker/tracker
CL=$ROOT/client/client
MSG=$ROOT/tests/p2pmsg

echo "Building..."
make -s -C "$ROOT/tracker" >/dev/null && make -s -C "$ROOT/client" >/dev/null &&
    g++ -std=c++17 -O2 -o "$MSG" "$ROOT/tests/p2pmsg.cpp" || { echo "build failed"; exit 1; }

W=$(mktemp -d "${TMPDIR:-/tmp}/p2ptest.XXXXXX")
PORT=$((20000 + RANDOM % 20000))
PASS=0
FAIL=0
PID_t1= PID_t2= PID_a= PID_b= PID_c=

stop_all(){
    for v in a b c; do
        eval "p=\$PID_$v"
        if [ -n "$p" ]; then kill_client $v; fi
    done
    for n in 1 2; do
        eval "p=\$PID_t$n"
        if [ -n "$p" ]; then kill_tracker $n; fi
    done
}

cleanup(){
    stop_all
    if [ "${KEEP:-0}" = 1 ]; then echo "Kept $W"; else rm -rf "$W"; fi
}
trap cleanup EXIT

pass(){ echo "  PASS  $1"; PASS=$((PASS+1)); }
fail(){ echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
check(){ local d=$1; shift; if "$@"; then pass "$d"; else fail "$d"; fi; }

mk(){ head -c "$2" /dev/urandom > "$1"; }

# a fresh set of ports and directories for one group of tests
new_world(){
    stop_all
    WD=$W/$1
    mkdir -p "$WD"
    TP1=$PORT; TP2=$((PORT+1)); CP_a=$((PORT+2)); CP_b=$((PORT+3)); CP_c=$((PORT+4))
    PORT=$((PORT+10))
    printf "127.0.0.1 %s\n127.0.0.1 %s\n" $TP1 $TP2 > "$WD/ti.txt"
    echo
    echo "== $2"
}

# start_tracker <1|2> [VAR=value] [dashboard port]: runs in $WD/t<n>, where its state file lives
start_tracker(){
    local n=$1 dir=$WD/t$1 port
    mkdir -p "$dir"
    (cd "$dir" && exec env ${2:-} "$TR" ../ti.txt "$n" ${3:-} </dev/null >>out 2>&1) &
    eval "PID_t$n=$!"
    eval "port=\$TP$n"
    local end=$((SECONDS+10))
    while [ $SECONDS -le $end ]; do
        "$MSG" $port list_groups >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    return 1
}

kill_tracker(){
    local p
    eval "p=\$PID_t$1"
    kill -CONT $p 2>/dev/null; kill -9 $p 2>/dev/null; wait $p 2>/dev/null
    eval "PID_t$1="
}

fd_of(){ case $1 in a) echo 3;; b) echo 4;; c) echo 5;; esac; }

# start_client <a|b|c> [VAR=value] [web port]: runs in $WD/<name>; commands go in through a FIFO
start_client(){
    local name=$1 dir=$WD/$1 fd port
    fd=$(fd_of $name)
    eval "port=\$CP_$name"
    mkdir -p "$dir"
    rm -f "$dir/in"
    mkfifo "$dir/in"
    (cd "$dir" && exec env ${2:-} "$CL" 127.0.0.1:$port ../ti.txt ${3:-} <in >>out 2>&1) &
    eval "PID_$name=$!"
    eval "exec $fd>\"$dir/in\""
}

kill_client(){
    local fd p
    fd=$(fd_of $1)
    eval "p=\$PID_$1"
    eval "exec $fd>&-"
    kill -CONT $p 2>/dev/null; kill -9 $p 2>/dev/null; wait $p 2>/dev/null
    eval "PID_$1="
}

# cmd <client> <command...>
cmd(){ local fd; fd=$(fd_of $1); shift; echo "$*" >&$fd; }

# lines <client>: how much it has printed so far
lines(){ wc -l < "$WD/$1/out" | tr -d ' '; }

# wait_out <client> <pattern> <after line> [seconds]: wait for new output matching pattern
wait_out(){
    local end=$((SECONDS+${4:-10}))
    while [ $SECONDS -le $end ]; do
        tail -n +$(($3+1)) "$WD/$1/out" | grep -q -- "$2" && return 0
        sleep 0.2
    done
    return 1
}

# wait_n <file> <pattern> <count> [seconds]: wait until pattern appears count times
wait_n(){
    local end=$((SECONDS+${4:-10}))
    while [ $SECONDS -le $end ]; do
        [ "$(grep -c -- "$2" "$1" 2>/dev/null)" -ge "$3" ] && return 0
        sleep 0.2
    done
    return 1
}

# peers <tracker port> <file>: the peers the tracker hands out for g/<file>
peers(){ "$MSG" $1 "login v p 127.0.0.1 9" "download_file g $2" | tail -1 | sed 's/.* PEERS//'; }

# pieces <client> <from client> <file>: pieces of <file> a client got from another
pieces(){
    local port
    eval "port=\$CP_$2"
    grep "Pieces of $3 by peer" "$WD/$1/cl.log" | tail -1 | grep -o "127.0.0.1:$port=[0-9]*" | cut -d= -f2
}

threads(){
    if [ -d /proc/$1/task ]; then ls /proc/$1/task | wc -l; else ps -M -p $1 | tail -n +2 | wc -l; fi | tr -d ' '
}

# a group g owned by a, with b, c and the checking user v as members
setup_group(){
    "$MSG" $TP1 "create_user a p" "create_user b p" "create_user c p" "create_user v p" >/dev/null
    for x in "$@"; do cmd $x login $x p; done
    cmd a create_group g
    sleep 0.5
    for x in "$@"; do [ $x != a ] && cmd $x join_group g; done
    "$MSG" $TP1 "login v p 127.0.0.1 9" "join_group g" >/dev/null
    sleep 0.5
    for x in "$@"; do [ $x != a ] && cmd a accept_request g $x; done
    cmd a accept_request g v
    sleep 0.5
}

test_basic(){
    new_world basic "Basic flow, status codes, bad input"
    start_tracker 1; start_tracker 2
    mk "$WD/small.bin" 300000; mk "$WD/medium.bin" 12000000
    start_client a; start_client b; start_client c
    setup_group a b c
    cmd a upload_file g "$WD/small.bin"; cmd a upload_file g "$WD/medium.bin"
    wait_n "$WD/a/out" "file uploaded" 2 10
    mkdir -p "$WD/b/dl"
    cmd b download_file g small.bin "$WD/b/dl"; cmd b download_file g medium.bin "$WD/b/dl"
    check "two downloads at once (300 KB and 12 MB) finish" wait_n "$WD/b/out" "File downloaded successfully" 2 30
    check "the 300 KB file is identical to the original" cmp -s "$WD/small.bin" "$WD/b/dl/small.bin"
    check "the 12 MB file is identical to the original" cmp -s "$WD/medium.bin" "$WD/b/dl/medium.bin"
    cmd c download_file g medium.bin "$WD/c/medium.bin"
    check "a third client downloads the file" wait_n "$WD/c/out" "File downloaded successfully" 1 30
    sleep 0.5
    check "it took pieces from both A and B (multi-peer)" [ "$(pieces c a medium.bin)" -gt 0 -a "$(pieces c b medium.bin)" -gt 0 ]
    codes=$("$MSG" $TP1 "create_user a p" "login a wrong 127.0.0.1 9" "login v p 127.0.0.1 9" "join_group nosuch" "accept_request g v" "frobnicate" | cut -c1 | tr -d '\n')
    check "status codes: exists=4, auth=3, ok=0, not found=5, forbidden=6, unknown=9" [ "$codes" = "430569" ]
    mkdir -p "$WD/b/alt"; mk "$WD/b/alt/medium.bin" 1000
    m=$(lines b); cmd b upload_file g "$WD/b/alt/medium.bin"
    check "a different file under an existing name is refused" wait_out b "already shared" $m 5
    "$MSG" $TP1 "raw:this is not a framed message" >/dev/null
    "$MSG" $CP_a "raw:junk" >/dev/null
    p1=$("$MSG" $CP_a "GETPIECE g medium.bin abc"); p2=$("$MSG" $CP_a "GETPIECE g medium.bin 99999"); p3=$("$MSG" $CP_a "GETPIECE g nothere 0")
    check "a seeder answers malformed piece requests with error codes" [ "${p1%% *}${p2%% *}${p3%% *}" = "155" ]
    check "tracker and seeder are still running after garbage input" kill -0 $PID_t1 $PID_a
}

test_swarm(){
    new_world swarm "Partial seeding, rarest-first, dropping bad peers"
    start_tracker 1; start_tracker 2
    mk "$WD/big.bin" 40000000
    start_client a P2P_TEST_DELAY_MS=60; start_client b; start_client c
    setup_group a b c
    cmd a upload_file g "$WD/big.bin"
    wait_n "$WD/a/out" "file uploaded" 1 10
    cmd b download_file g big.bin "$WD/b/big.bin"
    sleep 1.5
    m=$(lines b); cmd b show_downloads
    wait_out b "\[D\] g big.bin" $m 3 && b_partial=1 || b_partial=0
    cmd c download_file g big.bin "$WD/c/big.bin"
    check "B finishes" wait_n "$WD/b/out" "File downloaded successfully" 1 40
    check "C finishes" wait_n "$WD/c/out" "File downloaded successfully" 1 40
    check "B's copy is identical to the original" cmp -s "$WD/big.bin" "$WD/b/big.bin"
    check "C's copy is identical to the original" cmp -s "$WD/big.bin" "$WD/c/big.bin"
    sleep 0.5
    fromb=$(pieces c b big.bin)
    check "C got pieces from B while B was still downloading (partial seeding)" [ $b_partial = 1 -a "${fromb:-0}" -gt 0 ]
    froma=$(( $(pieces b a big.bin) + $(pieces c a big.bin) ))
    check "A uploaded $froma pieces for two 77-piece downloads (154 without partial seeding)" [ $froma -lt 154 ]
    mk "$WD/b/big.bin" 40000000   # B's copy is now garbage, but B still offers it
    cmd c download_file g big.bin "$WD/c/again.bin"
    check "a download with a corrupting peer still finishes" wait_n "$WD/c/out" "File downloaded successfully" 2 40
    check "the corrupting peer is dropped after its first bad piece" grep -q "127.0.0.1:$CP_b; no longer using this peer" "$WD/c/cl.log"
    check "the result is still identical to the original" cmp -s "$WD/big.bin" "$WD/c/again.bin"
}

test_restart(){
    new_world restart "Resumable downloads, re-sharing after a client restart"
    start_tracker 1; start_tracker 2
    mk "$WD/big.bin" 40000000; mk "$WD/only.bin" 2000000
    start_client a P2P_TEST_DELAY_MS=60; start_client b
    setup_group a b
    cmd a upload_file g "$WD/big.bin"; cmd a upload_file g "$WD/only.bin"
    wait_n "$WD/a/out" "file uploaded" 2 10
    cmd b download_file g big.bin "$WD/b/big.bin"
    sleep 2.5
    kill_client b
    check "a killed download leaves its .part file" [ -f "$WD/b/big.bin.part" ]
    start_client b
    m=$(lines b); cmd b login b p
    check "after logging in again, the download resumes by itself" wait_out b "resumed 1 downloads" $m 10
    check "and finishes" wait_out b "File downloaded successfully" $m 40
    check "the result is identical to the original" cmp -s "$WD/big.bin" "$WD/b/big.bin"
    check "pieces already downloaded were kept" grep -q "Resuming download of big.bin: [1-9][0-9]*/77" "$WD/b/cl.log"
    check "the .part file is removed once complete" [ ! -f "$WD/b/big.bin.part" ]
    kill_client a
    start_client a
    m=$(lines a); cmd a login a p
    check "a restarted seeder shares its files again after login" wait_out a "Re-shared 2 files" $m 10
    start_client c
    cmd c login c p; cmd c join_group g; sleep 0.5; cmd a accept_request g c; sleep 0.5
    cmd c download_file g only.bin "$WD/c/only.bin"
    check "a file only the restarted seeder has can be downloaded" wait_n "$WD/c/out" "File downloaded successfully" 1 20
    check "and is identical" cmp -s "$WD/only.bin" "$WD/c/only.bin"
}

test_trackers(){
    new_world trackers "Tracker failover, restarts, catch-up, conflicts"
    start_tracker 1; start_tracker 2
    mk "$WD/f.bin" 2000000
    start_client a; start_client b
    setup_group a b
    cmd a upload_file g "$WD/f.bin"
    wait_n "$WD/a/out" "file uploaded" 1 10
    kill_tracker 1
    sleep 8
    check "an idle seeder is still listed after tracker 1 dies (heartbeat failover)" sh -c "echo '$(peers $TP2 f.bin)' | grep -q ' a 127.0.0.1 $CP_a C'"
    cmd b download_file g f.bin "$WD/b/f.bin"
    check "downloads keep working through tracker 2" wait_n "$WD/b/out" "File downloaded successfully" 1 20
    kill_tracker 2
    start_tracker 1; start_tracker 2
    check "both trackers restart from their state files" sh -c "grep -q 'Recovered state' '$WD/t1/out' && grep -q 'Recovered state' '$WD/t2/out'"
    check "users and groups survived both trackers going down" sh -c "'$MSG' $TP1 'login v p 127.0.0.1 9' list_groups | tail -1 | grep -q '^0 g'"
    sleep 8
    check "clients continue their sessions after the restart (token, no password)" sh -c "echo '$(peers $TP1 f.bin)' | grep -q ' a 127.0.0.1 $CP_a C'"
    kill -STOP $PID_t2
    "$MSG" $TP1 "login v p 127.0.0.1 9" "create_group g9" "leave_group g" >/dev/null
    kill -CONT $PID_t2
    sleep 4
    kill_tracker 1
    out=$("$MSG" $TP2 "login v p 127.0.0.1 9" "list_groups" "list_files g")
    check "a frozen tracker catches up on what it missed, deletions included" sh -c "echo '$out' | sed -n 2p | grep -q g9 && echo '$out' | sed -n 3p | grep -q '^6'"
    start_tracker 1
    kill_tracker 2
    rm -f "$WD/t2/tracker2.state"
    start_tracker 2
    check "a tracker that lost its state file copies the other's state" grep -q "Copied state from the other tracker" "$WD/t2/out"
    kill_tracker 2
    start_tracker 2 TRACKER_SECRET=wrong   # cut off: the two trackers reject each other
    "$MSG" $TP1 "create_user x one" >/dev/null
    "$MSG" $TP2 "create_user x two" >/dev/null
    kill_tracker 2
    start_tracker 2
    sleep 4
    r=$("$MSG" $TP2 "login x one 127.0.0.1 9" | cut -c1)$("$MSG" $TP2 "login x two 127.0.0.1 9" | cut -c1)$("$MSG" $TP1 "login x one 127.0.0.1 9" | cut -c1)
    check "a name signed up on both while cut off ends as tracker 1's account on both" [ "$r" = "030" ]
}

test_liveness(){
    new_world liveness "Heartbeats, frozen and crashed clients, stalled seeders"
    start_tracker 1; start_tracker 2
    mk "$WD/f.bin" 4000000
    start_client a; start_client b; start_client c
    setup_group a b c
    cmd a upload_file g "$WD/f.bin"
    wait_n "$WD/a/out" "file uploaded" 1 10
    cmd b download_file g f.bin "$WD/b/f.bin"
    wait_n "$WD/b/out" "File downloaded successfully" 1 20
    sleep 0.5
    kill -STOP $PID_b
    sleep 18
    check "a frozen client drops off the peer list after 15 s" sh -c "! echo '$(peers $TP1 f.bin)' | grep -q ' b '"
    kill -CONT $PID_b
    sleep 7
    check "it is listed again once it resumes" sh -c "echo '$(peers $TP1 f.bin)' | grep -q ' b '"
    kill -STOP $PID_b
    start=$SECONDS
    cmd c download_file g f.bin "$WD/c/f.bin"
    check "a download finishes from A while seeder B is frozen" wait_n "$WD/c/out" "File downloaded successfully" 1 30
    check "it took $((SECONDS-start)) s (stalled seeder timed out, not hung)" [ $((SECONDS-start)) -le 20 ]
    check "and is identical" cmp -s "$WD/f.bin" "$WD/c/f.bin"
    kill -CONT $PID_b
    kill_client b
    sleep 1
    check "a crashed client disappears from the peer list at once" sh -c "! echo '$(peers $TP1 f.bin)' | grep -q ' b '"
}

test_misc(){
    new_world misc "Security and the tracker's event loop"
    start_tracker 1; start_tracker 2
    "$MSG" $TP1 "create_user s Secr3t-Passw0rd" >/dev/null
    sleep 1
    check "state files hold no plaintext passwords" sh -c "! grep -q Secr3t-Passw0rd '$WD/t1/tracker1.state' '$WD/t2/tracker2.state'"
    check "the password works on both trackers" sh -c "'$MSG' $TP2 'login s Secr3t-Passw0rd 127.0.0.1 9' | grep -q '^0'"
    check "sync messages need the shared secret" sh -c "'$MSG' $TP1 'SYNC_HELLO wrong abc' | grep -q '^6'"
    before=$(threads $PID_t1)
    "$MSG" $TP1 --hold 300 6 >/dev/null &
    H=$!
    sleep 3
    during=$(threads $PID_t1)
    wait $H
    check "300 extra connections cost no extra threads ($before threads before, $during with them)" [ "$during" -le "$before" ]
}

test_web(){
    new_world web "Web UI and tracker dashboard"
    local wp=$PORT dp=$((PORT+1))
    PORT=$((PORT+10))
    start_tracker 1 "" $dp; start_tracker 2
    start_client a "" $wp
    wait_n "$WD/a/out" "Web UI: http" 1 5
    local tok base=http://127.0.0.1:$wp
    tok=$(grep -o 't=[0-9a-f]*' "$WD/a/out" | head -1 | cut -c3-)
    check "the client serves its page" sh -c "curl -s -o /dev/null -w '%{http_code}' $base/ | grep -q 200"
    check "the API refuses calls without the access token" sh -c "curl -s -o /dev/null -w '%{http_code}' $base/api/state | grep -q 403"
    local out
    out=$(curl -s -X POST -H "X-P2P-Token: $tok" --data-binary "create_user w p" $base/api/cmd; curl -s -X POST -H "X-P2P-Token: $tok" --data-binary "login w p" $base/api/cmd)
    check "commands run through the API as in the terminal" sh -c "echo '$out' | grep -q 'user created' && echo '$out' | grep -q 'login successfull'"
    check "the API state shows the logged-in user" sh -c "curl -s -H 'X-P2P-Token: $tok' $base/api/state | grep -q '\"user\":\"w\"'"
    check "the tracker dashboard shows the user online" sh -c "curl -s http://127.0.0.1:$dp/api/state | grep -q '\"name\":\"w\",\"online\":true'"
}

# tests/run_tests.sh [section...]: run some sections only, e.g. tests/run_tests.sh liveness
for t in ${@:-basic swarm restart trackers liveness misc web}; do
    test_$t
done
stop_all

echo
echo "$PASS passed, $FAIL failed"
exit $FAIL
