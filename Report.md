# Peer-to-Peer Distributed File Sharing System: Design Report

**Name:** Kotha Chakradhar
**Roll No:** 2025201053
**Course:** Advanced Operating Systems, Assignment 3 (Monsoon 2025)

Build and run instructions, the full message reference and the test suite are in `README.md`. This report explains the design choices behind them.

---

## 1. Implementation approach

The system has two programs, written in C++17 using only POSIX sockets, POSIX threads and OpenSSL.

**Tracker.** Each tracker keeps all metadata in memory and in a state file:
- users, with salted password hashes and session-token hashes;
- groups, with members and join requests;
- for every shared file: its size, its SHA-1 hashes, and who has it (complete *seeders*, and partial *leechers*).

Every state change goes through one function, `apply_op`, under one mutex. The same change is queued for the other tracker and appended to the state file under that lock.

Connections are served by an **event loop**: one thread `poll()`s every socket and hands each complete message to a pool of 8 worker threads. A connection therefore costs a file descriptor, not a thread. With 300 extra idle connections open, the tracker stayed at 11 threads.

**Client.** A client runs:
- the command shell;
- a peer server, one thread per connected downloader;
- a heartbeat thread;
- for each download, a coordinator plus one worker per peer.

**Uploading.** The client reads the file once and computes both the SHA-1 of every 512 KB piece and the SHA-1 of the whole file. It sends only this metadata to the tracker.

**Downloading.**
1. The client registers with the tracker as a partial peer for the file.
2. It opens one persistent connection to each peer and keeps up to 4 piece requests outstanding on it.
3. It verifies every piece, writes it at its offset with `pwrite`, and immediately serves it to other peers.
4. Once the whole file's hash matches, it becomes a full seeder.

Which pieces are done is saved to `<destination>.part`, so downloads resume after a crash. The user's shared files are saved too, so a restarted client shares them again at login.

---

## 2. Synchronization algorithm

**Goal.** A client must see the same users, groups and files whichever tracker it talks to. The system must keep working with one tracker, and a tracker that comes back must catch up.

### 2.1 An ordered, acknowledged change stream
- **One sequence of changes.** Every change made on a tracker gets the next sequence number in that tracker's `outbox`. This happens under the same lock that applies the change, so the outbox order is exactly the order changes took effect.
- **One link per direction.** A `sync_sender` thread keeps one TCP link to the other tracker and sends changes in order as `OP <seq> <change>`. Each change stays in the outbox until the receiver replies `ACK <seq>`.
- **The receiver enforces order.** It applies a change only if its number is exactly one more than the last change it applied. It acknowledges repeats without applying them again. A gap closes the link, and the sender then resumes from the right place.
- **Changes, not commands.** What is sent is the change itself, such as `join_group g u`. The receiver applies it with the same `apply_op` the sender used, which ignores changes that no longer fit.

### 2.2 Epochs, catching up, and recovery
Each tracker's state has a random id, its *epoch*. A sync link opens with `SYNC_HELLO <secret> <epoch>`, and the receiver answers in one of two ways:
- **`RESUME n`:** the receiver knows this state and has applied its changes up to `n`. The sender continues from `n+1` with changes still in its outbox. This covers a dropped link, a tracker that was frozen (Ctrl+Z) or cut off, and a tracker restarted from its state file (§2.3). Deletions made in the meantime are replayed in order, so nothing deleted comes back.
- **`NEW`:** the receiver hasn't seen this state. If the sender's outbox still holds every change since its state began, and all its other data came from the receiver, the sender sends its outbox from change 1. Otherwise it first sends a full `SNAPSHOT`, which the receiver merges.

### 2.3 Durable state
Every change, every confirmation and the sync bookkeeping are appended to `tracker<N>.state`. At startup, and every 20,000 lines, the file is rewritten as a single checkpoint: a temporary file, `fsync`, then `rename`, so a crash mid-write leaves the old file intact.

A restarted tracker replays this file and keeps its epoch, so to the other tracker it looks like a tracker that was briefly unreachable. Both directions resume exactly where they stopped, including changes that hadn't been confirmed yet. Only live sessions are not restored: clients log in again automatically with their tokens (§4).

A tracker with no state file copies the other tracker's whole state (`SNAPSHOT_REQ`) before accepting clients.

Tested:
- Both trackers were killed and restarted. Users, groups and files were all there, and clients continued their sessions without sending their passwords again.
- A tracker whose state file was deleted copied the other's state.

### 2.4 Conflicts
Both trackers accept changes, so if they are cut off from each other, both can create the same user name, group id or file name. Each such record carries the number of the tracker it was created on, and on a clash **tracker 1's version wins, on both trackers**:
- **Users:** the losing account's credentials are replaced.
- **Groups:** the losing creator stays a member.
- **Files:** seeders of the losing file version are dropped.

Tested: the trackers were cut off with mismatched secrets, `x` was created with different passwords on each, and the link was restored. Both trackers then accepted only tracker 1's password.

**Why not Raft or a leader/backup design.** Raft needs a majority. With exactly two trackers (the specification's setup), losing either one would stop all changes, which contradicts "the system should continue operating as long as at least one tracker remains online". A leader/backup design has the same problem whenever the backup can't tell a dead leader from a cut-off one. Letting both accept changes and settling creation clashes deterministically keeps the system available. The cost is that other concurrent changes are merged rather than ordered globally.

### 2.5 Who is online
Seeders are useful only while online:
- **On this tracker:** each tracker counts the logged-in connections a user has on it. A connection's session ends on logout, on disconnect, or after 15 s without a heartbeat.
- **Through the other tracker:** trackers exchange `online` and `offline` changes, plus a full `PRESENCE` list whenever the link reconnects.
- **When the link drops,** the other tracker's logins stay trusted for 20 s. Its clients detect the failure through heartbeats within about 5 s and log in here, and clients that died with it expire. The grace period came from a test: forgetting those logins immediately made a download that started half a second after a tracker failure find no seeders.

---

## 3. Piece selection strategy

**Partial seeding.** A downloader serves every piece it has verified. Other downloaders learn which ones with a `BITFIELD` request, and ask again every second while they have nothing left to fetch from that peer. In tests where B and C downloaded a 77-piece file at overlapping times, the original seeder uploaded 106–116 pieces instead of 154, because B and C exchanged the rest.

**Rarest first.** Each peer has its own worker. When the worker has a free request slot, it asks a shared piece picker for the piece that:
- this peer has,
- we still need,
- isn't being fetched elsewhere,
- and is held by the fewest usable peers (ties broken at random).

Copying rare pieces first spreads every piece across the group quickly, so the file stays complete even if its original seeder leaves. Availability counts are kept per piece and updated when bitfields or peers change, so each choice is one pass over the pieces.

**Endgame.** Once every missing piece has already been requested somewhere, a piece may also be requested from a second peer, and whichever copy arrives first wins. Without this, the last piece waits on whichever peer happens to be slowest.

**Load balancing and peer scoring.**
- **Pull-based balancing.** Workers pull work as they finish it, so faster peers deliver more pieces with no extra bookkeeping. With two equal seeders, a 1,908-piece file split 952 / 956.
- **Bans.** A peer that sends a piece failing its hash is banned for the rest of that download. In the test, that took one bad piece, and the file still came out identical.
- **Backoff.** A peer that can't be reached or stops answering is retried after 2, 4, 8 … 30 s.
- **Refresh.** Every 5 s the peer list is refreshed from the tracker, so new peers and newly complete ones are used.
- **Pause.** If nothing arrives for 30 s, the download pauses with its progress saved, and resumes at the next login or `download_file`.

---

## 4. Protocol design

**Header.** Every message on every connection starts with a 6-byte header:
- type (request, reply, or piece data);
- status (the result code of a reply);
- payload length, 4 bytes in network byte order.

TCP is a byte stream with no message boundaries, so the receiver reads exactly 6 bytes and then exactly the payload length, looping over `recv`; senders loop over `send`. The length is checked against a maximum (64 MB, or 4 KB for requests to a peer), so a malformed header can't make a process allocate gigabytes.

**Status codes.** Each reply carries a status code: `0` OK, `1` bad request, `2` not logged in, `3` authentication failed, `4` exists, `5` not found, `6` forbidden, `7` conflict, `8` unavailable, `9` unknown command. The client acts on the code alone.

**Piece data.** Pieces travel as type 3 messages with raw bytes. The seeder sends them with `sendfile()`, straight from the page cache, which kept its memory around 5 MB while serving 1 GB.

**Text commands.** Commands stay as space-separated text inside requests (`join_group g1`), which keeps them readable and easy to test with the `tests/p2pmsg` tool. Numeric fields are parsed strictly, rejecting rather than throwing on bad input, because an uncaught exception in any thread terminates the whole process.

**Persistent, pipelined peer connections.** A downloader keeps one connection per peer and up to 4 requests outstanding, and replies come back in request order. A 1 GB file needs one connection per peer instead of about 1,900.

**Sessions.** `login` returns a random token, and a client continues on a new connection or another tracker with `resume <user> <token>`, so the password is sent once per login. Passwords are stored only as salted PBKDF2-HMAC-SHA256 hashes (20,000 rounds), and tokens only as SHA-256 hashes. Tokens are synced between trackers and revoked on logout. Tracker-to-tracker messages require a shared secret, compared in constant time.

---

## 5. Web interfaces

The terminal shell the specification requires stays the primary interface. Two optional web pages make the system visible:
- **Client web UI** (`./client … <web_port>`): accounts, groups, sharing and downloading from the browser. It shows live per-file progress and, for each peer, how many pieces it delivered, so partial seeding and peer bans can be watched as they happen.
- **Tracker dashboard** (`./tracker … <web_port>`): read-only and live. It shows who is online (here or through the other tracker), groups, files with their seeders and partial downloaders, and the state of the sync link.

**Design.**
- **No libraries.** Each program serves its page through a small HTTP/1.1 server written with plain sockets: one request per connection, one short-lived thread per request. The pages are plain HTML, CSS and JavaScript compiled into the binaries (`webui.h`, `dashboard.h`), and poll JSON endpoints every second.
- **One command path.** Every browser action goes through `run_command`, the same function the terminal uses. It writes its output to a stream: the terminal, or the HTTP response. A mutex runs commands from both one at a time, so the two interfaces can't diverge.

**Security.** A local page that can run commands, including sharing any file on the computer, is a classic target, so it has three defences:
- **It listens only on `127.0.0.1`.**
- **Every API call must carry a random per-run token** from the link the client prints. Another website open in the same browser can make the browser send requests to `127.0.0.1`, but it can't know the token. Without one, the request is refused, which also defeats DNS-rebinding tricks.
- **Every name from the tracker or other users is HTML-escaped.** Otherwise a file or user name containing markup could run script inside the page, and that script would hold the token.

The dashboard is read-only and also listens only on `127.0.0.1`.

## 6. Challenges and solutions

| Challenge | Solution |
|---|---|
| One malformed number (`stoi("abc")`) crashed a tracker or seeder, because an exception escaping a thread terminates the process | Strict number parsing, length checks before indexing, and a per-request exception guard |
| Changes between trackers were lost while one was down, could arrive out of order, and a restarted tracker came back empty | Ordered, acknowledged change stream with epochs (§2.1–2.2), state file (§2.3), state copy for a tracker without one |
| Logged-out, crashed and frozen clients stayed listed as seeders | Session counting per connection, heartbeats checked from login onward, `online`/`offline`/`PRESENCE`, the grace period (§2.5) |
| A frozen client was never dropped if it froze before its first heartbeat | The 15 s silence rule now starts at login, not at the first heartbeat |
| A stalled peer hung downloads; `connect()` to an unreachable host blocks for over a minute | Timeouts on every socket, a non-blocking `connect` with `poll`, and closing every peer connection the moment a download completes |
| Every downloader fetched every piece from the original seeder | Partial seeding with rarest-first selection (§3) |
| A download interrupted near the end started over | `.part` files with re-verification on resume |
| One thread per tracker connection | Event loop with a fixed worker pool |
| Passwords were stored and re-sent in plain text | Salted slow hashes, session tokens |
| Messages printed from download threads didn't appear when output was piped (buffered `cout`) | Flush after background messages; found by the automated test suite |
| macOS gives threads a 512 KB stack, smaller than a piece buffer | Piece buffers on the heap |
| A local web page that can run commands is reachable by any website the user has open | Listen on `127.0.0.1` only, a random token on every API call, and HTML-escaping of all names (§5) |
| The web UI had to behave exactly like the terminal | Both call `run_command`, which writes to the terminal or the HTTP response |

---

## 7. Performance

These are measurements, not estimates. The machine was an Apple M5 running macOS 26.6, with all processes on one machine (loopback network).

| Operation | Result |
|---|---|
| Hashing a 1 GB file on upload (one pass for piece and file hashes) | 0.69 s |
| Downloading 1 GB from 1 seeder, including the final hash check | 1.0 s |
| Downloading 1 GB from 2 seeders | 0.78 s (pieces split 952 / 956) |
| Peak memory during a 1 GB transfer | about 6 MB for the downloader, about 5–6 MB per seeder |
| Tracker threads with 300 extra idle connections | 11 (unchanged) |
| Full automated test suite (50 checks) | about 100 s |

**What the numbers mean:**
- **Memory doesn't grow with file size.** Each peer worker holds about one 512 KB piece at a time, and seeders don't buffer pieces at all (`sendfile`).
- **Loopback isn't a real network.** On loopback, one persistent connection to a single seeder is slightly slower than the earlier design's 4 parallel connections (1.0 s against 0.7 s), because the seeder serves each connection on one thread. In exchange, it opens one connection instead of about 1,900. A second seeder helps even here.
- **On a real network, bandwidth sets the floor.** Download time is at least size ÷ bandwidth: 100 MB over 100 Mb/s takes at least 8 s, and 1 GB at least 80 s. With *k* peers each limited by their upload link, partial seeding and rarest-first let a download approach *k* times one peer's rate, until the downloader's own link is full.
- **Small files.** A file under 512 KB is a single piece, so the time is dominated by one round trip to the tracker and one connection to a peer.

---

## 8. References
1. B. Cohen, *The BitTorrent Protocol Specification* (BEP 3). https://www.bittorrent.org/beps/bep_0003.html (rarest-first, endgame, bitfields)
2. B. Cohen, "Incentives Build Robustness in BitTorrent", Workshop on Economics of Peer-to-Peer Systems, 2003.
3. B. Hall, *Beej's Guide to Network Programming*. https://beej.us/guide/bgnet/
4. Linux manual pages: `socket(7)`, `tcp(7)`, `poll(2)`, `connect(2)`, `sendfile(2)`, `pread(2)`, `pwrite(2)`, `rename(2)`, `fsync(2)`. https://man7.org/linux/man-pages/
5. OpenSSL 3.0 manual: `SHA256_Init(3)` (SHA-1), `PKCS5_PBKDF2_HMAC(3)`, `RAND_bytes(3)`, `CRYPTO_memcmp(3)`. https://docs.openssl.org/
6. M. Kleppmann, *Designing Data-Intensive Applications*, O'Reilly, 2017, chapters 5 (Replication) and 9 (Consistency and Consensus).
7. D. Ongaro and J. Ousterhout, "In Search of an Understandable Consensus Algorithm (Raft)", USENIX ATC 2014.
