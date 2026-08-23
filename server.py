#!/usr/bin/env python3
"""
AGY Web Browser Proxy
Provides a web chat interface to an AGY (Antigravity) session.
Uses `agy --print --output-format stream-json` for reliable structured output
instead of fragile PTY scraping.
"""

import asyncio
import json
import mimetypes
import os
import secrets
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiofiles
from fastapi import FastAPI, File, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AGY_BIN    = os.environ.get("AGY_BIN", "agy")
WORKSPACE  = os.environ.get("AGY_WORKSPACE", str(Path.home()))
HOST       = os.environ.get("AGY_PROXY_HOST", "0.0.0.0")
PORT       = int(os.environ.get("AGY_PROXY_PORT", "7788"))
AUTH_TOKEN = os.environ.get("AGY_PROXY_TOKEN", "")

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

SESSIONS_DIR = Path(__file__).parent / "sessions_data"
SESSIONS_DIR.mkdir(exist_ok=True)

ARTIFACTS_DIR = Path(__file__).parent / "hosted_artifacts"
ARTIFACTS_DIR.mkdir(exist_ok=True)

BRAIN_DIR = Path.home() / ".gemini" / "antigravity-cli" / "brain"

ARCHIVE_STORE = Path(__file__).parent / "archived_conversations.json"

def get_archived_ids() -> set[str]:
    """Read set of archived conversation IDs from local persistent JSON store."""
    if not ARCHIVE_STORE.exists():
        return set()
    try:
        data = json.loads(ARCHIVE_STORE.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()

def save_archived_id(conv_id: str, archived: bool):
    """Update archived status in local persistent JSON store."""
    try:
        current = get_archived_ids()
        if archived:
            current.add(conv_id)
        else:
            current.discard(conv_id)
        ARCHIVE_STORE.write_text(json.dumps(list(current), indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[ERROR] Failed to save archive store: {e}")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="AGY Relay", version="v202608.0021")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class AgySession:
    """
    Each session maintains a conversation_id (from AGY) so that --continue
    resumes the right thread. Messages are run one at a time via
    `agy --print --output-format stream-json --continue`.
    """

    def __init__(self, session_id: str, workspace: str = WORKSPACE, model: str = "", conv_id: Optional[str] = None, output_buffer: Optional[list] = None, created_at: Optional[str] = None, name: Optional[str] = None, archived: bool = False, mode: str = ""):
        self.session_id    = session_id
        self.name          = name or f"Session {session_id}"
        self.workspace     = workspace
        self.model         = model
        self.mode          = mode      # "", "plan", "accept-edits"
        self.conv_id: Optional[str] = conv_id   # AGY conversation ID
        self.websockets: list[WebSocket] = []
        self.output_buffer: list[dict] = output_buffer if output_buffer is not None else []
        self.busy          = False
        self.last_turn_time: float = 0
        self.created_at    = created_at or now_iso()
        self.archived      = archived
        self.message_queue: list[dict] = []  # FIFO queue of {"id": str, "text": str, "timestamp": str}
        self.active_proc: Optional[asyncio.subprocess.Process] = None

    def save_to_disk(self):
        try:
            filepath = SESSIONS_DIR / f"{self.session_id}.json"
            data = {
                "session_id": self.session_id,
                "name": self.name,
                "workspace": self.workspace,
                "model": self.model,
                "mode": self.mode,
                "conv_id": self.conv_id,
                "created_at": self.created_at,
                "archived": self.archived,
                "output_buffer": self.output_buffer,
            }
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ERROR] Failed to save session {self.session_id} to disk: {e}")

    @classmethod
    def load_from_disk(cls, filepath: Path) -> Optional["AgySession"]:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls(
                session_id=data["session_id"],
                name=data.get("name"),
                workspace=data.get("workspace", WORKSPACE),
                model=data.get("model", ""),
                conv_id=data.get("conv_id"),
                output_buffer=data.get("output_buffer", []),
                created_at=data.get("created_at"),
                archived=data.get("archived", False),
                mode=data.get("mode", "")
            )
        except Exception as e:
            print(f"[ERROR] Failed to load session from {filepath}: {e}")
            return None

    # ── broadcast ──────────────────────────────────────────────────────────

    async def _broadcast(self, msg: dict):
        dead = []
        for ws in self.websockets:
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in self.websockets:
                self.websockets.remove(ws)

    # ── message queue management ───────────────────────────────────────────

    async def queue_message(self, user_text: str) -> dict:
        """Enqueue a user message. If session is idle, runs immediately. If busy, adds to FIFO queue."""
        queue_id = uuid.uuid4().hex[:8]
        item = {
            "id": queue_id,
            "text": user_text,
            "timestamp": now_iso(),
        }

        # If idle and no items in queue, execute immediately
        if not self.busy and len(self.message_queue) == 0:
            asyncio.create_task(self.run_turn(user_text, queue_id=queue_id))
            return {"status": "running", "id": queue_id, "position": 0}

        # Otherwise add to FIFO queue without broadcasting to chat history yet
        self.message_queue.append(item)
        await self._broadcast({
            "type": "queue_status",
            "queue_length": len(self.message_queue),
            "queued_messages": self.message_queue,
            "timestamp": now_iso()
        })
        print(f"[QUEUE] Queued message {queue_id} (position #{len(self.message_queue)}) for session {self.session_id}")
        return {"status": "queued", "id": queue_id, "position": len(self.message_queue)}

    def cancel_queued_message(self, queue_id: str) -> bool:
        initial_len = len(self.message_queue)
        self.message_queue = [m for m in self.message_queue if m["id"] != queue_id]
        return len(self.message_queue) < initial_len

    # ── run one turn ───────────────────────────────────────────────────────

    async def run_turn(self, user_text: str, queue_id: Optional[str] = None):
        """Run one user→agent turn using stream-json output."""
        now = time.time()
        # Reset lock if busy for more than 45s (prevent stale lock)
        if self.busy and (now - self.last_turn_time > 45):
            print(f"[WARN] Session {self.session_id} busy lock expired after 45s. Resetting busy lock.")
            self.busy = False

        if self.busy:
            # Route to queue if already busy
            return await self.queue_message(user_text)

        self.busy = True
        self.last_turn_time = now

        # Echo user message into chat history now that it is actively executing
        user_msg = {"type": "user", "text": user_text, "timestamp": now_iso()}
        self.output_buffer.append(user_msg)
        await self._broadcast(user_msg)
        await self._broadcast({"type": "thinking_pulse", "timestamp": now_iso()})

        cmd = [AGY_BIN, "--output-format", "stream-json",
               "--dangerously-skip-permissions"]
        active_model = self.model if (self.model and self.model != "(default)") else os.environ.get("AGY_DEFAULT_MODEL", "gemini-3.7-flash-high")
        if active_model:
            cmd += ["--model", active_model]
        if self.mode:
            cmd += ["--mode", self.mode]
        if self.conv_id:
            cmd += ["--conversation", self.conv_id]
        else:
            cmd += ["--continue"]
        cmd += ["--prompt", user_text]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace,
            )
            self.active_proc = proc

            turn_start_time = time.time()
            total_chars = 0
            current_text = ""
            streamed_any = False
            last_output_idx = None

            async def flush_text():
                nonlocal current_text, streamed_any, total_chars, last_output_idx
                t = current_text.rstrip("\n")
                if t.strip():
                    lower_t = t.lower()
                    if "stream was interrupted" in lower_t or "the stream was interrupted" in lower_t:
                        current_text = ""
                        return
                    total_chars += len(t)
                    msg = {"type": "output", "text": t, "timestamp": now_iso()}
                    self.output_buffer.append(msg)
                    last_output_idx = len(self.output_buffer) - 1
                    await self._broadcast(msg)
                    streamed_any = True
                current_text = ""

            async for raw_line in proc.stdout:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Guard: skip any line that parsed as non-dict (bare string, list, etc.)
                if not isinstance(ev, dict):
                    continue

                event_type = ev.get("event", "")

                # ── step_update ────────────────────────────────────────────
                if event_type == "step_update":
                    su = ev.get("step_update")
                    if not isinstance(su, dict):
                        continue

                    step_type = su.get("step_type", "")
                    state     = su.get("state", "")

                    # Agent response: accumulate text_delta across the step
                    if step_type == "agent_response":
                        delta = su.get("text_delta", "")
                        if delta:
                            current_text += delta
                        if state == "DONE":
                            await flush_text()

                    # Tool call: show name + output
                    elif step_type == "tool":
                        await flush_text()
                        tool_info = su.get("tool_info") or {}
                        tool_name = su.get("tool_name") or tool_info.get("name", "tool")

                        if state == "ACTIVE":
                            params = tool_info.get("parameters") or {}
                            # Show the most relevant param as a hint
                            hint = ""
                            for key in ("CommandLine", "Query", "AbsolutePath", "Url", "Prompt"):
                                if key in params:
                                    v = str(params[key])
                                    hint = ": " + (v[:80] + "…" if len(v) > 80 else v)
                                    break
                            tool_msg = {
                                "type": "tool",
                                "text": f"⚙ {tool_name}{hint}",
                                "timestamp": now_iso(),
                            }
                            self.output_buffer.append(tool_msg)
                            await self._broadcast(tool_msg)

                        elif state == "DONE":
                            output = tool_info.get("output", "")
                            preview = ""
                            if output:
                                out_str = str(output).strip()
                                preview = ": " + (out_str[:100] + "…" if len(out_str) > 100 else out_str)
                            done_msg = {
                                "type": "tool",
                                "text": f"✓ {tool_name}{preview}",
                                "timestamp": now_iso(),
                            }
                            self.output_buffer.append(done_msg)
                            await self._broadcast(done_msg)

                    # Thinking / reasoning steps
                    elif step_type in ("thinking", "reasoning", "unknown"):
                        await self._broadcast({"type": "thinking_pulse", "timestamp": now_iso()})

                # ── result: save conv_id + fallback if nothing was emitted ──
                elif event_type == "result":
                    res = ev.get("result")
                    if isinstance(res, dict):
                        self.conv_id = res.get("conversation_id") or self.conv_id
                        # If no text step was emitted during turn, fallback to final response
                        if not streamed_any and not current_text.strip():
                            final = res.get("response", "").strip()
                            if final:
                                current_text = final
                        await flush_text()
                        status = res.get("status", "")
                        err_detail = res.get("error", "")
                        if status not in ("SUCCESS", ""):
                            err_str = str(err_detail or "").lower()
                            if "quota reached" in err_str or "subscription" in err_str:
                                err_text = f"⚠️ Model Quota Reached: {err_detail}. Please switch to another model using the selector in the bottom toolbar (e.g. Gemini 3.7 Flash or Claude)."
                                err_msg = {"type": "error", "text": err_text, "timestamp": now_iso()}
                                self.output_buffer.append(err_msg)
                                await self._broadcast(err_msg)
                            else:
                                is_noise = any(k in err_str for k in (
                                    "declaring permissions", "cortex tool", "invalid tool call error",
                                    "convert tool call", "grep command timed out", "context deadline exceeded",
                                    "stream was interrupted", "the stream was interrupted", "stream interrupted"
                                ))
                                if not is_noise:
                                    err_text = f"AGY [{status}]"
                                    if err_detail:
                                        err_text += f": {err_detail}"
                                    err_msg = {"type": "error", "text": err_text, "timestamp": now_iso()}
                                    self.output_buffer.append(err_msg)
                                    await self._broadcast(err_msg)

            # Drain any leftover buffer
            await flush_text()

            # Capture stderr (ignore deprecation warnings, non-fatal quota notices, and cortex noise)
            stderr_raw = await proc.stderr.read()
            if stderr_raw:
                err_text = stderr_raw.decode("utf-8", errors="replace").strip()
                for line in err_text.splitlines():
                    lower_line = line.lower()
                    is_noise = any(k in lower_line for k in (
                        "deprecationwarning", "timezone", "quota reached", "subscription",
                        "declaring permissions", "cortex tool", "invalid tool call error",
                        "convert tool call", "grep command timed out", "context deadline exceeded",
                        "stream was interrupted", "the stream was interrupted", "stream interrupted"
                    ))
                    if line and not is_noise:
                        err_msg = {"type": "error", "text": line[:400], "timestamp": now_iso()}
                        self.output_buffer.append(err_msg)
                        await self._broadcast(err_msg)

            await proc.wait()

        except Exception as e:
            err = {"type": "error", "text": f"Proxy error: {e}", "timestamp": now_iso()}
            self.output_buffer.append(err)
            await self._broadcast(err)
        finally:
            self.active_proc = None
            self.busy = False
            turn_elapsed = max(0.1, round(time.time() - turn_start_time, 2))
            approx_tokens = max(1, int(total_chars / 4))
            tps = round(approx_tokens / turn_elapsed, 1)
            stats = {
                "duration": f"{turn_elapsed:.1f}s",
                "tokens": approx_tokens,
                "tps": f"{tps} t/s"
            }
            if last_output_idx is not None and last_output_idx < len(self.output_buffer):
                self.output_buffer[last_output_idx]["stats"] = stats

            self.save_to_disk()
            await self._broadcast({"type": "done", "stats": stats, "timestamp": now_iso()})

            # Check if there are queued messages waiting to execute next
            if self.message_queue:
                next_item = self.message_queue.pop(0)
                await self._broadcast({
                    "type": "queue_status",
                    "queue_length": len(self.message_queue),
                    "queued_messages": self.message_queue,
                    "timestamp": now_iso()
                })
                print(f"[QUEUE] Auto-dispatching queued message {next_item['id']} for session {self.session_id}")
                asyncio.create_task(self.run_turn(next_item["text"], queue_id=next_item["id"]))


    # ── client management ──────────────────────────────────────────────────

    def add_client(self, ws: WebSocket):
        self.websockets.append(ws)

    def remove_client(self, ws: WebSocket):
        if ws in self.websockets:
            self.websockets.remove(ws)

    def to_dict(self):
        return {
            "session_id":    self.session_id,
            "name":          self.name,
            "workspace":     self.workspace,
            "model":         self.model or "(default)",
            "mode":          self.mode or "",
            "conv_id":       self.conv_id,
            "busy":          self.busy,
            "queue_length":  len(self.message_queue),
            "queued":        self.message_queue,
            "clients":       len(self.websockets),
            "created_at":    self.created_at,
        }


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

sessions: dict[str, AgySession] = {}

def load_all_sessions_from_disk():
    for f in SESSIONS_DIR.glob("*.json"):
        sess = AgySession.load_from_disk(f)
        if sess:
            sessions[sess.session_id] = sess
    print(f"[AGY Web Proxy] Loaded {len(sessions)} session(s) from disk.")

load_all_sessions_from_disk()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    p = Path(__file__).parent / "static" / "index.html"
    content = p.read_text() if p.exists() else "<h1>index.html missing</h1>"
    return HTMLResponse(
        content=content,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/clear", response_class=HTMLResponse)
async def clear_cache():
    return HTMLResponse(
        content="""<!DOCTYPE html><html><body style="background:#0d1117;color:#c9d1d9;font-family:monospace;padding:40px">
<h2>Clearing cache...</h2><pre id="log"></pre>
<script>
const log = document.getElementById('log');
function msg(t) { log.textContent += t + '\\n'; }
(async () => {
  if ('serviceWorker' in navigator) {
    const regs = await navigator.serviceWorker.getRegistrations();
    for (const r of regs) { await r.unregister(); msg('Unregistered SW: ' + r.scope); }
  }
  if ('caches' in window) {
    const keys = await caches.keys();
    for (const k of keys) { await caches.delete(k); msg('Deleted cache: ' + k); }
  }
  msg('\\nDone! Redirecting in 2s...');
  setTimeout(() => window.location.href = '/', 2000);
})();
</script></body></html>""",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/api/sessions")
async def list_sessions():
    return {"sessions": [s.to_dict() for s in sessions.values()]}


@app.post("/api/sessions")
async def create_session(body: dict = None):
    body = body or {}
    sid       = str(uuid.uuid4())[:8]
    name      = body.get("name")
    workspace = body.get("workspace", WORKSPACE)
    model     = body.get("model", "")
    session   = AgySession(sid, workspace, model=model, name=name)
    sessions[sid] = session
    session.save_to_disk()
    return session.to_dict()


@app.post("/api/sessions/{session_id}/rename")
async def rename_session(session_id: str, body: dict = None):
    if session_id not in sessions:
        return JSONResponse({"error": "session not found"}, status_code=404)
    body = body or {}
    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    session = sessions[session_id]
    session.name = name
    session.save_to_disk()
    return session.to_dict()


@app.get("/api/sessions/{session_id}/history")
async def get_history(session_id: str):
    if session_id not in sessions:
        return JSONResponse({"messages": [], "busy": False}, status_code=404)
    session = sessions[session_id]
    if session.conv_id and not session.busy:
        latest = load_transcript_messages(session.conv_id)
        if latest:
            session.output_buffer = latest
            session.save_to_disk()
    return {
        "messages": session.output_buffer,
        "busy": session.busy,
        "queue_length": len(session.message_queue)
    }


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    sessions.pop(session_id, None)
    f = SESSIONS_DIR / f"{session_id}.json"
    if f.exists():
        try:
            f.unlink()
        except Exception:
            pass
    return {"ok": True}


@app.post("/api/conversations/{conv_id}/archive")
async def archive_conversation(conv_id: str, body: dict = None):
    """Mark conversation status as archived or active in both JSON store and SQLite."""
    body = body or {}
    archive_state = body.get("archived", True)
    new_status = "archived" if archive_state else ""
    
    # 1. Save to dedicated persistent JSON store
    save_archived_id(conv_id, archive_state)

    # 2. Also update session in-memory and on-disk if active
    for s in sessions.values():
        if s.conv_id == conv_id:
            s.archived = archive_state
            s.save_to_disk()

    # 3. Best-effort update to SQLite DB
    db_path = Path.home() / ".gemini" / "antigravity-cli" / "conversation_summaries.db"
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            c.execute("UPDATE conversation_summaries SET status = ? WHERE conversation_id = ?", (new_status, conv_id))
            conn.commit()
            conn.close()
        except Exception:
            pass

    return {"ok": True, "conversation_id": conv_id, "archived": archive_state}


@app.get("/api/conversations")
async def list_conversations(query: str = "", limit: int = 50, archived: bool = False):
    """List non-archived or archived AGY conversations from conversation_summaries.db."""
    db_path = Path.home() / ".gemini" / "antigravity-cli" / "conversation_summaries.db"
    if not db_path.exists():
        return {"conversations": []}
    try:
        archived_ids = get_archived_ids()
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        c = conn.cursor()
        
        # Select all conversations, then filter using our permanent archived_ids set
        if query:
            q_param = f"%{query}%"
            c.execute(
                f"SELECT conversation_id, title, preview, step_count, last_modified_time, status "
                f"FROM conversation_summaries "
                f"WHERE (conversation_id LIKE ? OR title LIKE ? OR preview LIKE ?) "
                f"ORDER BY last_modified_time DESC LIMIT ?",
                (q_param, q_param, q_param, limit * 2)
            )
        else:
            c.execute(
                f"SELECT conversation_id, title, preview, step_count, last_modified_time, status "
                f"FROM conversation_summaries "
                f"WHERE step_count > 0 "
                f"ORDER BY last_modified_time DESC LIMIT ?",
                (limit * 2,)
            )
        rows = c.fetchall()
        conn.close()

        convs = []
        for r in rows:
            cid = r[0]
            # Considered archived if in JSON store OR marked archived in sqlite
            is_arch = (cid in archived_ids) or (r[5] == "archived")
            if is_arch != archived:
                continue

            convs.append({
                "conversation_id": cid,
                "title": r[1] or r[2] or cid[:12],
                "preview": r[2],
                "step_count": r[3],
                "last_modified_time": r[4]
            })
            if len(convs) >= limit:
                break

        return {"conversations": convs}
    except Exception as e:
        return {"conversations": [], "error": str(e)}


@app.post("/api/conversations/{conv_id}/rename")
async def rename_conversation(conv_id: str, body: dict = None):
    """Update title in conversation_summaries.db."""
    body = body or {}
    title = (body.get("title") or body.get("name") or "").strip()
    if not title:
        return JSONResponse({"error": "title required"}, status_code=400)
    db_path = Path.home() / ".gemini" / "antigravity-cli" / "conversation_summaries.db"
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            c.execute("UPDATE conversation_summaries SET title = ? WHERE conversation_id = ?", (title, conv_id))
            conn.commit()
            conn.close()
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
    return {"ok": True, "conversation_id": conv_id, "title": title}


@app.delete("/api/conversations/{conv_id}")
async def delete_conversation(conv_id: str):
    """Delete a conversation from conversation_summaries.db and its files."""
    db_path = Path.home() / ".gemini" / "antigravity-cli" / "conversation_summaries.db"
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            c.execute("DELETE FROM conversation_summaries WHERE conversation_id = ?", (conv_id,))
            conn.commit()
            conn.close()
        except Exception:
            pass
    # Clean up conversation db and brain folder if present
    conv_db = Path.home() / ".gemini" / "antigravity-cli" / "conversations" / f"{conv_id}.db"
    if conv_db.exists():
        try:
            conv_db.unlink()
        except Exception:
            pass
    brain_dir = Path.home() / ".gemini" / "antigravity-cli" / "brain" / conv_id
    if brain_dir.exists():
        try:
            import shutil
            shutil.rmtree(brain_dir)
        except Exception:
            pass
    return {"ok": True}


def load_transcript_messages(conv_id: str) -> list[dict]:
    """Parse transcript.jsonl for a conversation and extract messages."""
    transcript_path = Path.home() / ".gemini" / "antigravity-cli" / "brain" / conv_id / ".system_generated" / "logs" / "transcript.jsonl"
    if not transcript_path.exists():
        return []
    messages = []
    import re
    try:
        with open(transcript_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    stype = data.get("type")
                    time = data.get("created_at") or now_iso()
                    if stype == "USER_INPUT":
                        content = data.get("content", "")
                        match = re.search(r'<USER_REQUEST>(.*?)</USER_REQUEST>', content, re.DOTALL)
                        text = match.group(1).strip() if match else content.strip()
                        if text:
                            messages.append({"type": "user", "text": text, "timestamp": time})
                    elif stype == "PLANNER_RESPONSE":
                        content = data.get("content", "")
                        if content and content.strip():
                            messages.append({"type": "output", "text": content.strip(), "timestamp": time})
                    elif stype == "ERROR_MESSAGE":
                        err = data.get("error") or data.get("content")
                        err_str = str(err or "").lower()
                        is_noise = any(k in err_str for k in (
                            "context canceled", "stopped due to server restart",
                            "quota reached", "subscription", "declaring permissions",
                            "cortex tool", "invalid tool call error", "convert tool call",
                            "grep command timed out", "context deadline exceeded",
                            "stream was interrupted", "the stream was interrupted", "stream interrupted"
                        ))
                        if err and not is_noise:
                            messages.append({"type": "error", "text": f"AGY: {err}", "timestamp": time})
                except Exception:
                    pass
    except Exception as e:
        print(f"[ERROR] Failed to load transcript for {conv_id}: {e}")
    return messages


@app.post("/api/conversations/{conv_id}/open")
async def open_conversation(conv_id: str):
    """Open a past AGY conversation into its own dedicated active session."""
    # Check if an active session already exists for this conv_id
    for s in sessions.values():
        if s.conv_id == conv_id:
            return s.to_dict()

    # Fetch conversation title
    title = conv_id[:12]
    db_path = Path.home() / ".gemini" / "antigravity-cli" / "conversation_summaries.db"
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            c.execute("SELECT title, preview FROM conversation_summaries WHERE conversation_id = ?", (conv_id,))
            r = c.fetchone()
            conn.close()
            if r:
                title = r[0] or r[1] or title
        except Exception:
            pass

    # Create a new active session
    sid = str(uuid.uuid4())[:8]
    session = AgySession(sid, WORKSPACE, name=title)
    session.conv_id = conv_id
    
    # Load transcript history into session buffer
    past_messages = load_transcript_messages(conv_id)
    if past_messages:
        session.output_buffer = past_messages
    
    sessions[sid] = session
    session.save_to_disk()
    return session.to_dict()


@app.get("/api/models")
async def list_models():
    try:
        r = subprocess.run([AGY_BIN, "models"], capture_output=True, text=True, timeout=10)
        models = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("Fetching"):
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                models.append({"id": parts[0].strip(), "label": parts[1].strip()})
            elif parts[0]:
                models.append({"id": parts[0].strip(), "label": parts[0].strip()})
        return {"models": models}
    except Exception as e:
        return {"models": [], "error": str(e)}


@app.post("/api/upload")
@app.post("/api/sessions/{session_id}/upload")
async def upload_image(session_id: str = "default", file: UploadFile = File(...)):
    # Auto-initialize session if not in memory
    if session_id and session_id not in sessions:
        sess_file = SESSIONS_DIR / f"{session_id}.json"
        if sess_file.exists():
            sess = AgySession.load_from_disk(sess_file)
            if sess:
                sessions[session_id] = sess
        if session_id not in sessions:
            sessions[session_id] = AgySession(session_id, WORKSPACE)
            sessions[session_id].save_to_disk()

    ext = Path(file.filename or "upload.png").suffix or ".png"
    prefix = session_id if session_id else "upload"
    fname = f"{prefix}_{uuid.uuid4().hex[:8]}{ext}"
    dest  = UPLOAD_DIR / fname
    async with aiofiles.open(dest, "wb") as f:
        content = await file.read()
        await f.write(content)
    return {
        "ok": True,
        "filename": file.filename,
        "path": str(dest),
        "url": f"/uploads/{fname}",
        "size": len(content)
    }


@app.get("/api/sessions/{session_id}/change-model")
async def change_model(session_id: str, model: str = ""):
    if session_id not in sessions:
        return {"error": "not found"}
    session = sessions[session_id]
    old = session.model
    session.model = model
    session.save_to_disk()
    return {"ok": True, "model": model, "previous": old}


@app.get("/api/sessions/{session_id}/change-mode")
async def change_mode(session_id: str, mode: str = ""):
    if session_id not in sessions:
        return {"error": "not found"}
    session = sessions[session_id]
    session.mode = mode
    session.save_to_disk()
    return {"ok": True, "mode": mode}


@app.get("/api/sessions/{session_id}/change-effort")
async def change_effort(session_id: str, effort: str = ""):
    return {"ok": True, "deprecated": True}


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    print(f"[WS] WebSocket connected: session_id={session_id}, client={websocket.client}", flush=True)

    if AUTH_TOKEN:
        token = websocket.headers.get("authorization", "").replace("Bearer ", "")
        if token != AUTH_TOKEN:
            await websocket.send_json({"type": "error", "text": "Unauthorized"})
            await websocket.close(code=4001)
            return

    if session_id not in sessions:
        sessions[session_id] = AgySession(session_id, WORKSPACE)
        sessions[session_id].save_to_disk()

    session = sessions[session_id]
    session.add_client(websocket)

    # Replay history and busy state
    print(f"[WS] Replaying {len(session.output_buffer)} messages (busy={session.busy}) to client for session {session_id}", flush=True)
    await websocket.send_json({
        "type": "history",
        "messages": session.output_buffer,
        "busy": session.busy,
    })

    # If currently working/busy, notify client immediately so 3 dots appear
    if session.busy:
        await websocket.send_json({
            "type": "thinking_pulse",
            "timestamp": now_iso()
        })

    # Replay active queue status
    if session.message_queue:
        await websocket.send_json({
            "type": "queue_status",
            "queue_length": len(session.message_queue),
            "queued_messages": session.message_queue,
            "timestamp": now_iso()
        })

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "input")

            if action in ("input", "prompt"):
                text = data.get("text", "").strip()
                if text:
                    # Enqueue prompt (runs immediately if idle, queues if busy)
                    asyncio.create_task(session.queue_message(text))

            elif action == "cancel_queue_item":
                qid = data.get("queue_id")
                if qid:
                    session.cancel_queued_message(qid)
                    await session._broadcast({
                        "type": "queue_cancelled",
                        "id": qid,
                        "queue_length": len(session.message_queue),
                        "queued_messages": session.message_queue,
                        "timestamp": now_iso()
                    })

            elif action == "clear_queue":
                session.message_queue.clear()
                await session._broadcast({
                    "type": "queue_status",
                    "queue_length": 0,
                    "queued_messages": [],
                    "timestamp": now_iso()
                })

            elif action == "ping":
                await websocket.send_json({"type": "pong"})

            elif action == "clear":
                session.output_buffer.clear()
                session.message_queue.clear()
                session.conv_id = None
                session.save_to_disk()

            elif action == "interrupt":
                if session.active_proc and session.active_proc.returncode is None:
                    try:
                        session.active_proc.terminate()
                    except Exception:
                        pass
                session.busy = False
                await session._broadcast({
                    "type": "system",
                    "text": "Turn stopped by user.",
                    "timestamp": now_iso(),
                })
                await session._broadcast({"type": "done", "timestamp": now_iso()})

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        session.remove_client(websocket)


@app.get("/api/sessions/{session_id}/queue")
async def get_session_queue(session_id: str):
    s = sessions.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "ok": True,
        "busy": s.busy,
        "queue_length": len(s.message_queue),
        "queue": s.message_queue,
    }


@app.delete("/api/sessions/{session_id}/queue/{queue_id}")
async def cancel_session_queue_item(session_id: str, queue_id: str):
    s = sessions.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Session not found")
    cancelled = s.cancel_queued_message(queue_id)
    if cancelled:
        await s._broadcast({
            "type": "queue_cancelled",
            "id": queue_id,
            "queue_length": len(s.message_queue),
            "queued_messages": s.message_queue,
            "timestamp": now_iso()
        })
    return {"ok": True, "cancelled": cancelled, "queue_length": len(s.message_queue)}


@app.delete("/api/sessions/{session_id}/queue")
async def clear_session_queue(session_id: str):
    s = sessions.get(session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Session not found")
    s.message_queue.clear()
    await s._broadcast({
        "type": "queue_status",
        "queue_length": 0,
        "queued_messages": [],
        "timestamp": now_iso()
    })
    return {"ok": True, "cleared": True}


@app.get("/api/health")
async def health():
    return {"status": "ok", "agy_bin": AGY_BIN, "workspace": WORKSPACE,
            "sessions": len(sessions), "timestamp": now_iso()}


@app.get("/manifest.json")
async def get_manifest():
    p = Path(__file__).parent / "static" / "manifest.json"
    return JSONResponse(json.loads(p.read_text()))

@app.get("/sw.js")
async def get_sw():
    p = Path(__file__).parent / "static" / "sw.js"
    from fastapi.responses import FileResponse
    return FileResponse(p, media_type="application/javascript")

@app.get("/icon-192.png")
async def get_icon_192():
    return FileResponse(Path(__file__).parent / "static" / "icon-192.png", media_type="image/png")

@app.get("/icon-512.png")
async def get_icon_512():
    return FileResponse(Path(__file__).parent / "static" / "icon-512.png", media_type="image/png")

@app.get("/apple-touch-icon.png")
async def get_apple_icon():
    return FileResponse(Path(__file__).parent / "static" / "apple-touch-icon.png", media_type="image/png")

@app.get("/favicon.ico")
async def get_favicon():
    return FileResponse(Path(__file__).parent / "static" / "icon-192.png", media_type="image/png")


# ---------------------------------------------------------------------------
# Hosted Mockups & Artifacts (/artifact/<id>)
# ---------------------------------------------------------------------------

@app.get("/artifact/{artifact_id}")
@app.get("/artifact/{artifact_id}/{subpath:path}")
async def serve_artifact(artifact_id: str, subpath: str = ""):
    safe_id = Path(artifact_id).name

    # 1. Look in hosted_artifacts directory
    target_dir = ARTIFACTS_DIR / safe_id
    if target_dir.exists() and target_dir.is_dir():
        if subpath:
            file_path = (target_dir / subpath).resolve()
            if str(file_path).startswith(str(target_dir.resolve())) and file_path.is_file():
                mime, _ = mimetypes.guess_type(str(file_path))
                return FileResponse(file_path, media_type=mime or "application/octet-stream")
        else:
            for candidate_name in ("index.html", "mockup.html", "app.html"):
                idx = target_dir / candidate_name
                if idx.is_file():
                    return FileResponse(idx, media_type="text/html; charset=utf-8")
            for f in target_dir.iterdir():
                if f.is_file():
                    mime, _ = mimetypes.guess_type(str(f))
                    return FileResponse(f, media_type=mime or "text/html; charset=utf-8")

    # Check standalone single file in hosted_artifacts (e.g. hosted_artifacts/abc123.html)
    for ext in ("", ".html", ".htm", ".png", ".jpg", ".svg", ".json", ".txt"):
        candidate = ARTIFACTS_DIR / f"{safe_id}{ext}"
        if candidate.is_file():
            mime, _ = mimetypes.guess_type(str(candidate))
            return FileResponse(candidate, media_type=mime or "text/html; charset=utf-8")

    # 2. Look in brain directory (~/.gemini/antigravity-cli/brain)
    if BRAIN_DIR.exists():
        matched = list(BRAIN_DIR.glob(f"*/{safe_id}*")) + list(BRAIN_DIR.glob(f"*/*/{safe_id}*"))
        for mf in matched:
            if mf.is_file():
                mime, _ = mimetypes.guess_type(str(mf))
                return FileResponse(mf, media_type=mime or "text/html; charset=utf-8")
            elif mf.is_dir():
                idx = mf / "index.html"
                if idx.is_file():
                    return FileResponse(idx, media_type="text/html; charset=utf-8")

    raise HTTPException(status_code=404, detail=f"Artifact '{safe_id}' not found")


@app.post("/api/artifacts")
async def create_artifact(request: Request):
    """Publish / host a new mockup HTML or file"""
    content_type = request.headers.get("content-type", "")
    artifact_id = secrets.token_hex(4)

    if "application/json" in content_type:
        body = await request.json()
        custom_id = body.get("id")
        if custom_id:
            artifact_id = Path(str(custom_id)).name
        html_content = body.get("content") or body.get("html") or ""
        filename = body.get("filename", "index.html")
    else:
        raw_body = await request.body()
        html_content = raw_body.decode("utf-8", errors="replace")
        filename = "index.html"

    artifact_folder = ARTIFACTS_DIR / artifact_id
    artifact_folder.mkdir(exist_ok=True)
    target_file = artifact_folder / filename
    target_file.write_text(html_content, encoding="utf-8")

    return {
        "status": "ok",
        "artifact_id": artifact_id,
        "filename": filename,
        "url": f"/artifact/{artifact_id}",
        "full_url": f"http://{HOST}:{PORT}/artifact/{artifact_id}",
    }


@app.get("/api/artifacts")
async def list_artifacts():
    """List all currently hosted artifacts & mockups"""
    items = []
    if ARTIFACTS_DIR.exists():
        for path in ARTIFACTS_DIR.iterdir():
            if path.is_dir():
                files = [f.name for f in path.iterdir() if f.is_file()]
                items.append({
                    "id": path.name,
                    "type": "directory",
                    "files": files,
                    "url": f"/artifact/{path.name}",
                    "created_at": path.stat().st_mtime,
                })
            elif path.is_file():
                items.append({
                    "id": path.stem,
                    "filename": path.name,
                    "type": "file",
                    "url": f"/artifact/{path.name}",
                    "created_at": path.stat().st_mtime,
                })
    return {"artifacts": sorted(items, key=lambda x: x.get("created_at", 0), reverse=True)}


@app.get("/api/mcp")
async def list_mcp():
    """List connected MCP servers and tools"""
    try:
        r = subprocess.run([AGY_BIN, "mcp", "list"], capture_output=True, text=True, timeout=10)
        output = (r.stdout or "").strip() or (r.stderr or "").strip() or "No MCP servers configured."
        return {"ok": True, "output": output}
    except Exception as e:
        return {"ok": False, "error": str(e)}


_usage_cache = {"timestamp": 0, "data": None}

@app.get("/api/usage")
async def get_usage(force: bool = False):
    """Query current AGY quota, smoothing, and usage breakdown"""
    global _usage_cache
    now = time.time()
    if not force and _usage_cache["data"] and (now - _usage_cache["timestamp"] < 45):
        return _usage_cache["data"]

    try:
        proc = await asyncio.create_subprocess_exec(
            AGY_BIN, "--output-format", "stream-json", "--dangerously-skip-permissions", "--prompt", "/usage",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=WORKSPACE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=12)
        
        usage_data = None
        for line in stdout.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                if ev.get("event") == "command_result" and ev.get("command", {}).get("name") == "usage":
                    usage_data = ev.get("command", {}).get("data")
                    break
                elif ev.get("event") == "result" and "command" in ev.get("result", {}):
                    usage_data = ev.get("result", {}).get("command", {}).get("data")
                    break
            except Exception:
                continue

        if usage_data:
            result = {"ok": True, "usage": usage_data, "updated_at": now_iso()}
            _usage_cache = {"timestamp": now, "data": result}
            return result
        else:
            return {"ok": False, "error": "No usage data found in output", "raw": stdout.decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    print(f"[AGY Web Proxy v2] http://{HOST}:{PORT}")
    print(f"[AGY Web Proxy v2] Workspace: {WORKSPACE}  |  AGY: {AGY_BIN}")
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*",
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )
