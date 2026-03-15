from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from loguru import logger

ProgressCallback = Callable[[str], Awaitable[None]]


@dataclass(slots=True)
class AppServerRunResult:
    output: str
    session_id: str | None
    success: bool
    interrupted: bool = False
    event_lines: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class _PendingTurn:
    run_key: str | None
    thread_id: str
    turn_id: str
    on_progress: ProgressCallback | None
    created_at: float = field(default_factory=time.time)
    event_lines: list[dict[str, Any]] = field(default_factory=list)
    last_usage: dict[str, int] | None = None
    last_agent_message: str = ""
    delta_buffers: dict[str, str] = field(default_factory=dict)
    final_status: str | None = None
    final_error: str | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


class CodexAppServerRuntime:
    def __init__(self, route: Any, default_bin: str = "codex"):
        self.route = route
        self.default_bin = default_bin
        self.proc: asyncio.subprocess.Process | None = None
        self._request_id = 0
        self._pending_requests: dict[int, asyncio.Future[Any]] = {}
        self._reader_task: asyncio.Task[Any] | None = None
        self._stderr_task: asyncio.Task[Any] | None = None
        self._wait_task: asyncio.Task[Any] | None = None
        self._write_lock = asyncio.Lock()
        self._startup_lock = asyncio.Lock()
        self._initialized = False
        self._turns: dict[str, _PendingTurn] = {}
        self._run_key_to_turn_id: dict[str, str] = {}
        self._interrupted_run_keys: set[str] = set()
        self._recent_stderr_lines: deque[str] = deque(maxlen=20)
        self.idle_timeout_seconds = float(getattr(route, "app_server_idle_timeout_seconds", 900.0) or 0.0)
        self.last_used_at = time.time()
        self._idle_task: asyncio.Task[Any] | None = None

    async def ensure_started(self) -> None:
        self.last_used_at = time.time()
        self._cancel_idle_task()
        if self.proc is not None and self.proc.returncode is None and self._initialized:
            return
        async with self._startup_lock:
            if self.proc is not None and self.proc.returncode is None and self._initialized:
                return
            await self._start_process()
            await self._initialize()

    async def _start_process(self) -> None:
        await self.close()
        env = os.environ.copy()
        env.setdefault("NO_COLOR", "1")
        codex_bin = getattr(self.route, "codex_bin", None) or self.default_bin
        codex_args = list(getattr(self.route, "codex_args", []) or [])
        cmd = [codex_bin, *codex_args, "app-server", "--listen", "stdio://"]
        logger.info("Starting Codex app-server route={} workdir={}", self.route.name, self.route.workdir)
        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.route.workdir,
            env=env,
        )
        self._initialized = False
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._wait_task = asyncio.create_task(self._wait_for_exit())

    async def _initialize(self) -> None:
        result = await self._request(
            "initialize",
            {
                "clientInfo": {
                    "name": "flycodex",
                    "title": "FlyCodex",
                    "version": "0.3.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                },
            },
            timeout=20.0,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Invalid initialize response from codex app-server")
        await self._notify("initialized", {})
        self._initialized = True

    async def close(self) -> None:
        proc = self.proc
        self.proc = None
        self._initialized = False
        self._cancel_idle_task()
        for task in (self._reader_task, self._stderr_task, self._wait_task):
            if task is not None and not task.done():
                task.cancel()
        self._reader_task = None
        self._stderr_task = None
        self._wait_task = None
        pending_requests = list(self._pending_requests.values())
        self._pending_requests.clear()
        for fut in pending_requests:
            if not fut.done():
                fut.set_exception(RuntimeError("Codex app-server closed"))
        for turn in list(self._turns.values()):
            if turn.final_status is None:
                turn.final_status = "failed"
                turn.final_error = "Codex app-server 已断开。"
                turn.done.set()
        self._turns.clear()
        self._run_key_to_turn_id.clear()
        self._interrupted_run_keys.clear()
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), timeout=5)
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

    async def run(
        self,
        *,
        prompt: str,
        session_id: str | None,
        image_paths: list[str],
        run_key: str | None,
        on_progress: ProgressCallback | None,
        developer_instructions: str | None = None,
    ) -> AppServerRunResult:
        await self.ensure_started()
        thread_id = await self._ensure_thread(session_id, developer_instructions=developer_instructions)
        turn_id = await self._start_turn(thread_id=thread_id, prompt=prompt, image_paths=image_paths)
        turn = _PendingTurn(
            run_key=run_key,
            thread_id=thread_id,
            turn_id=turn_id,
            on_progress=on_progress,
        )
        self._turns[turn_id] = turn
        if run_key:
            self._run_key_to_turn_id[run_key] = turn_id
            if run_key in self._interrupted_run_keys:
                await self._interrupt_turn(thread_id, turn_id)
        await turn.done.wait()
        self._turns.pop(turn_id, None)
        if run_key:
            self._run_key_to_turn_id.pop(run_key, None)
            self._interrupted_run_keys.discard(run_key)
        self.last_used_at = time.time()
        self._schedule_idle_release()
        output = turn.last_agent_message.strip()
        if not output and turn.delta_buffers:
            output = max(turn.delta_buffers.values(), key=len).strip()
        if not output and turn.final_error:
            output = turn.final_error.strip()
        if not output and turn.final_status == "interrupted":
            output = "任务已中断。"
        success = turn.final_status == "completed"
        interrupted = turn.final_status == "interrupted"
        return AppServerRunResult(
            output=output or ("Codex 没有返回可显示的最终文本。" if success else "Codex 执行失败。"),
            session_id=thread_id,
            success=success,
            interrupted=interrupted,
            event_lines=list(turn.event_lines),
        )

    async def interrupt(self, run_key: str) -> bool:
        turn_id = self._run_key_to_turn_id.get(run_key)
        if turn_id is None:
            self._interrupted_run_keys.add(run_key)
            return False
        turn = self._turns.get(turn_id)
        if turn is None or turn.done.is_set():
            return False
        self._interrupted_run_keys.add(run_key)
        await self._interrupt_turn(turn.thread_id, turn.turn_id)
        return True

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None and self._initialized

    def status_snapshot(self) -> dict[str, Any]:
        idle_for = max(0.0, time.time() - self.last_used_at)
        return {
            "running": self.is_running(),
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "idle_for_seconds": idle_for,
            "active_turns": len(self._turns),
            "last_used_at": int(self.last_used_at),
        }

    def _cancel_idle_task(self) -> None:
        current = asyncio.current_task()
        task = self._idle_task
        if task is not None and task is not current and not task.done():
            task.cancel()
        self._idle_task = None

    def _schedule_idle_release(self) -> None:
        if self.idle_timeout_seconds <= 0 or self._turns:
            return
        self._cancel_idle_task()
        self._idle_task = asyncio.create_task(self._idle_release_loop())

    async def _idle_release_loop(self) -> None:
        try:
            while True:
                idle_for = time.time() - self.last_used_at
                wait_seconds = self.idle_timeout_seconds - idle_for
                if wait_seconds <= 0:
                    break
                await asyncio.sleep(wait_seconds)
                if self._turns:
                    return
            if self._turns:
                return
            if self.proc is None or self.proc.returncode is not None:
                return
            logger.info("Releasing idle Codex app-server route={} after {:.1f}s idle", self.route.name, self.idle_timeout_seconds)
            await self.close()
        except asyncio.CancelledError:
            return

    async def _ensure_thread(self, session_id: str | None, *, developer_instructions: str | None = None) -> str:
        if session_id:
            result = await self._request(
                "thread/resume",
                {
                    "threadId": session_id,
                    "cwd": self.route.workdir,
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                    **({"developerInstructions": developer_instructions} if developer_instructions else {}),
                },
                timeout=30.0,
            )
        else:
            result = await self._request(
                "thread/start",
                {
                    "cwd": self.route.workdir,
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                    "serviceName": "flycodex",
                    **({"developerInstructions": developer_instructions} if developer_instructions else {}),
                },
                timeout=30.0,
            )
        thread = (result or {}).get("thread") if isinstance(result, dict) else None
        thread_id = str((thread or {}).get("id") or "").strip()
        if not thread_id:
            raise RuntimeError("Failed to create/resume codex thread")
        return thread_id

    async def _start_turn(self, *, thread_id: str, prompt: str, image_paths: list[str]) -> str:
        input_items: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        for image_path in image_paths:
            input_items.append({"type": "localImage", "path": image_path})
        result = await self._request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": input_items,
                "cwd": self.route.workdir,
                "approvalPolicy": "never",
                "sandboxPolicy": {"type": "dangerFullAccess"},
            },
            timeout=30.0,
        )
        turn = (result or {}).get("turn") if isinstance(result, dict) else None
        turn_id = str((turn or {}).get("id") or "").strip()
        if not turn_id:
            raise RuntimeError("Failed to start codex turn")
        return turn_id

    async def _interrupt_turn(self, thread_id: str, turn_id: str) -> None:
        self.last_used_at = time.time()
        with contextlib.suppress(Exception):
            await self._request(
                "turn/interrupt",
                {
                    "threadId": thread_id,
                    "turnId": turn_id,
                },
                timeout=10.0,
            )

    async def _request(self, method: str, params: dict[str, Any], timeout: float = 30.0) -> Any:
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("Codex app-server stdin is not available")
        self._request_id += 1
        request_id = self._request_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_requests[request_id] = future
        message = {"id": request_id, "method": method, "params": params}
        async with self._write_lock:
            self.proc.stdin.write((json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8"))
            await self.proc.stdin.drain()
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            stderr_tail = self._format_recent_stderr()
            detail = f"Codex app-server request timed out after {timeout:.1f}s: {method}"
            if stderr_tail:
                detail += f"\nRecent stderr:\n{stderr_tail}"
            raise RuntimeError(detail) from exc
        finally:
            self._pending_requests.pop(request_id, None)

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("Codex app-server stdin is not available")
        message = {"method": method, "params": params}
        async with self._write_lock:
            self.proc.stdin.write((json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8"))
            await self.proc.stdin.drain()

    async def _read_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                return
            line_text = line.decode("utf-8", errors="replace").strip()
            if not line_text:
                continue
            try:
                payload = json.loads(line_text)
            except json.JSONDecodeError:
                logger.debug("Ignoring non-JSON app-server stdout: {}", line_text)
                continue
            if isinstance(payload, dict) and "method" in payload and "id" not in payload:
                await self._handle_notification(payload)
                continue
            if isinstance(payload, dict) and "id" in payload:
                request_id = payload.get("id")
                future = self._pending_requests.get(request_id)
                if future is None or future.done():
                    continue
                if "error" in payload and payload["error"] is not None:
                    future.set_exception(RuntimeError(self._format_rpc_error(payload["error"])))
                else:
                    future.set_result(payload.get("result"))

    async def _read_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        while True:
            line = await self.proc.stderr.readline()
            if not line:
                return
            line_text = line.decode("utf-8", errors="replace").rstrip()
            if line_text:
                self._recent_stderr_lines.append(line_text)
                logger.debug("codex app-server[{}] {}", self.route.name, line_text)

    async def _wait_for_exit(self) -> None:
        if self.proc is None:
            return
        return_code = await self.proc.wait()
        logger.warning("Codex app-server exited route={} code={}", self.route.name, return_code)
        for future in list(self._pending_requests.values()):
            if not future.done():
                future.set_exception(RuntimeError(f"Codex app-server exited: {return_code}"))
        self._pending_requests.clear()
        for turn in list(self._turns.values()):
            if turn.final_status is None:
                turn.final_status = "failed"
                turn.final_error = f"Codex app-server exited: {return_code}"
                turn.done.set()
        self._initialized = False

    async def _handle_notification(self, payload: dict[str, Any]) -> None:
        method = str(payload.get("method") or "")
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        if method == "thread/started":
            thread = params.get("thread") if isinstance(params, dict) else None
            thread_id = str((thread or {}).get("id") or "").strip()
            if thread_id:
                for turn in self._turns.values():
                    if turn.thread_id == thread_id:
                        turn.event_lines.append({"type": "thread.started", "thread_id": thread_id})
            return
        if method == "thread/tokenUsage/updated":
            turn_id = str(params.get("turnId") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            token_usage = params.get("tokenUsage") if isinstance(params, dict) else None
            last_usage = (token_usage or {}).get("last") if isinstance(token_usage, dict) else None
            if isinstance(last_usage, dict):
                turn.last_usage = {
                    "input_tokens": int(last_usage.get("inputTokens") or 0),
                    "cached_input_tokens": int(last_usage.get("cachedInputTokens") or 0),
                    "output_tokens": int(last_usage.get("outputTokens") or 0),
                }
            return
        if method == "turn/started":
            turn_info = params.get("turn") if isinstance(params, dict) else None
            turn_id = str((turn_info or {}).get("id") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            turn.event_lines.append({"type": "turn.started", "turn_id": turn_id})
            if turn.on_progress is not None:
                await turn.on_progress("我正在项目里翻代码、跑命令。")
            return
        if method == "turn/completed":
            turn_info = params.get("turn") if isinstance(params, dict) else None
            turn_id = str((turn_info or {}).get("id") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            status = str((turn_info or {}).get("status") or "")
            event: dict[str, Any] = {"type": "turn.completed", "status": status}
            if turn.last_usage is not None:
                event["usage"] = turn.last_usage
            error_info = (turn_info or {}).get("error") if isinstance(turn_info, dict) else None
            if isinstance(error_info, dict):
                turn.final_error = str(error_info.get("message") or "").strip() or None
                if turn.final_error:
                    event["message"] = turn.final_error
            turn.event_lines.append(event)
            turn.final_status = status
            turn.done.set()
            return
        if method == "error":
            turn_id = str(params.get("turnId") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            error_info = params.get("error") if isinstance(params, dict) else None
            message = str((error_info or {}).get("message") or "").strip()
            if message:
                turn.event_lines.append({"type": "error", "message": message})
                if turn.on_progress is not None:
                    await turn.on_progress(f"中途冒了个提示：{message}")
            return
        if method == "item/agentMessage/delta":
            turn_id = str(params.get("turnId") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            item_id = str(params.get("itemId") or "")
            delta = str(params.get("delta") or "")
            if item_id and delta:
                turn.delta_buffers[item_id] = turn.delta_buffers.get(item_id, "") + delta
            return
        if method in {"item/started", "item/completed"}:
            turn_id = str(params.get("turnId") or "")
            turn = self._turns.get(turn_id)
            if turn is None:
                return
            item = params.get("item") if isinstance(params, dict) else None
            normalized = self._normalize_item(item)
            if normalized is None:
                return
            event_type = "item.started" if method == "item/started" else "item.completed"
            turn.event_lines.append({"type": event_type, "item": normalized})
            await self._handle_item_progress(turn, method, normalized)
            return

    async def _handle_item_progress(self, turn: _PendingTurn, method: str, item: dict[str, Any]) -> None:
        item_type = item.get("type")
        if item_type == "agent_message" and method == "item.completed":
            text = str(item.get("text") or "")
            if text.strip():
                turn.last_agent_message = text.strip()
            return
        if turn.on_progress is None:
            return
        if item_type == "command_execution":
            command = str(item.get("command") or "").strip()
            if method == "item.started" and command:
                preview = command[:80] + ("…" if len(command) > 80 else "")
                await turn.on_progress(f"刚执行命令：`{preview}`")
                return
            if method == "item.completed" and command and int(item.get("exit_code") or 0) != 0:
                preview = command[:80] + ("…" if len(command) > 80 else "")
                await turn.on_progress(f"命令执行失败：`{preview}`")
                return
        if item_type == "file_change" and method == "item.completed":
            changes = item.get("changes")
            if isinstance(changes, list) and changes:
                await turn.on_progress(f"刚修改了 {len(changes)} 个文件。")

    @staticmethod
    def _normalize_item(item: Any) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None
        item_type = str(item.get("type") or "")
        mapping = {
            "agentMessage": "agent_message",
            "commandExecution": "command_execution",
            "fileChange": "file_change",
            "reasoning": "reasoning",
            "plan": "plan",
            "mcpToolCall": "mcp_tool_call",
            "webSearch": "web_search",
        }
        normalized_type = mapping.get(item_type)
        if not normalized_type:
            return None
        result: dict[str, Any] = {"id": item.get("id"), "type": normalized_type}
        if normalized_type == "agent_message":
            result["text"] = str(item.get("text") or "")
            return result
        if normalized_type == "command_execution":
            result.update(
                {
                    "command": str(item.get("command") or ""),
                    "aggregated_output": str(item.get("aggregatedOutput") or ""),
                    "exit_code": item.get("exitCode"),
                    "status": str(item.get("status") or ""),
                }
            )
            return result
        if normalized_type == "file_change":
            changes = item.get("changes") if isinstance(item.get("changes"), list) else []
            result["changes"] = [
                {
                    "path": str(change.get("path") or ""),
                    "kind": str(change.get("kind") or ""),
                }
                for change in changes
                if isinstance(change, dict)
            ]
            result["status"] = str(item.get("status") or "")
            return result
        if normalized_type == "reasoning":
            summary = item.get("summary") if isinstance(item.get("summary"), list) else []
            result["text"] = "\n".join(str(part) for part in summary if part)
            return result
        if normalized_type == "plan":
            result["text"] = str(item.get("text") or "")
            return result
        if normalized_type == "mcp_tool_call":
            result["server"] = str(item.get("server") or "")
            result["tool"] = str(item.get("tool") or "")
            result["status"] = str(item.get("status") or "")
            return result
        if normalized_type == "web_search":
            result["query"] = str(item.get("query") or "")
            return result
        return result

    @staticmethod
    def _format_rpc_error(error: Any) -> str:
        if not isinstance(error, dict):
            return str(error)
        message = str(error.get("message") or "RPC request failed")
        code = error.get("code")
        if code is None:
            return message
        return f"{message} (code={code})"

    def _format_recent_stderr(self) -> str:
        if not self._recent_stderr_lines:
            return ""
        return "\n".join(self._recent_stderr_lines)


class CodexAppServerRunner:
    def __init__(self, default_bin: str = "codex"):
        self.default_bin = default_bin
        self._runtimes: dict[str, CodexAppServerRuntime] = {}

    def _runtime_for_route(self, route: Any) -> CodexAppServerRuntime:
        runtime = self._runtimes.get(route.name)
        if runtime is None:
            runtime = CodexAppServerRuntime(route=route, default_bin=self.default_bin)
            self._runtimes[route.name] = runtime
        return runtime

    async def run(
        self,
        *,
        prompt: str,
        route: Any,
        session_id: str | None,
        image_paths: list[str],
        run_key: str | None = None,
        on_progress: ProgressCallback | None = None,
        developer_instructions: str | None = None,
    ) -> AppServerRunResult:
        runtime = self._runtime_for_route(route)
        try:
            return await runtime.run(
                prompt=prompt,
                session_id=session_id,
                image_paths=image_paths,
                run_key=run_key,
                on_progress=on_progress,
                developer_instructions=developer_instructions,
            )
        except Exception:
            with contextlib.suppress(Exception):
                await runtime.close()
            self._runtimes.pop(route.name, None)
            raise

    async def interrupt(self, run_key: str) -> bool:
        for runtime in self._runtimes.values():
            interrupted = await runtime.interrupt(run_key)
            if interrupted:
                return True
        return False

    async def release_route(self, route_name: str) -> bool:
        runtime = self._runtimes.get(route_name)
        if runtime is None or not runtime.is_running():
            return False
        await runtime.close()
        self._runtimes.pop(route_name, None)
        return True

    def route_status(self, route_name: str) -> dict[str, Any] | None:
        runtime = self._runtimes.get(route_name)
        if runtime is None:
            return None
        return runtime.status_snapshot()

    async def close(self) -> None:
        for runtime in list(self._runtimes.values()):
            await runtime.close()
        self._runtimes.clear()
