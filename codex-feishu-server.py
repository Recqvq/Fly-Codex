from __future__ import annotations

import argparse
import contextlib
import asyncio
import hashlib
import json
import os
import re
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

from codex_app_server import CodexAppServerRunner
from config import APP_ID, APP_SECRET
from feishu_bot import FeishuAttachment, FeishuBot, FeishuInboundMessage, IMAGE_SUFFIXES

DEFAULT_ROUTES_FILE = "routes.json"
DEFAULT_SESSION_STORE = ".fly-codex-sessions.json"
DEFAULT_CACHE_DIR = ".fly-codex"
RECENT_ARTIFACT_SUFFIXES = IMAGE_SUFFIXES | {
    ".pdf",
    ".csv",
    ".tsv",
    ".xlsx",
    ".xls",
    ".docx",
    ".doc",
    ".pptx",
    ".ppt",
    ".zip",
    ".txt",
    ".md",
    ".json",
}
BLOCKED_ARTIFACT_SUFFIXES = {
    ".py",
    ".pyi",
    ".ipynb",
    ".js",
    ".jsx",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".java",
    ".kt",
    ".kts",
    ".scala",
    ".go",
    ".rs",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hpp",
    ".m",
    ".mm",
    ".swift",
    ".rb",
    ".php",
    ".sh",
    ".bash",
    ".zsh",
    ".fish",
    ".ps1",
    ".bat",
    ".cmd",
    ".pl",
    ".pm",
    ".lua",
    ".r",
    ".jl",
    ".dart",
    ".ex",
    ".exs",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".env",
    ".lock",
}
IGNORE_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".mypy_cache", ".pytest_cache"}
ARTIFACT_LINE_RE = re.compile(r"^(MEDIA|FILE):\s*(.+?)\s*$", re.IGNORECASE)
COMMAND_OUTPUT_PREVIEW = 1200
MESSAGE_PREVIEW_LIMIT = 160
INPUT_BUNDLE_IDLE_SECONDS = 8.0
INPUT_BUNDLE_MAX_WAIT_SECONDS = 20.0
DEFAULT_SHOW_PROGRESS = True
DEFAULT_PROGRESS_DELAY_SECONDS = 8.0
DEFAULT_PROGRESS_FORCE_SHOW_SECONDS = 15.0
DEFAULT_PROGRESS_KEEPALIVE_SECONDS = 3.0
DEFAULT_APP_SERVER_IDLE_TIMEOUT_SECONDS = 900.0
PROGRESS_UPDATE_MIN_INTERVAL_SECONDS = 1.2
SESSION_STORE_VERSION = 4
DEFAULT_CONTEXT_NAME = "default"


@dataclass(slots=True)
class RouteConfig:
    name: str
    chat_id: str
    workdir: str
    allowed_senders: list[str] = field(default_factory=list)
    codex_bin: str | None = None
    codex_args: list[str] = field(default_factory=list)
    auto_send_recent_artifacts: bool = True
    max_auto_artifacts: int = 3
    show_progress: bool = DEFAULT_SHOW_PROGRESS
    progress_delay_seconds: float = DEFAULT_PROGRESS_DELAY_SECONDS
    progress_force_show_seconds: float = DEFAULT_PROGRESS_FORCE_SHOW_SECONDS
    progress_keepalive_seconds: float = DEFAULT_PROGRESS_KEEPALIVE_SECONDS
    app_server_idle_timeout_seconds: float = DEFAULT_APP_SERVER_IDLE_TIMEOUT_SECONDS


@dataclass(slots=True)
class Artifact:
    kind: str
    path: Path


@dataclass(slots=True)
class CodexRunResult:
    output: str
    session_id: str | None
    success: bool
    interrupted: bool = False
    event_lines: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class InputBundle:
    bundle_key: str
    route_name: str
    reply_target: str
    chat_id: str
    chat_type: str
    sender_id: str
    sender_name: str | None
    root_id: str | None
    parent_id: str | None
    thread_id: str | None
    message_id: str
    message_type: str
    texts: list[str] = field(default_factory=list)
    attachments: list[FeishuAttachment] = field(default_factory=list)
    first_at: float = 0.0
    last_at: float = 0.0
    due_at: float = 0.0
    generation: int = 0
    flush_task: asyncio.Task[Any] | None = None


@dataclass(slots=True)
class QueuedTask:
    message: FeishuInboundMessage
    context_name: str
    context_scope_key: str


@dataclass(slots=True)
class ProgressCardState:
    task_key: str
    reply_target: str
    route_name: str
    workdir: str
    context_name: str
    message_preview: str
    started_at: float
    show_delay_seconds: float = DEFAULT_PROGRESS_DELAY_SECONDS
    force_show_seconds: float = DEFAULT_PROGRESS_FORCE_SHOW_SECONDS
    keepalive_seconds: float = DEFAULT_PROGRESS_KEEPALIVE_SECONDS
    status: str = "任务已启动，正在等待 Codex 建立上下文。"
    visible: bool = False
    done: bool = False
    message_id: str | None = None
    last_render_key: str = ""
    last_render_at: float = 0.0
    show_task: asyncio.Task[Any] | None = None
    keepalive_task: asyncio.Task[Any] | None = None
    render_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    has_meaningful_activity: bool = False


class RouteTable:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).expanduser().resolve()
        self.defaults: dict[str, Any] = {}
        self.routes_by_chat: dict[str, RouteConfig] = {}
        self.routes_by_name: dict[str, RouteConfig] = {}
        self._load()

    def _load(self) -> None:
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Route config not found: {self.config_path}. Please create it from routes.example.json."
            )
        raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("routes.json must be a JSON object")
        defaults = raw.get("defaults") or {}
        if not isinstance(defaults, dict):
            raise ValueError("defaults must be an object")
        self.defaults = defaults
        routes = raw.get("routes") or []
        if not isinstance(routes, list):
            raise ValueError("routes must be a JSON array")
        self.routes_by_chat.clear()
        self.routes_by_name.clear()
        for entry in routes:
            if not isinstance(entry, dict):
                raise ValueError("each route must be an object")
            name = str(entry.get("name", "")).strip()
            chat_id = str(entry.get("chat_id", "")).strip()
            workdir = str(entry.get("workdir", "")).strip()
            if not name or not chat_id or not workdir:
                raise ValueError(f"route requires name/chat_id/workdir: {entry}")
            route = RouteConfig(
                name=name,
                chat_id=chat_id,
                workdir=os.path.abspath(os.path.expanduser(workdir)),
                allowed_senders=[
                    str(item).strip()
                    for item in (entry.get("allowed_senders") or defaults.get("allowed_senders") or [])
                    if str(item).strip()
                ],
                codex_bin=str(entry.get("codex_bin") or defaults.get("codex_bin") or "codex").strip() or "codex",
                codex_args=[
                    str(item).strip()
                    for item in (entry.get("codex_args") or defaults.get("codex_args") or [])
                    if str(item).strip()
                ],
                auto_send_recent_artifacts=bool(
                    entry.get(
                        "auto_send_recent_artifacts",
                        defaults.get("auto_send_recent_artifacts", True),
                    )
                ),
                max_auto_artifacts=max(
                    0,
                    int(entry.get("max_auto_artifacts") or defaults.get("max_auto_artifacts") or 3),
                ),
                show_progress=bool(entry.get("show_progress", defaults.get("show_progress", DEFAULT_SHOW_PROGRESS))),
                progress_delay_seconds=max(
                    0.0,
                    float(
                        entry.get(
                            "progress_delay_seconds",
                            defaults.get("progress_delay_seconds", DEFAULT_PROGRESS_DELAY_SECONDS),
                        )
                    ),
                ),
                progress_force_show_seconds=max(
                    8.0,
                    float(
                        entry.get(
                            "progress_force_show_seconds",
                            defaults.get("progress_force_show_seconds", DEFAULT_PROGRESS_FORCE_SHOW_SECONDS),
                        )
                    ),
                ),
                progress_keepalive_seconds=max(
                    2.0,
                    float(
                        entry.get(
                            "progress_keepalive_seconds",
                            defaults.get("progress_keepalive_seconds", DEFAULT_PROGRESS_KEEPALIVE_SECONDS),
                        )
                    ),
                ),
                app_server_idle_timeout_seconds=max(
                    0.0,
                    float(
                        entry.get(
                            "app_server_idle_timeout_seconds",
                            defaults.get("app_server_idle_timeout_seconds", DEFAULT_APP_SERVER_IDLE_TIMEOUT_SECONDS),
                        )
                    ),
                ),
            )
            if not os.path.isdir(route.workdir):
                raise ValueError(f"route {route.name} workdir does not exist: {route.workdir}")
            if route.chat_id in self.routes_by_chat:
                raise ValueError(f"duplicate chat_id in routes: {route.chat_id}")
            self.routes_by_chat[route.chat_id] = route
            self.routes_by_name[route.name] = route

    def resolve(self, chat_id: str) -> RouteConfig | None:
        return self.routes_by_chat.get(chat_id)

    def summary_lines(self) -> list[str]:
        lines = []
        for route in self.routes_by_name.values():
            lines.append(f"- {route.name}: {route.chat_id} -> {route.workdir}")
        return lines


class SessionStore:
    def __init__(self, store_path: str, routes: RouteTable, legacy_path: str | None = None):
        self.store_path = Path(store_path).expanduser().resolve()
        self.routes = routes
        self.legacy_path = Path(legacy_path).expanduser().resolve() if legacy_path else None
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()
        self._migrate_legacy_if_needed()

    def _load(self) -> dict[str, Any]:
        if not self.store_path.exists():
            return {
                "version": SESSION_STORE_VERSION,
                "sessions": {},
                "scope_metrics": {},
                "route_metrics": {},
                "chat_contexts": {},
            }
        try:
            data = json.loads(self.store_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("version", SESSION_STORE_VERSION)
                data.setdefault("sessions", {})
                data.setdefault("scope_metrics", {})
                data.setdefault("route_metrics", {})
                data.setdefault("chat_contexts", {})
                return data
        except Exception as exc:
            logger.warning("Failed to load session store {}: {}", self.store_path, exc)
        return {
            "version": SESSION_STORE_VERSION,
            "sessions": {},
            "scope_metrics": {},
            "route_metrics": {},
            "chat_contexts": {},
        }

    def _save(self) -> None:
        self._data["version"] = SESSION_STORE_VERSION
        self._data.setdefault("sessions", {})
        self._data.setdefault("scope_metrics", {})
        self._data.setdefault("route_metrics", {})
        self._data.setdefault("chat_contexts", {})
        temp_path = self.store_path.with_suffix(f"{self.store_path.suffix}.tmp")
        temp_path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temp_path, self.store_path)

    def _migrate_legacy_if_needed(self) -> None:
        if not self.legacy_path or not self.legacy_path.exists():
            return
        sessions = self._data.setdefault("sessions", {})
        if sessions:
            return
        try:
            legacy = json.loads(self.legacy_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read legacy session store {}: {}", self.legacy_path, exc)
            return
        workspaces = legacy.get("workspaces") if isinstance(legacy, dict) else None
        if not isinstance(workspaces, dict):
            return
        migrated = 0
        for route in self.routes.routes_by_name.values():
            legacy_sessions = workspaces.get(route.workdir)
            if not isinstance(legacy_sessions, dict):
                continue
            for chat_id, entry in legacy_sessions.items():
                if not isinstance(entry, dict):
                    continue
                session_id = str(entry.get("thread_id", "")).strip()
                if not session_id:
                    continue
                key = self._session_storage_key(route.name, f"chat:{chat_id}")
                sessions[key] = {
                    "route_name": route.name,
                    "chat_id": chat_id,
                    "scope_key": f"chat:{chat_id}",
                    "session_id": session_id,
                    "updated_at": int(entry.get("updated_at") or time.time()),
                    "workdir": route.workdir,
                }
                migrated += 1
        if migrated:
            logger.info("Migrated {} legacy Codex session(s) from {}", migrated, self.legacy_path)
            self._save()

    @staticmethod
    def _session_storage_key(route_name: str, scope_key: str) -> str:
        return f"{route_name}::{scope_key}"

    @staticmethod
    def _chat_context_storage_key(route_name: str, chat_scope_key: str) -> str:
        return f"{route_name}::{chat_scope_key}"

    @staticmethod
    def _normalize_context_name(name: str) -> str:
        compact = re.sub(r"\s+", " ", name or "").strip()
        return compact[:80]

    @staticmethod
    def _context_scope_key(chat_scope_key: str, context_name: str) -> str:
        normalized = SessionStore._normalize_context_name(context_name)
        if not normalized or normalized.casefold() == DEFAULT_CONTEXT_NAME:
            return chat_scope_key
        slug_base = re.sub(r"[^a-zA-Z0-9._-]+", "-", normalized).strip("-_.").lower()
        digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
        if slug_base:
            slug_base = slug_base[:40].rstrip("-_.")
            slug = f"{slug_base}-{digest}"
        else:
            slug = f"ctx-{digest}"
        return f"{chat_scope_key}:ctx:{slug}"

    @staticmethod
    def _max_timestamp(*values: Any) -> int:
        numbers = [int(value) for value in values if value]
        return max(numbers) if numbers else 0

    def _ensure_chat_context_entry(self, route: RouteConfig, chat_scope_key: str) -> dict[str, Any]:
        storage_key = self._chat_context_storage_key(route.name, chat_scope_key)
        chat_contexts = self._data.setdefault("chat_contexts", {})
        entry = chat_contexts.get(storage_key)
        if not isinstance(entry, dict):
            entry = {
                "route_name": route.name,
                "chat_id": route.chat_id,
                "chat_scope_key": chat_scope_key,
                "active_context": DEFAULT_CONTEXT_NAME,
                "contexts": {},
                "updated_at": int(time.time()),
                "workdir": route.workdir,
            }
            chat_contexts[storage_key] = entry
        contexts = entry.get("contexts")
        if not isinstance(contexts, dict):
            contexts = {}
            entry["contexts"] = contexts
        if DEFAULT_CONTEXT_NAME not in contexts or not isinstance(contexts.get(DEFAULT_CONTEXT_NAME), dict):
            contexts[DEFAULT_CONTEXT_NAME] = {
                "name": DEFAULT_CONTEXT_NAME,
                "scope_key": chat_scope_key,
                "created_at": int(time.time()),
                "updated_at": int(time.time()),
            }
        active_context = str(entry.get("active_context") or "").strip() or DEFAULT_CONTEXT_NAME
        if active_context not in contexts:
            active_context = DEFAULT_CONTEXT_NAME
        entry["active_context"] = active_context
        entry["route_name"] = route.name
        entry["chat_id"] = route.chat_id
        entry["chat_scope_key"] = chat_scope_key
        entry["workdir"] = route.workdir
        return entry

    def get(self, route: RouteConfig, scope_key: str) -> str | None:
        entry = self._data.setdefault("sessions", {}).get(self._session_storage_key(route.name, scope_key))
        if isinstance(entry, dict):
            session_id = entry.get("session_id")
            if isinstance(session_id, str) and session_id.strip():
                return session_id.strip()
        return None

    def set(
        self,
        route: RouteConfig,
        scope_key: str,
        session_id: str,
        *,
        chat_scope_key: str | None = None,
        context_name: str | None = None,
    ) -> None:
        now = int(time.time())
        self._data.setdefault("sessions", {})[self._session_storage_key(route.name, scope_key)] = {
            "route_name": route.name,
            "chat_id": route.chat_id,
            "scope_key": scope_key,
            "session_id": session_id,
            "updated_at": now,
            "workdir": route.workdir,
        }
        if chat_scope_key:
            entry = self._ensure_chat_context_entry(route, chat_scope_key)
            contexts = entry.setdefault("contexts", {})
            target_name = None
            normalized_name = self._normalize_context_name(context_name or "")
            if normalized_name:
                for existing_name in contexts:
                    if existing_name.casefold() == normalized_name.casefold():
                        target_name = existing_name
                        break
                if target_name is None:
                    target_name = normalized_name
                    contexts[target_name] = {
                        "name": target_name,
                        "scope_key": scope_key,
                        "created_at": now,
                        "updated_at": now,
                    }
            else:
                for existing_name, metadata in contexts.items():
                    if isinstance(metadata, dict) and str(metadata.get("scope_key") or "") == scope_key:
                        target_name = existing_name
                        break
            if target_name is not None:
                metadata = contexts.setdefault(target_name, {})
                metadata["name"] = target_name
                metadata["scope_key"] = scope_key
                metadata["created_at"] = int(metadata.get("created_at") or now)
                metadata["updated_at"] = now
                entry["active_context"] = target_name
                entry["updated_at"] = now
        self._save()

    def clear(self, route: RouteConfig, scope_key: str) -> bool:
        storage_key = self._session_storage_key(route.name, scope_key)
        removed = self._data.setdefault("sessions", {}).pop(storage_key, None)
        removed_metrics = self._data.setdefault("scope_metrics", {}).pop(storage_key, None)
        if removed is not None or removed_metrics is not None:
            self._save()
            return True
        return False

    def describe(self, route: RouteConfig, scope_key: str) -> dict[str, Any] | None:
        entry = self._data.setdefault("sessions", {}).get(self._session_storage_key(route.name, scope_key))
        return entry if isinstance(entry, dict) else None

    def resolve_context(self, route: RouteConfig, chat_scope_key: str) -> dict[str, Any]:
        entry = self._ensure_chat_context_entry(route, chat_scope_key)
        contexts = entry.get("contexts") if isinstance(entry.get("contexts"), dict) else {}
        active_name = str(entry.get("active_context") or DEFAULT_CONTEXT_NAME)
        context_meta = contexts.get(active_name) if isinstance(contexts, dict) else None
        if not isinstance(context_meta, dict):
            active_name = DEFAULT_CONTEXT_NAME
            context_meta = contexts.get(active_name) if isinstance(contexts, dict) else None
        scope_key = str((context_meta or {}).get("scope_key") or chat_scope_key)
        session = self.describe(route, scope_key) or {}
        metrics = self.describe_scope_metrics(route, scope_key) or {}
        session_id = self.get(route, scope_key)
        return {
            "name": active_name,
            "scope_key": scope_key,
            "active": True,
            "has_session": bool(session_id),
            "session_id": session_id,
            "updated_at": self._max_timestamp(
                (context_meta or {}).get("updated_at"),
                session.get("updated_at"),
                metrics.get("updated_at"),
            ),
            "last_usage": metrics.get("last_usage") if isinstance(metrics, dict) else None,
        }

    def list_contexts(self, route: RouteConfig, chat_scope_key: str) -> list[dict[str, Any]]:
        entry = self._ensure_chat_context_entry(route, chat_scope_key)
        contexts = entry.get("contexts") if isinstance(entry.get("contexts"), dict) else {}
        active_name = str(entry.get("active_context") or DEFAULT_CONTEXT_NAME)
        result: list[dict[str, Any]] = []
        if not isinstance(contexts, dict):
            return result
        for context_name, metadata in contexts.items():
            if not isinstance(metadata, dict):
                continue
            scope_key = str(metadata.get("scope_key") or chat_scope_key)
            session = self.describe(route, scope_key) or {}
            metrics = self.describe_scope_metrics(route, scope_key) or {}
            session_id = self.get(route, scope_key)
            result.append(
                {
                    "name": context_name,
                    "scope_key": scope_key,
                    "active": context_name == active_name,
                    "has_session": bool(session_id),
                    "session_id": session_id,
                    "updated_at": self._max_timestamp(
                        metadata.get("updated_at"),
                        session.get("updated_at"),
                        metrics.get("updated_at"),
                    ),
                    "last_usage": metrics.get("last_usage") if isinstance(metrics, dict) else None,
                }
            )
        result.sort(
            key=lambda item: (
                0 if item.get("active") else 1,
                -int(item.get("updated_at") or 0),
                str(item.get("name") or "").casefold(),
            )
        )
        return result

    def create_context(self, route: RouteConfig, chat_scope_key: str, context_name: str) -> tuple[dict[str, Any], bool]:
        normalized_name = self._normalize_context_name(context_name)
        if not normalized_name:
            raise ValueError("上下文名称不能为空。")
        entry = self._ensure_chat_context_entry(route, chat_scope_key)
        contexts = entry.setdefault("contexts", {})
        for existing_name in contexts:
            if existing_name.casefold() == normalized_name.casefold():
                entry["active_context"] = existing_name
                entry["updated_at"] = int(time.time())
                self._save()
                return self.resolve_context(route, chat_scope_key), False
        now = int(time.time())
        contexts[normalized_name] = {
            "name": normalized_name,
            "scope_key": self._context_scope_key(chat_scope_key, normalized_name),
            "created_at": now,
            "updated_at": now,
        }
        entry["active_context"] = normalized_name
        entry["updated_at"] = now
        self._save()
        return self.resolve_context(route, chat_scope_key), True

    def activate_context(self, route: RouteConfig, chat_scope_key: str, context_name: str) -> dict[str, Any] | None:
        normalized_name = self._normalize_context_name(context_name)
        if not normalized_name:
            return None
        entry = self._ensure_chat_context_entry(route, chat_scope_key)
        contexts = entry.get("contexts") if isinstance(entry.get("contexts"), dict) else {}
        if not isinstance(contexts, dict):
            return None
        for existing_name in contexts:
            if existing_name.casefold() != normalized_name.casefold():
                continue
            entry["active_context"] = existing_name
            entry["updated_at"] = int(time.time())
            self._save()
            return self.resolve_context(route, chat_scope_key)
        return None

    def count_contexts(self, route: RouteConfig) -> int:
        chat_scope_key = f"chat:{route.chat_id}"
        entry = self._ensure_chat_context_entry(route, chat_scope_key)
        contexts = entry.get("contexts") if isinstance(entry.get("contexts"), dict) else {}
        return len(contexts) if isinstance(contexts, dict) else 0

    def count(self, route: RouteConfig | None = None) -> int:
        sessions = self._data.setdefault("sessions", {})
        if route is None:
            return len(sessions)
        prefix = f"{route.name}::"
        return sum(1 for key in sessions if key.startswith(prefix))

    @staticmethod
    def _empty_usage() -> dict[str, int]:
        return {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
        }

    @staticmethod
    def _preview_text(text: str, limit: int = MESSAGE_PREVIEW_LIMIT) -> str:
        compact = re.sub(r"\s+", " ", text or "").strip()
        if not compact:
            return "(空消息或仅附件)"
        return compact[:limit] + ("…" if len(compact) > limit else "")

    def _ensure_scope_metrics(self, route: RouteConfig, scope_key: str) -> dict[str, Any]:
        storage_key = self._session_storage_key(route.name, scope_key)
        scope_metrics = self._data.setdefault("scope_metrics", {})
        entry = scope_metrics.get(storage_key)
        if not isinstance(entry, dict):
            entry = {
                "route_name": route.name,
                "chat_id": route.chat_id,
                "scope_key": scope_key,
                "workdir": route.workdir,
                "task_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "usage_totals": self._empty_usage(),
            }
            scope_metrics[storage_key] = entry
        return entry

    def _ensure_route_metrics(self, route: RouteConfig) -> dict[str, Any]:
        route_metrics = self._data.setdefault("route_metrics", {})
        entry = route_metrics.get(route.name)
        if not isinstance(entry, dict):
            entry = {
                "route_name": route.name,
                "chat_id": route.chat_id,
                "workdir": route.workdir,
                "task_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "usage_totals": self._empty_usage(),
            }
            route_metrics[route.name] = entry
        return entry

    @staticmethod
    def _normalize_usage(usage: dict[str, Any] | None) -> dict[str, int] | None:
        if not isinstance(usage, dict):
            return None
        return {
            "input_tokens": int(usage.get("input_tokens") or 0),
            "cached_input_tokens": int(usage.get("cached_input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
        }

    def record_run(
        self,
        route: RouteConfig,
        scope_key: str,
        *,
        message_text: str,
        success: bool,
        usage: dict[str, Any] | None,
        last_successful_command: dict[str, Any] | None,
    ) -> None:
        now = int(time.time())
        normalized_usage = self._normalize_usage(usage)
        metrics_targets = [
            self._ensure_scope_metrics(route, scope_key),
            self._ensure_route_metrics(route),
        ]
        preview = self._preview_text(message_text)
        for metrics in metrics_targets:
            metrics["task_count"] = int(metrics.get("task_count") or 0) + 1
            if success:
                metrics["success_count"] = int(metrics.get("success_count") or 0) + 1
            else:
                metrics["failure_count"] = int(metrics.get("failure_count") or 0) + 1
            metrics["updated_at"] = now
            metrics["last_message_preview"] = preview
            if normalized_usage:
                totals = metrics.setdefault("usage_totals", self._empty_usage())
                for key, value in normalized_usage.items():
                    totals[key] = int(totals.get(key) or 0) + value
                metrics["last_usage"] = {**normalized_usage, "updated_at": now}
            if isinstance(last_successful_command, dict):
                metrics["last_successful_command"] = {
                    "command": str(last_successful_command.get("command") or "").strip(),
                    "output": str(last_successful_command.get("output") or "")[:COMMAND_OUTPUT_PREVIEW],
                    "exit_code": int(last_successful_command.get("exit_code") or 0),
                    "updated_at": now,
                }
        self._save()

    def describe_scope_metrics(self, route: RouteConfig, scope_key: str) -> dict[str, Any] | None:
        entry = self._data.setdefault("scope_metrics", {}).get(self._session_storage_key(route.name, scope_key))
        return entry if isinstance(entry, dict) else None

    def describe_route_metrics(self, route: RouteConfig) -> dict[str, Any] | None:
        entry = self._data.setdefault("route_metrics", {}).get(route.name)
        return entry if isinstance(entry, dict) else None


class CodexExecRunner:
    def __init__(self, default_bin: str = "codex"):
        self.default_bin = default_bin
        self._active_processes: dict[str, asyncio.subprocess.Process] = {}
        self._interrupted_runs: set[str] = set()

    async def run(
        self,
        *,
        prompt: str,
        route: RouteConfig,
        session_id: str | None,
        image_paths: list[str],
        run_key: str | None = None,
        on_progress: callable | None = None,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        output_path = None
        env = os.environ.copy()
        env.setdefault("NO_COLOR", "1")
        events: list[dict[str, Any]] = []
        resolved_session_id = session_id
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                prefix="fly-codex-last-message-",
                suffix=".txt",
            ) as temp_file:
                output_path = temp_file.name
            cmd = self._build_cmd(prompt=prompt, output_path=output_path, route=route, session_id=session_id, image_paths=image_paths)
            logger.info("Running Codex route={} mode={} workdir={}", route.name, "resume" if session_id else "new", route.workdir)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=route.workdir,
                env=env,
            )
            if run_key:
                self._active_processes[run_key] = proc

            last_command_preview = ""

            async def read_stdout() -> None:
                nonlocal last_command_preview
                nonlocal resolved_session_id
                assert proc.stdout is not None
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    line_text = line.decode("utf-8", errors="replace").strip()
                    if not line_text:
                        continue
                    try:
                        event = json.loads(line_text)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    events.append(event)
                    event_type = str(event.get("type", "")).strip()
                    if event_type == "thread.started":
                        thread_id = str(event.get("thread_id", "")).strip()
                        if thread_id:
                            resolved_session_id = thread_id
                            if on_progress:
                                await on_progress("上下文接好了，我开始动手。")
                    elif event_type == "turn.started" and on_progress:
                        await on_progress("我正在项目里翻代码、跑命令。")
                    elif event_type == "item.completed" and on_progress:
                        item = event.get("item")
                        if isinstance(item, dict) and item.get("type") == "command_execution":
                            command = re.sub(r"\s+", " ", str(item.get("command") or "")).strip()
                            if command:
                                preview = command[:80] + ("…" if len(command) > 80 else "")
                                if preview != last_command_preview:
                                    last_command_preview = preview
                                    await on_progress(f"刚执行命令：`{preview}`")
                    elif event_type == "error":
                        message = str(event.get("message", "")).strip()
                        if on_progress and message and not message.startswith("Reconnecting..."):
                            await on_progress(f"中途冒了个提示：{message}")

            async def read_stderr() -> str:
                assert proc.stderr is not None
                content = await proc.stderr.read()
                return content.decode("utf-8", errors="replace").strip()

            stderr_task = asyncio.create_task(read_stderr())
            stdout_task = asyncio.create_task(read_stdout())
            await asyncio.gather(stdout_task)
            return_code = await proc.wait()
            stderr_text = await stderr_task
            interrupted = bool(run_key and run_key in self._interrupted_runs)

            result_text = ""
            if output_path and os.path.exists(output_path):
                result_text = Path(output_path).read_text(encoding="utf-8", errors="replace").strip()
            if not result_text:
                result_text = self._extract_last_agent_message(events)

            if interrupted or return_code in {-15, -9}:
                return CodexRunResult(
                    output=result_text or "任务已中断。",
                    session_id=resolved_session_id,
                    success=False,
                    interrupted=True,
                    event_lines=events,
                )

            if return_code != 0:
                parts = [f"命令退出码：{return_code}"]
                if result_text:
                    parts.append(result_text)
                if stderr_text:
                    parts.append(stderr_text)
                return CodexRunResult(
                    output="\n\n".join(part for part in parts if part).strip() or "Codex 执行失败。",
                    session_id=resolved_session_id,
                    success=False,
                    interrupted=False,
                    event_lines=events,
                )

            if not result_text:
                result_text = "Codex 没有返回可显示的最终文本。"
            return CodexRunResult(
                output=result_text,
                session_id=resolved_session_id,
                success=True,
                interrupted=False,
                event_lines=events,
            )
        finally:
            if run_key:
                self._active_processes.pop(run_key, None)
                self._interrupted_runs.discard(run_key)
            if output_path and os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except OSError:
                    pass

    async def interrupt(self, run_key: str) -> bool:
        proc = self._active_processes.get(run_key)
        if proc is None or proc.returncode is not None:
            return False
        self._interrupted_runs.add(run_key)
        try:
            proc.terminate()
        except ProcessLookupError:
            return False
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        return True

    def _build_cmd(
        self,
        *,
        prompt: str,
        output_path: str,
        route: RouteConfig,
        session_id: str | None,
        image_paths: list[str],
    ) -> list[str]:
        common_args = [
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
            "--json",
            "-o",
            output_path,
        ]
        if route.codex_args:
            common_args.extend(route.codex_args)
        for image_path in image_paths:
            common_args.extend(["-i", image_path])
        codex_bin = route.codex_bin or self.default_bin
        if session_id:
            return [codex_bin, "exec", "resume", *common_args, session_id, prompt]
        return [codex_bin, "exec", *common_args, prompt]

    @staticmethod
    def _extract_last_agent_message(events: list[dict[str, Any]]) -> str:
        last_message = ""
        for event in events:
            if event.get("type") != "item.completed":
                continue
            item = event.get("item")
            if not isinstance(item, dict):
                continue
            if item.get("type") == "agent_message":
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    last_message = text.strip()
        return last_message


class CodexRunner:
    def __init__(self, default_bin: str = "codex", backend: str = "exec"):
        self.default_bin = default_bin
        self.backend = backend
        self._exec_runner = CodexExecRunner(default_bin=default_bin)
        self._app_server_runner = CodexAppServerRunner(default_bin=default_bin)

    async def run(
        self,
        *,
        prompt: str,
        route: RouteConfig,
        session_id: str | None,
        image_paths: list[str],
        run_key: str | None = None,
        on_progress: callable | None = None,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        if self.backend == "app_server":
            result = await self._app_server_runner.run(
                prompt=prompt,
                route=route,
                session_id=session_id,
                image_paths=image_paths,
                run_key=run_key,
                on_progress=on_progress,
                developer_instructions=developer_instructions,
            )
            return CodexRunResult(
                output=result.output,
                session_id=result.session_id,
                success=result.success,
                interrupted=result.interrupted,
                event_lines=result.event_lines,
            )
        return await self._exec_runner.run(
            prompt=prompt,
            route=route,
            session_id=session_id,
            image_paths=image_paths,
            run_key=run_key,
            on_progress=on_progress,
            developer_instructions=developer_instructions,
        )

    async def interrupt(self, run_key: str) -> bool:
        if self.backend == "app_server":
            return await self._app_server_runner.interrupt(run_key)
        return await self._exec_runner.interrupt(run_key)

    async def release_route(self, route_name: str) -> bool:
        if self.backend == "app_server":
            return await self._app_server_runner.release_route(route_name)
        return False

    def route_status(self, route_name: str) -> dict[str, Any] | None:
        if self.backend == "app_server":
            return self._app_server_runner.route_status(route_name)
        return None

    async def close(self) -> None:
        await self._app_server_runner.close()


class MultiRouteCodexFeishuService:
    def __init__(
        self,
        *,
        routes_path: str,
        session_store_path: str,
        codex_bin: str,
        backend: str,
        state_dir: str,
        legacy_session_store: str | None,
    ):
        self.routes = RouteTable(routes_path)
        self.bot = FeishuBot(APP_ID, APP_SECRET)
        self.sessions = SessionStore(session_store_path, self.routes, legacy_session_store)
        self.backend = backend
        self.runner = CodexRunner(default_bin=codex_bin, backend=backend)
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._active_tasks: dict[str, bool] = {}
        self._pending_messages: dict[str, deque[QueuedTask]] = {}
        self._running_meta: dict[str, dict[str, Any]] = {}
        self._progress_cards: dict[str, ProgressCardState] = {}
        self._input_bundles: dict[str, InputBundle] = {}
        self._dispatch_lock = asyncio.Lock()
        self._bundle_lock = asyncio.Lock()
        self.bot.on_message(self._handle_message)

    async def start(self) -> None:
        logger.info("Starting FlyingCodex with {} route(s)", len(self.routes.routes_by_name))
        logger.info("Backend mode: {}", self.backend)
        for line in self.routes.summary_lines():
            logger.info(line)
        await self.bot.start()

    async def shutdown(self) -> None:
        await self.runner.close()

    async def _handle_message(self, message: FeishuInboundMessage) -> None:
        route = self.routes.resolve(message.chat_id)
        if route is None:
            logger.info("Ignoring message from unmapped chat {}", message.chat_id)
            return
        if route.allowed_senders and message.sender_id not in route.allowed_senders:
            logger.info("Ignoring message from unauthorized sender {} in route {}", message.sender_id, route.name)
            return

        chat_scope_key = self._build_scope_key(route, message)
        task_key = f"{route.name}::{chat_scope_key}"
        input_bundle_key = self._build_input_bundle_key(message)
        raw_text = message.text.strip()
        command = raw_text.split(None, 1)[0].lower() if raw_text else ""
        if command == "/help":
            await self.bot.send_card(message.reply_target, "使用说明", self._build_help_text(route))
            return
        if command in {"/ctx", "/context", "/contexts"}:
            await self._handle_context_command(route, message, chat_scope_key, task_key, raw_text)
            return
        if command in {"/status", "status"}:
            await self.bot.send_card(
                message.reply_target,
                "群状态",
                self._build_status_text(route, chat_scope_key, task_key, input_bundle_key),
            )
            return
        if command == "/usage":
            await self.bot.send_card(message.reply_target, "上下文用量", self._build_usage_text(route, chat_scope_key))
            return
        if command == "/lastcmd":
            await self.bot.send_card(message.reply_target, "上一条成功命令", self._build_last_command_text(route, chat_scope_key))
            return
        if command in {"/interrupt", "/stop", "/cancel"}:
            stopped = await self.runner.interrupt(task_key)
            if stopped:
                await self._update_progress_status(task_key, "已发送中断信号，正在等待 Codex 收尾。", force=True)
                pending_count = len(self._pending_messages.get(task_key) or ())
                text = "当前正在跑的任务已发送中断信号。"
                if pending_count:
                    text += f" 后面还排着 {pending_count} 条，会继续顺序执行。"
                await self.bot.send_text(message.reply_target, text)
            else:
                await self.bot.send_text(message.reply_target, "当前没有可中断的运行中任务。")
            return
        if command == "/clearqueue":
            cleared = await self._clear_pending_queue(task_key)
            if cleared:
                await self.bot.send_text(message.reply_target, f"已清空排队中的 {cleared} 条消息；当前正在跑的这条不受影响。")
            else:
                await self.bot.send_text(message.reply_target, "当前没有排队中的消息。")
            return
        if command == "/send":
            bundled_message = await self._submit_input_bundle(input_bundle_key)
            if bundled_message is None:
                await self.bot.send_text(message.reply_target, "当前没有待提交的图文输入。")
                return
            await self.bot.send_text(message.reply_target, "已提交当前待补充输入，开始进入任务队列。")
            await self._enqueue_task_message(route, bundled_message)
            return
        if command == "/dropinput":
            dropped_bundle = await self._drop_input_bundle(input_bundle_key)
            if dropped_bundle is None:
                await self.bot.send_text(message.reply_target, "当前没有待补充的图文输入。")
                return
            await self.bot.send_text(
                message.reply_target,
                f"已丢弃待补充输入（{len(dropped_bundle.texts)} 段文字，{len(dropped_bundle.attachments)} 个附件）。",
            )
            return
        if command == "/release":
            released = await self.runner.release_route(route.name)
            if released:
                await self.bot.send_text(message.reply_target, "当前项目的常驻 Codex 进程已释放；下次有新消息时会自动重建。")
            else:
                await self.bot.send_text(message.reply_target, "当前没有可释放的常驻 Codex 进程。")
            return
        if command in {"/new", "/reset"}:
            if self._active_tasks.get(task_key):
                await self.bot.send_text(message.reply_target, "这个群里上一条任务还在跑，等它收工后我再帮你重开上下文。")
                return
            await self._drop_input_bundle(input_bundle_key)
            active_context = self.sessions.resolve_context(route, chat_scope_key)
            removed = self.sessions.clear(route, str(active_context.get("scope_key") or chat_scope_key))
            context_name = str(active_context.get("name") or DEFAULT_CONTEXT_NAME)
            text = f"当前上下文 `{context_name}` 已清空；你下一条消息会在同一路径下重新开一轮。"
            if not removed:
                text = f"当前上下文 `{context_name}` 目前还没有历史会话；你下一条消息会直接新开一轮。"
            await self.bot.send_card(message.reply_target, "上下文已重开", text)
            return

        if message.attachments:
            await self._stage_input_bundle(route, message, input_bundle_key)
            return

        if raw_text:
            bundled_message = await self._submit_input_bundle(input_bundle_key, extra_message=message)
            if bundled_message is not None:
                await self._enqueue_task_message(route, bundled_message)
                return

        await self._enqueue_task_message(route, message)

    async def _handle_context_command(
        self,
        route: RouteConfig,
        message: FeishuInboundMessage,
        chat_scope_key: str,
        task_key: str,
        raw_text: str,
    ) -> None:
        parts = raw_text.split(None, 2)
        action = parts[1].lower() if len(parts) > 1 else "list"
        argument = parts[2].strip() if len(parts) > 2 else ""
        if action in {"list", "ls"}:
            await self.bot.send_card(message.reply_target, "上下文列表", self._build_contexts_text(route, chat_scope_key))
            return
        if action in {"new", "create"}:
            if self._task_has_pending_work(task_key):
                await self.bot.send_text(message.reply_target, "当前群里还有运行中或排队中的任务，等它们处理完再切换上下文。")
                return
            try:
                context, created = self.sessions.create_context(route, chat_scope_key, argument)
            except ValueError as exc:
                await self.bot.send_text(message.reply_target, f"{exc}\n用法：`/ctx new 名称`")
                return
            title = "上下文已创建" if created else "上下文已切换"
            await self.bot.send_card(message.reply_target, title, self._build_context_switch_text(route, context, created))
            return
        if action in {"use", "switch"}:
            if self._task_has_pending_work(task_key):
                await self.bot.send_text(message.reply_target, "当前群里还有运行中或排队中的任务，等它们处理完再切换上下文。")
                return
            contexts = self.sessions.list_contexts(route, chat_scope_key)
            selected = self._select_context_entry(contexts, argument)
            if selected is None:
                await self.bot.send_text(
                    message.reply_target,
                    "没找到对应上下文。先发 `/ctx` 看编号，再用 `/ctx use 编号` 或 `/ctx use 名称` 切换。",
                )
                return
            if bool(selected.get("active")):
                await self.bot.send_card(message.reply_target, "当前上下文", self._build_context_switch_text(route, selected, False))
                return
            context = self.sessions.activate_context(route, chat_scope_key, str(selected.get("name") or ""))
            if context is None:
                await self.bot.send_text(message.reply_target, "切换失败：没有找到对应上下文。")
                return
            await self.bot.send_card(message.reply_target, "上下文已切换", self._build_context_switch_text(route, context, False))
            return
        await self.bot.send_text(
            message.reply_target,
            "用法：`/ctx` 查看列表，`/ctx use 编号` 切换，`/ctx new 名称` 新建并切换。",
        )

    def _task_has_pending_work(self, task_key: str) -> bool:
        return bool(self._active_tasks.get(task_key) or self._pending_messages.get(task_key))

    @staticmethod
    def _select_context_entry(contexts: list[dict[str, Any]], selector: str) -> dict[str, Any] | None:
        normalized = re.sub(r"\s+", " ", selector or "").strip()
        if not normalized:
            return None
        if normalized.isdigit():
            index = int(normalized)
            if 1 <= index <= len(contexts):
                return contexts[index - 1]
        folded = normalized.casefold()
        for context in contexts:
            if str(context.get("name") or "").casefold() == folded:
                return context
        return None

    async def _enqueue_task_message(self, route: RouteConfig, message: FeishuInboundMessage) -> None:
        chat_scope_key = self._build_scope_key(route, message)
        task_key = f"{route.name}::{chat_scope_key}"
        context = self.sessions.resolve_context(route, chat_scope_key)
        queued_task = QueuedTask(
            message=message,
            context_name=str(context.get("name") or DEFAULT_CONTEXT_NAME),
            context_scope_key=str(context.get("scope_key") or chat_scope_key),
        )
        should_start = False
        ahead_count = 0
        async with self._dispatch_lock:
            queue = self._pending_messages.setdefault(task_key, deque())
            queue.append(queued_task)
            if self._active_tasks.get(task_key):
                ahead_count = len(queue)
            else:
                self._active_tasks[task_key] = True
                should_start = True

        if not should_start:
            await self.bot.send_text(
                message.reply_target,
                f"收到，先排队。前面还有 {ahead_count} 条，这条跑完我会自动继续。",
            )
            return

        await self._drain_task_queue(route, chat_scope_key, task_key)

    async def _stage_input_bundle(self, route: RouteConfig, message: FeishuInboundMessage, bundle_key: str) -> None:
        now = time.time()
        async with self._bundle_lock:
            bundle = self._input_bundles.get(bundle_key)
            if bundle is None:
                bundle = InputBundle(
                    bundle_key=bundle_key,
                    route_name=route.name,
                    reply_target=message.reply_target,
                    chat_id=message.chat_id,
                    chat_type=message.chat_type,
                    sender_id=message.sender_id,
                    sender_name=message.sender_name,
                    root_id=message.root_id,
                    parent_id=message.parent_id,
                    thread_id=message.thread_id,
                    message_id=message.message_id,
                    message_type=message.message_type,
                    first_at=now,
                )
                self._input_bundles[bundle_key] = bundle
            self._append_to_input_bundle(bundle, message, now)
            self._reschedule_input_bundle_locked(bundle, now)

    async def _submit_input_bundle(
        self,
        bundle_key: str,
        *,
        extra_message: FeishuInboundMessage | None = None,
    ) -> FeishuInboundMessage | None:
        async with self._bundle_lock:
            bundle = self._input_bundles.get(bundle_key)
            if bundle is None:
                return None
            now = time.time()
            if extra_message is not None:
                self._append_to_input_bundle(bundle, extra_message, now)
            self._input_bundles.pop(bundle_key, None)
            self._cancel_input_bundle_task(bundle)
            bundle.flush_task = None
            return self._bundle_to_message(bundle)

    async def _drop_input_bundle(self, bundle_key: str) -> InputBundle | None:
        async with self._bundle_lock:
            bundle = self._input_bundles.pop(bundle_key, None)
            if bundle is None:
                return None
            self._cancel_input_bundle_task(bundle)
            bundle.flush_task = None
            return bundle

    async def _auto_flush_input_bundle(self, bundle_key: str, generation: int, due_at: float) -> None:
        wait_seconds = max(0.0, due_at - time.time())
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            return

        async with self._bundle_lock:
            bundle = self._input_bundles.get(bundle_key)
            if bundle is None or bundle.generation != generation:
                return
            self._input_bundles.pop(bundle_key, None)
            bundle.flush_task = None
            bundled_message = self._bundle_to_message(bundle)
            route = self.routes.routes_by_name.get(bundle.route_name)

        if route is None:
            logger.warning("Ignoring auto-submitted input bundle for unknown route {}", bundle.route_name)
            return
        await self._enqueue_task_message(route, bundled_message)

    async def _clear_pending_queue(self, task_key: str) -> int:
        async with self._dispatch_lock:
            queue = self._pending_messages.get(task_key)
            if not queue:
                return 0
            cleared = len(queue)
            queue.clear()
            self._pending_messages.pop(task_key, None)
            return cleared

    def _cancel_input_bundle_task(self, bundle: InputBundle) -> None:
        current_task = asyncio.current_task()
        flush_task = bundle.flush_task
        if flush_task is not None and flush_task is not current_task and not flush_task.done():
            flush_task.cancel()
        bundle.flush_task = None

    def _reschedule_input_bundle_locked(self, bundle: InputBundle, now: float) -> None:
        self._cancel_input_bundle_task(bundle)
        bundle.generation += 1
        max_due_at = bundle.first_at + INPUT_BUNDLE_MAX_WAIT_SECONDS
        bundle.due_at = min(now + INPUT_BUNDLE_IDLE_SECONDS, max_due_at)
        bundle.flush_task = asyncio.create_task(
            self._auto_flush_input_bundle(bundle.bundle_key, bundle.generation, bundle.due_at)
        )

    @staticmethod
    def _append_to_input_bundle(bundle: InputBundle, message: FeishuInboundMessage, now: float) -> None:
        text = message.text.strip()
        if text:
            bundle.texts.append(text)
        if message.attachments:
            bundle.attachments.extend(message.attachments)
        if bundle.first_at <= 0:
            bundle.first_at = now
        bundle.last_at = now
        bundle.reply_target = message.reply_target
        bundle.chat_id = message.chat_id
        bundle.chat_type = message.chat_type
        bundle.sender_id = message.sender_id
        bundle.sender_name = message.sender_name
        bundle.root_id = message.root_id
        bundle.parent_id = message.parent_id
        bundle.thread_id = message.thread_id
        bundle.message_id = message.message_id
        bundle.message_type = message.message_type

    @staticmethod
    def _bundle_to_message(bundle: InputBundle) -> FeishuInboundMessage:
        text = "\n\n".join(part for part in bundle.texts if part.strip()).strip()
        return FeishuInboundMessage(
            sender_id=bundle.sender_id,
            sender_name=bundle.sender_name,
            reply_target=bundle.reply_target,
            chat_id=bundle.chat_id,
            chat_type=bundle.chat_type,
            message_id=bundle.message_id,
            message_type=bundle.message_type,
            text=text,
            root_id=bundle.root_id,
            parent_id=bundle.parent_id,
            thread_id=bundle.thread_id,
            attachments=list(bundle.attachments),
        )

    async def _drain_task_queue(self, route: RouteConfig, chat_scope_key: str, task_key: str) -> None:
        try:
            while True:
                async with self._dispatch_lock:
                    queue = self._pending_messages.get(task_key)
                    if not queue:
                        self._pending_messages.pop(task_key, None)
                        self._active_tasks[task_key] = False
                        return
                    queued_task = queue.popleft()
                    if not queue:
                        self._pending_messages.pop(task_key, None)
                await self._run_single_message(route, chat_scope_key, task_key, queued_task)
        finally:
            async with self._dispatch_lock:
                if not self._pending_messages.get(task_key):
                    self._pending_messages.pop(task_key, None)
                    self._active_tasks[task_key] = False

    async def _run_single_message(
        self,
        route: RouteConfig,
        chat_scope_key: str,
        task_key: str,
        queued_task: QueuedTask,
    ) -> None:
        message = queued_task.message
        context_scope_key = queued_task.context_scope_key
        context_name = queued_task.context_name
        started_at = time.time()
        downloads_dir = self.state_dir / "downloads" / route.name / self._scope_dir_name(context_scope_key)
        initial_progress = "任务已启动，正在等待 Codex 建立上下文。"
        self._running_meta[task_key] = {
            "started_at": int(started_at),
            "context_name": context_name,
            "message_preview": self._message_preview(message),
            "progress_status": initial_progress,
        }
        self._start_progress_tracking(route, task_key, message.reply_target, message, started_at, context_name)
        try:
            downloaded_paths = await self._download_attachments(message.attachments, downloads_dir)
            include_bridge_preamble = self.backend != "app_server"
            prompt, image_paths = self._build_prompt(
                message,
                downloaded_paths,
                include_bridge_preamble=include_bridge_preamble,
            )
            developer_instructions = self._build_thread_instructions() if self.backend == "app_server" else None
            existing_session = self.sessions.get(route, context_scope_key)
            result = await self.runner.run(
                prompt=prompt,
                route=route,
                session_id=existing_session,
                image_paths=image_paths,
                run_key=task_key,
                on_progress=lambda text: self._update_progress_status(task_key, text),
                developer_instructions=developer_instructions,
            )
            if result.session_id:
                self.sessions.set(
                    route,
                    context_scope_key,
                    result.session_id,
                    chat_scope_key=chat_scope_key,
                    context_name=context_name,
                )
            usage = self._extract_turn_usage(result.event_lines)
            last_successful_command = self._extract_last_successful_command(result.event_lines)
            self.sessions.record_run(
                route,
                context_scope_key,
                message_text=self._message_preview(message),
                success=result.success,
                usage=usage,
                last_successful_command=last_successful_command,
            )
            clean_text, artifacts = self._resolve_artifacts(
                route=route,
                output_text=result.output,
                started_at=started_at,
            )
            if result.interrupted:
                title = "这轮已中断"
                template = "orange"
            else:
                title = "已完成" if result.success else "这轮卡住了"
                template = "green" if result.success else "red"

            extra_text_needed = False
            if clean_text.strip():
                progress_content = clean_text
                if len(progress_content) > 20000:
                    extra_text_needed = True
                    progress_content = "最终结果较长，我会在下面继续分段补发完整文本。"
            elif artifacts:
                progress_content = "文字结果不多，但我把这轮产出的文件给你带回来了。"
            else:
                progress_content = "这轮已经跑完，不过暂时没有可直接展示的文字结果。"

            progress_handled = await self._finish_progress_tracking(
                task_key=task_key,
                title=title,
                content=progress_content,
                template=template,
            )

            if extra_text_needed:
                await self.bot.send_card(message.reply_target, title, clean_text)
            elif not progress_handled:
                if clean_text.strip():
                    await self._send_final_text(message.reply_target, title, clean_text)
                elif artifacts:
                    await self.bot.send_text(message.reply_target, "文字结果不多，但我把这轮产出的文件给你带回来了。")
                else:
                    await self.bot.send_text(message.reply_target, "这轮已经跑完，不过暂时没有可直接展示的文字结果。")
            await self._send_artifacts(message.reply_target, artifacts)
        except Exception as exc:
            logger.exception("Task error")
            error_text = f"```\n{exc}\n```"
            progress_handled = await self._finish_progress_tracking(
                task_key=task_key,
                title="这轮卡住了",
                content=error_text,
                template="red",
            )
            if not progress_handled:
                await self.bot.send_card(message.reply_target, "这轮卡住了", error_text)
        finally:
            await self._discard_progress_tracking(task_key)
            self._running_meta.pop(task_key, None)

    def _start_progress_tracking(
        self,
        route: RouteConfig,
        task_key: str,
        reply_target: str,
        message: FeishuInboundMessage,
        started_at: float,
        context_name: str,
    ) -> None:
        if not route.show_progress:
            return
        state = ProgressCardState(
            task_key=task_key,
            reply_target=reply_target,
            route_name=route.name,
            workdir=route.workdir,
            context_name=context_name,
            message_preview=self._message_preview(message),
            started_at=started_at,
            show_delay_seconds=route.progress_delay_seconds,
            force_show_seconds=max(route.progress_delay_seconds, route.progress_force_show_seconds),
            keepalive_seconds=route.progress_keepalive_seconds,
        )
        self._progress_cards[task_key] = state
        state.show_task = asyncio.create_task(self._show_progress_card_later(task_key))

    async def _show_progress_card_later(self, task_key: str) -> None:
        state = self._progress_cards.get(task_key)
        if state is None:
            return
        try:
            while True:
                elapsed = max(0.0, time.time() - state.started_at)
                if state.has_meaningful_activity and elapsed >= state.show_delay_seconds:
                    break
                if elapsed >= state.force_show_seconds:
                    break
                next_deadline = state.force_show_seconds
                if state.has_meaningful_activity:
                    next_deadline = min(next_deadline, state.show_delay_seconds)
                wait_seconds = max(0.2, next_deadline - elapsed)
                await asyncio.sleep(wait_seconds)
                state = self._progress_cards.get(task_key)
                if state is None or state.done:
                    return
        except asyncio.CancelledError:
            return
        state = self._progress_cards.get(task_key)
        if state is None or state.done:
            return
        shown = await self._render_progress_card(task_key, force=True)
        state = self._progress_cards.get(task_key)
        if shown and state is not None and state.keepalive_task is None:
            state.keepalive_task = asyncio.create_task(self._keep_progress_card_alive(task_key))

    async def _keep_progress_card_alive(self, task_key: str) -> None:
        while True:
            state = self._progress_cards.get(task_key)
            if state is None or state.done:
                return
            try:
                await asyncio.sleep(state.keepalive_seconds)
            except asyncio.CancelledError:
                return
            state = self._progress_cards.get(task_key)
            if state is None or state.done:
                return
            await self._render_progress_card(task_key, force=True)

    async def _update_progress_status(self, task_key: str, status: str, *, force: bool = False) -> None:
        clean_status = status.strip() or "处理中…"
        running_meta = self._running_meta.get(task_key)
        if running_meta is not None:
            running_meta["progress_status"] = clean_status
        state = self._progress_cards.get(task_key)
        if state is None or state.done:
            return
        state.status = clean_status
        if self._progress_status_indicates_meaningful_activity(clean_status):
            state.has_meaningful_activity = True
        elapsed = max(0.0, time.time() - state.started_at)
        if not state.visible and state.has_meaningful_activity and elapsed >= state.show_delay_seconds:
            shown = await self._render_progress_card(task_key, force=True)
            if shown and state.keepalive_task is None:
                state.keepalive_task = asyncio.create_task(self._keep_progress_card_alive(task_key))
            return
        if state.visible:
            await self._render_progress_card(task_key, force=force)

    @staticmethod
    def _progress_status_indicates_meaningful_activity(status: str) -> bool:
        return status.startswith("刚执行命令：") or status.startswith("中途冒了个提示：") or status.startswith("已发送中断信号")

    async def _render_progress_card(self, task_key: str, *, force: bool = False) -> bool:
        state = self._progress_cards.get(task_key)
        if state is None or state.done:
            return False
        async with state.render_lock:
            if state.done:
                return False
            title = "Codex 正在处理"
            content = self._build_progress_card_content(state)
            render_key = f"{title}\n{content}"
            now = time.time()
            if not force and render_key == state.last_render_key:
                return state.visible
            if state.visible and not force and now - state.last_render_at < PROGRESS_UPDATE_MIN_INTERVAL_SECONDS:
                return True
            if state.message_id:
                ok = await self.bot.update_card_message(state.message_id, title, content, "blue")
                if ok:
                    state.visible = True
                    state.last_render_key = render_key
                    state.last_render_at = now
                    return True
                return False
            message_id = await self.bot.send_card_message(state.reply_target, title, content, "blue")
            if not message_id:
                return False
            state.message_id = message_id
            state.visible = True
            state.last_render_key = render_key
            state.last_render_at = now
            return True

    async def _finish_progress_tracking(self, *, task_key: str, title: str, content: str, template: str) -> bool:
        state = self._progress_cards.get(task_key)
        if state is None:
            return False
        async with state.render_lock:
            state.done = True
            self._cancel_progress_state_tasks(state)
            if not state.message_id:
                return False
            return await self.bot.update_card_message(state.message_id, title, content, template)

    async def _discard_progress_tracking(self, task_key: str) -> None:
        state = self._progress_cards.pop(task_key, None)
        if state is None:
            return
        state.done = True
        self._cancel_progress_state_tasks(state)

    @staticmethod
    def _cancel_progress_state_tasks(state: ProgressCardState) -> None:
        current_task = asyncio.current_task()
        for task in (state.show_task, state.keepalive_task):
            if task is not None and task is not current_task and not task.done():
                task.cancel()
        state.show_task = None
        state.keepalive_task = None

    @staticmethod
    def _build_progress_card_content(state: ProgressCardState) -> str:
        elapsed = max(1, int(time.time() - state.started_at))
        return "\n".join(
            [
                f"**项目:** {state.route_name}",
                f"**路径:** `{state.workdir}`",
                f"**上下文:** `{state.context_name}`",
                f"**状态:** {state.status}",
                f"**已耗时:** {elapsed} 秒",
                f"**任务:** {state.message_preview}",
            ]
        )

    @staticmethod
    def _build_thread_instructions() -> str:
        return "\n\n".join(
            [
                "你正在通过飞书与用户协作，当前工作目录已经由外层固定。",
                "请默认使用中文回答。",
                "只有当你生成的是适合直接回传飞书查看的结果文件时，才在回复末尾单独使用以下格式列出：",
                "MEDIA:相对路径",
                "FILE:相对路径",
                "这类结果通常指图片、PDF、表格、演示文稿、压缩包等面向查看或交付的产物。",
                "不要把源码文件、脚本、配置文件、依赖清单、测试文件或普通项目文件修改列为回传文件；这些只需要在正文里说明即可，除非用户明确要求把该文件发到飞书。",
            ]
        )

    async def _download_attachments(self, attachments: list[FeishuAttachment], downloads_dir: Path) -> list[Path]:
        downloaded: list[Path] = []
        for attachment in attachments:
            path_obj = await self.bot.download_attachment(attachment, str(downloads_dir))
            downloaded.append(path_obj)
        return downloaded

    def _build_prompt(
        self,
        message: FeishuInboundMessage,
        downloaded_paths: list[Path],
        *,
        include_bridge_preamble: bool = True,
    ) -> tuple[str, list[str]]:
        image_paths = [str(path_obj) for path_obj in downloaded_paths if path_obj.suffix.lower() in IMAGE_SUFFIXES]
        file_paths = [str(path_obj) for path_obj in downloaded_paths if path_obj.suffix.lower() not in IMAGE_SUFFIXES]
        user_text = message.text.strip()
        if not user_text:
            if image_paths and file_paths:
                user_text = "请同时分析我刚发送的图片和文件，并给出结论。"
            elif image_paths:
                user_text = "请分析我刚发送的图片，并给出结论。"
            elif file_paths:
                user_text = "请分析我刚发送的文件，并给出结论。"
            else:
                user_text = "请处理我刚发送的内容。"

        parts: list[str] = []
        if include_bridge_preamble:
            parts.extend(
                [
                    "你正在通过飞书与用户协作，当前工作目录已经由外层固定。",
                    "请默认使用中文回答。",
                    "只有当你生成的是适合直接回传飞书查看的结果文件时，才在回复末尾单独使用以下格式列出：",
                    "MEDIA:相对路径",
                    "FILE:相对路径",
                    "这类结果通常指图片、PDF、表格、演示文稿、压缩包等面向查看或交付的产物。",
                    "不要把源码文件、脚本、配置文件、依赖清单、测试文件或普通项目文件修改列为回传文件；这些只需要在正文里说明即可，除非用户明确要求把该文件发到飞书。",
                    "如果有多份文件，可以写多行；相对路径以当前工作目录为基准。",
                ]
            )
        if image_paths:
            parts.append("本轮收到的图片附件：\n- " + "\n- ".join(image_paths))
        if file_paths:
            parts.append(
                "本轮收到的文件附件（你可以直接读取这些本地文件）：\n- " + "\n- ".join(file_paths)
            )
        parts.append("用户请求：\n" + user_text)
        return "\n\n".join(parts), image_paths

    def _resolve_artifacts(self, *, route: RouteConfig, output_text: str, started_at: float) -> tuple[str, list[Artifact]]:
        explicit, clean_text = self._extract_explicit_artifacts(output_text, route.workdir)
        if explicit:
            return clean_text, explicit
        if route.auto_send_recent_artifacts:
            inferred = self._find_recent_artifacts(route.workdir, started_at, route.max_auto_artifacts)
            if inferred:
                return clean_text, inferred
        return clean_text, []

    def _extract_explicit_artifacts(self, output_text: str, workdir: str) -> tuple[list[Artifact], str]:
        artifacts: list[Artifact] = []
        kept_lines: list[str] = []
        for raw_line in output_text.splitlines():
            match = ARTIFACT_LINE_RE.match(raw_line.strip())
            if not match:
                kept_lines.append(raw_line)
                continue
            kind = match.group(1).lower()
            value = match.group(2).strip().strip('"\'')
            resolved = self._resolve_path_within_workdir(value, workdir)
            if resolved and resolved.exists() and resolved.is_file():
                if not self._is_sendable_artifact_path(resolved):
                    logger.info("Skipping non-sendable artifact path {}", resolved)
                    continue
                actual_kind = "image" if resolved.suffix.lower() in IMAGE_SUFFIXES else "file"
                if kind == "media":
                    kind = actual_kind
                artifacts.append(Artifact(kind=kind if kind in {"image", "file"} else actual_kind, path=resolved))
                continue
            kept_lines.append(raw_line)
        cleaned = "\n".join(line for line in kept_lines).strip()
        return artifacts, cleaned

    @staticmethod
    def _is_sendable_artifact_path(path_obj: Path) -> bool:
        suffix = path_obj.suffix.lower()
        if suffix in IMAGE_SUFFIXES:
            return True
        return suffix not in BLOCKED_ARTIFACT_SUFFIXES

    def _find_recent_artifacts(self, workdir: str, started_at: float, max_items: int) -> list[Artifact]:
        candidates: list[Artifact] = []
        root = Path(workdir)
        for current_root, dir_names, file_names in os.walk(root):
            dir_names[:] = [name for name in dir_names if name not in IGNORE_DIRS and not name.startswith(".")]
            for file_name in file_names:
                path_obj = Path(current_root) / file_name
                if path_obj.suffix.lower() not in RECENT_ARTIFACT_SUFFIXES:
                    continue
                if not self._is_sendable_artifact_path(path_obj):
                    continue
                try:
                    stat = path_obj.stat()
                except OSError:
                    continue
                if stat.st_mtime < started_at - 1:
                    continue
                if stat.st_size > 30 * 1024 * 1024:
                    continue
                kind = "image" if path_obj.suffix.lower() in IMAGE_SUFFIXES else "file"
                candidates.append(Artifact(kind=kind, path=path_obj))
        candidates.sort(key=lambda item: item.path.stat().st_mtime, reverse=True)
        deduped: list[Artifact] = []
        seen: set[str] = set()
        for item in candidates:
            key = str(item.path.resolve())
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= max_items:
                break
        return deduped

    async def _send_artifacts(self, reply_target: str, artifacts: list[Artifact]) -> None:
        for artifact in artifacts:
            if artifact.kind == "image":
                await self.bot.send_image(reply_target, str(artifact.path))
            else:
                await self.bot.send_file(reply_target, str(artifact.path), artifact.path.name)

    async def _send_final_text(self, reply_target: str, title: str, text: str) -> None:
        if self._should_send_final_as_card(text):
            await self.bot.send_card(reply_target, title, text)
            return
        await self.bot.send_text(reply_target, text)

    @staticmethod
    def _should_send_final_as_card(text: str) -> bool:
        stripped = text.strip()
        if not stripped:
            return False
        if len(stripped) > 280:
            return True
        if "```" in stripped:
            return True
        if re.search(r"^#{1,6}\s", stripped, flags=re.MULTILINE):
            return True
        if re.search(r"^\|.+\|$", stripped, flags=re.MULTILINE):
            return True
        if re.search(r"^\s*[-*]\s+.+", stripped, flags=re.MULTILINE) and "\n" in stripped:
            return True
        return False

    @staticmethod
    def _resolve_path_within_workdir(raw_path: str, workdir: str) -> Path | None:
        value = raw_path.strip()
        if not value:
            return None
        workdir_path = Path(workdir).resolve()
        candidate = Path(value)
        resolved = candidate.resolve() if candidate.is_absolute() else (workdir_path / candidate).resolve()
        try:
            resolved.relative_to(workdir_path)
        except ValueError:
            return None
        return resolved

    @staticmethod
    def _scope_dir_name(scope_key: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]+", "_", scope_key).strip("_") or "default"

    @staticmethod
    def _build_scope_key(route: RouteConfig, message: FeishuInboundMessage) -> str:
        return f"chat:{message.chat_id}"

    @staticmethod
    def _format_usage(usage: dict[str, Any] | None) -> str:
        if not isinstance(usage, dict):
            return "无"
        return (
            f"输入 {int(usage.get('input_tokens') or 0)}"
            f" / 缓存 {int(usage.get('cached_input_tokens') or 0)}"
            f" / 输出 {int(usage.get('output_tokens') or 0)}"
        )

    @staticmethod
    def _format_time(timestamp: Any) -> str:
        if not timestamp:
            return "无"
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))

    @staticmethod
    def _extract_turn_usage(events: list[dict[str, Any]]) -> dict[str, int] | None:
        for event in reversed(events):
            if event.get("type") != "turn.completed":
                continue
            usage = event.get("usage")
            if not isinstance(usage, dict):
                continue
            return {
                "input_tokens": int(usage.get("input_tokens") or 0),
                "cached_input_tokens": int(usage.get("cached_input_tokens") or 0),
                "output_tokens": int(usage.get("output_tokens") or 0),
            }
        return None

    @staticmethod
    def _extract_last_successful_command(events: list[dict[str, Any]]) -> dict[str, Any] | None:
        for event in reversed(events):
            if event.get("type") != "item.completed":
                continue
            item = event.get("item")
            if not isinstance(item, dict) or item.get("type") != "command_execution":
                continue
            command = str(item.get("command") or "").strip()
            if not command:
                continue
            exit_code = item.get("exit_code")
            if int(exit_code or -1) != 0:
                continue
            output = str(item.get("aggregated_output") or "")
            return {
                "command": command,
                "output": output[:COMMAND_OUTPUT_PREVIEW],
                "exit_code": 0,
            }
        return None

    @staticmethod
    def _message_preview(message: FeishuInboundMessage) -> str:
        if message.text.strip():
            return SessionStore._preview_text(message.text)
        if message.attachments:
            return f"(仅附件，共 {len(message.attachments)} 个)"
        return "(空消息)"

    @staticmethod
    def _build_input_bundle_key(message: FeishuInboundMessage) -> str:
        topic = message.root_id or message.thread_id or "main"
        return f"chat:{message.chat_id}:sender:{message.sender_id}:topic:{topic}"

    def _describe_input_bundle(self, bundle_key: str) -> tuple[str, str]:
        bundle = self._input_bundles.get(bundle_key)
        if bundle is None:
            return "无", "无"
        remaining_seconds = max(0, int(bundle.due_at - time.time() + 0.999))
        summary = (
            f"有（文字 {len(bundle.texts)} 段 / 附件 {len(bundle.attachments)} 个"
            f" / 约 {remaining_seconds} 秒后自动提交）"
        )
        return summary, self._message_preview(self._bundle_to_message(bundle))

    def _build_help_text(self, route: RouteConfig) -> str:
        return "\n".join(
            [
                "**FlyingCodex**",
                "",
                "这个服务会把当前飞书群固定映射到指定项目路径。一个群就是一个项目，可以在同一路径下维护多个上下文。",
                f"- 当前项目：`{route.name}`",
                f"- 当前路径：`{route.workdir}`",
                f"- 后端模式：`{self.backend}`",
                f"- 长任务进度卡：{'开启' if route.show_progress else '关闭'}",
                "",
                "**命令**",
                "- 直接发文本：继续这个群当前的项目上下文",
                "- 发图片或文件：会先进入一个短暂拼包窗口，方便你再补文字说明",
                "- 同一个群里连续发多条消息：会自动排队，依次执行",
                "- 长任务默认会在群里保留一张运行卡片，持续显示当前状态和耗时",
                "- `/status`：查看当前路由、排队情况和待补充输入状态",
                "- `/ctx`：查看当前项目下的上下文列表",
                "- `/ctx use 编号`：切换到指定上下文",
                "- `/ctx new 名称`：新建一个上下文并切过去",
                "- `/usage`：查看当前上下文与当前项目累计 token 用量",
                "- `/lastcmd`：查看上一条成功执行的 shell 命令",
                "- `/interrupt`：中断当前正在运行的任务",
                "- `/clearqueue`：清空当前群里排队但未开始的消息",
                "- `/send`：立即提交当前待补充的图文输入",
                "- `/dropinput`：丢弃当前待补充的图文输入",
                "- `/release`：释放当前项目的常驻 Codex app-server 进程（不清空上下文）",
                "- `/new` 或 `/reset`：在同一路径下开启新上下文",
                "- `/help`：查看帮助",
                "",
                "**结果回传**",
                "- 如果 Codex 生成图片或交付类结果文件，可以在回复末尾输出 `MEDIA:` 或 `FILE:` 路径，服务会自动回传；源码、脚本、配置文件默认不会作为附件发回飞书。",
            ]
        )

    def _build_contexts_text(self, route: RouteConfig, chat_scope_key: str) -> str:
        contexts = self.sessions.list_contexts(route, chat_scope_key)
        active_context = self.sessions.resolve_context(route, chat_scope_key)
        lines = [
            f"**项目:** {route.name}",
            f"**路径:** `{route.workdir}`",
            f"**当前选中上下文:** `{active_context.get('name') or DEFAULT_CONTEXT_NAME}`",
            "",
            "**上下文列表**",
        ]
        for index, context in enumerate(contexts, start=1):
            marker = "（当前）" if context.get("active") else ""
            status = "已建立" if context.get("has_session") else "未建立"
            lines.append(f"{index}. `{context.get('name')}` {marker}".rstrip())
            lines.append(
                "   "
                + f"状态：{status} | 最后更新：{self._format_time(context.get('updated_at'))} | "
                + f"上一轮用量：{self._format_usage(context.get('last_usage'))}"
            )
        lines.extend(
            [
                "",
                "**切换方式**",
                "- `/ctx use 编号`：按序号切换",
                "- `/ctx use 名称`：按名称切换",
                "- `/ctx new 名称`：新建并切换到一个空上下文",
            ]
        )
        return "\n".join(lines)

    def _build_context_switch_text(self, route: RouteConfig, context: dict[str, Any], created: bool) -> str:
        name = str(context.get("name") or DEFAULT_CONTEXT_NAME)
        has_session = bool(context.get("has_session"))
        session_id = str(context.get("session_id") or "").strip() or "无"
        status = "已建立，可直接续聊" if has_session else "还没有历史会话；你下一条消息会在这里新开一轮"
        lines = [
            f"**项目:** {route.name}",
            f"**路径:** `{route.workdir}`",
            f"**当前上下文:** `{name}`",
            f"**状态:** {status}",
            f"**线程 ID:** `{session_id}`",
        ]
        if created and not has_session:
            lines.append("这个上下文刚创建完成，目前还是空的。")
        return "\n".join(lines)

    def _build_status_text(self, route: RouteConfig, chat_scope_key: str, task_key: str, input_bundle_key: str) -> str:
        active = self._active_tasks.get(task_key, False)
        active_context = self.sessions.resolve_context(route, chat_scope_key)
        pending_count = len(self._pending_messages.get(task_key) or ())
        running_meta = self._running_meta.get(task_key) or {}
        runtime_status = self.runner.route_status(route.name) or {}
        draft_summary, draft_preview = self._describe_input_bundle(input_bundle_key)
        updated_line = self._format_time(active_context.get("updated_at"))
        idle_timeout_seconds = int(runtime_status.get("idle_timeout_seconds") or 0)
        idle_for_seconds = int(runtime_status.get("idle_for_seconds") or 0)
        idle_remaining = max(0, idle_timeout_seconds - idle_for_seconds) if idle_timeout_seconds > 0 else 0
        runtime_running = bool(runtime_status.get("running"))
        return "\n".join(
            [
                f"**项目:** {route.name}",
                f"**路径:** `{route.workdir}`",
                f"**后端模式:** `{self.backend}`",
                f"**任务状态:** {'运行中' if active else '空闲'}",
                f"**当前进度:** {running_meta.get('progress_status') or '无'}",
                f"**常驻进程:** {'运行中' if runtime_running else '未启动'}",
                f"**活跃 turn 数:** {int(runtime_status.get('active_turns') or 0)}",
                f"**空闲自动释放:** {str(idle_timeout_seconds) + ' 秒' if idle_timeout_seconds > 0 else '关闭'}",
                f"**距自动释放剩余:** {str(idle_remaining) + ' 秒' if runtime_running and idle_timeout_seconds > 0 and not active else '无'}",
                f"**常驻进程最近活跃:** {self._format_time(runtime_status.get('last_used_at'))}",
                f"**排队中消息数:** {pending_count}",
                f"**待补充输入:** {draft_summary}",
                f"**待补充内容:** {draft_preview}",
                f"**当前选中上下文:** `{active_context.get('name') or DEFAULT_CONTEXT_NAME}`",
                f"**当前运行上下文:** `{running_meta.get('context_name') or '无'}`",
                f"**当前运行开始于:** {self._format_time(running_meta.get('started_at'))}",
                f"**当前运行内容:** {running_meta.get('message_preview') or '无'}",
                f"**当前上下文状态:** {'已建立' if active_context.get('has_session') else '未建立'}",
                "",
                f"**终端接续用线程 ID:** `{active_context.get('session_id') or '无'}`",
                "",
                f"**上次更新时间:** {updated_line}",
                f"**当前项目已保存上下文数:** {self.sessions.count_contexts(route)}",
            ]
        )

    def _build_usage_text(self, route: RouteConfig, chat_scope_key: str) -> str:
        active_context = self.sessions.resolve_context(route, chat_scope_key)
        scope_metrics = self.sessions.describe_scope_metrics(route, str(active_context.get("scope_key") or chat_scope_key)) or {}
        route_metrics = self.sessions.describe_route_metrics(route) or {}
        return "\n".join(
            [
                f"**项目:** {route.name}",
                f"**路径:** `{route.workdir}`",
                "",
                f"**当前上下文:** `{active_context.get('name') or DEFAULT_CONTEXT_NAME}`",
                f"- 任务数：{int(scope_metrics.get('task_count') or 0)}",
                f"- 成功 / 失败：{int(scope_metrics.get('success_count') or 0)} / {int(scope_metrics.get('failure_count') or 0)}",
                f"- 上一轮用量：{self._format_usage(scope_metrics.get('last_usage'))}",
                f"- 累计用量：{self._format_usage(scope_metrics.get('usage_totals'))}",
                f"- 最后更新：{self._format_time(scope_metrics.get('updated_at'))}",
                "",
                "**当前项目累计**",
                f"- 任务数：{int(route_metrics.get('task_count') or 0)}",
                f"- 成功 / 失败：{int(route_metrics.get('success_count') or 0)} / {int(route_metrics.get('failure_count') or 0)}",
                f"- 上一轮用量：{self._format_usage(route_metrics.get('last_usage'))}",
                f"- 累计用量：{self._format_usage(route_metrics.get('usage_totals'))}",
                f"- 最后更新：{self._format_time(route_metrics.get('updated_at'))}",
            ]
        )

    def _build_last_command_text(self, route: RouteConfig, chat_scope_key: str) -> str:
        active_context = self.sessions.resolve_context(route, chat_scope_key)
        scope_metrics = self.sessions.describe_scope_metrics(route, str(active_context.get("scope_key") or chat_scope_key)) or {}
        route_metrics = self.sessions.describe_route_metrics(route) or {}
        command_info = scope_metrics.get("last_successful_command") or route_metrics.get("last_successful_command")
        if not isinstance(command_info, dict) or not str(command_info.get("command") or "").strip():
            return "暂时还没有记录到成功执行过的 shell 命令。"
        output = str(command_info.get("output") or "").strip()
        parts = [
            f"**上下文:** `{active_context.get('name') or DEFAULT_CONTEXT_NAME}`",
            f"**时间:** {self._format_time(command_info.get('updated_at'))}",
            f"**命令:**\n```bash\n{command_info.get('command')}\n```",
            f"**退出码:** {int(command_info.get('exit_code') or 0)}",
        ]
        if output:
            parts.append(f"**输出摘要:**\n```text\n{output}\n```")
        return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="FlyingCodex: Feishu -> Codex multi-route bridge")
    parser.add_argument(
        "--routes",
        default=str(Path(__file__).resolve().with_name(DEFAULT_ROUTES_FILE)),
        help="JSON route config path (default: routes.json beside the script)",
    )
    parser.add_argument(
        "--session-store",
        default=str(Path(__file__).resolve().with_name(DEFAULT_SESSION_STORE)),
        help="Session store path (default: .fly-codex-sessions.json beside the script)",
    )
    parser.add_argument(
        "--legacy-session-store",
        default=str(Path(__file__).resolve().with_name(".codex-feishu-sessions.json")),
        help="Legacy single-project session store for migration",
    )
    parser.add_argument(
        "--state-dir",
        default=str(Path(__file__).resolve().with_name(DEFAULT_CACHE_DIR)),
        help="Directory for downloaded attachments and runtime state",
    )
    parser.add_argument(
        "--codex-bin",
        default="codex",
        help="Fallback path to the Codex CLI binary",
    )
    parser.add_argument(
        "--backend",
        choices=["exec", "app_server"],
        default="app_server",
        help="Codex backend mode: one-shot exec or persistent app-server",
    )
    args = parser.parse_args()

    service = MultiRouteCodexFeishuService(
        routes_path=args.routes,
        session_store_path=args.session_store,
        codex_bin=args.codex_bin,
        backend=args.backend,
        state_dir=args.state_dir,
        legacy_session_store=args.legacy_session_store,
    )
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(service.shutdown())


if __name__ == "__main__":
    main()
