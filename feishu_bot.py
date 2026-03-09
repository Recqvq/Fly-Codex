from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateFileRequest,
    CreateFileRequestBody,
    CreateImageRequest,
    CreateImageRequestBody,
    CreateMessageRequest,
    CreateMessageRequestBody,
    GetMessageResourceRequest,
    PatchMessageRequest,
    PatchMessageRequestBody,
)
from loguru import logger

MAX_CARD_CONTENT_LEN = 28000
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".ico", ".tiff"}


@dataclass(slots=True)
class FeishuAttachment:
    kind: str
    message_id: str
    file_key: str
    file_name: str | None = None
    suffix_hint: str | None = None


@dataclass(slots=True)
class FeishuInboundMessage:
    sender_id: str
    sender_name: str | None
    reply_target: str
    chat_id: str
    chat_type: str
    message_id: str
    message_type: str
    text: str
    root_id: str | None = None
    parent_id: str | None = None
    thread_id: str | None = None
    attachments: list[FeishuAttachment] = field(default_factory=list)


class FeishuBot:
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self._client: lark.Client | None = None
        self._ws_client: Any = None
        self._ws_thread: threading.Thread | None = None
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._processed_ids: OrderedDict[str, None] = OrderedDict()
        self._message_handler: Callable[[FeishuInboundMessage], Awaitable[None]] | None = None

    def on_message(self, handler: Callable[[FeishuInboundMessage], Awaitable[None]]) -> None:
        self._message_handler = handler

    async def start(self) -> None:
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._client = (
            lark.Client.builder()
            .app_id(self.app_id)
            .app_secret(self.app_secret)
            .log_level(lark.LogLevel.INFO)
            .build()
        )
        event_handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._on_message_sync)
            .build()
        )
        self._ws_client = lark.ws.Client(
            self.app_id,
            self.app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )

        def run_ws() -> None:
            while self._running:
                try:
                    self._ws_client.start()
                except Exception as exc:
                    logger.warning("WebSocket error: {}", exc)
                if self._running:
                    time.sleep(5)

        self._ws_thread = threading.Thread(target=run_ws, daemon=True)
        self._ws_thread.start()
        logger.info("Feishu bot started (WebSocket)")
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        logger.info("Feishu bot stopped")

    def _on_message_sync(self, data: Any) -> None:
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._on_message(data), self._loop)

    async def _on_message(self, data: Any) -> None:
        try:
            event = data.event
            message = event.message
            sender = event.sender
            msg_id = message.message_id
            if msg_id in self._processed_ids:
                return
            self._processed_ids[msg_id] = None
            while len(self._processed_ids) > 1000:
                self._processed_ids.popitem(last=False)
            if sender.sender_type == "bot":
                return

            sender_id = getattr(sender.sender_id, "open_id", None) or "unknown"
            sender_name = getattr(sender, "name", None)
            chat_id = message.chat_id
            chat_type = message.chat_type
            msg_type = message.message_type
            reply_target = chat_id if chat_type == "group" else sender_id

            content_json: dict[str, Any]
            try:
                content_json = json.loads(message.content) if message.content else {}
            except json.JSONDecodeError:
                content_json = {}

            text = ""
            attachments: list[FeishuAttachment] = []
            if msg_type == "text":
                text = str(content_json.get("text", "")).strip()
            elif msg_type == "post":
                text = self._extract_post_text(content_json).strip()
            elif msg_type == "image":
                image_key = str(content_json.get("image_key", "")).strip()
                if image_key:
                    attachments.append(
                        FeishuAttachment(
                            kind="image",
                            message_id=msg_id,
                            file_key=image_key,
                            file_name=f"image-{msg_id}.jpg",
                            suffix_hint=".jpg",
                        )
                    )
            elif msg_type == "file":
                file_key = str(content_json.get("file_key", "")).strip()
                file_name = str(content_json.get("file_name", "")).strip() or None
                if file_key:
                    attachments.append(
                        FeishuAttachment(
                            kind="file",
                            message_id=msg_id,
                            file_key=file_key,
                            file_name=file_name,
                            suffix_hint=Path(file_name).suffix if file_name else None,
                        )
                    )
            else:
                logger.info("Ignoring unsupported message type: {}", msg_type)
                return

            text = re.sub(r"@_user_\d+\s*", "", text).strip()
            if not text and not attachments:
                return

            inbound = FeishuInboundMessage(
                sender_id=sender_id,
                sender_name=sender_name,
                reply_target=reply_target,
                chat_id=chat_id,
                chat_type=chat_type,
                message_id=msg_id,
                message_type=msg_type,
                text=text,
                root_id=getattr(message, "root_id", None),
                parent_id=getattr(message, "parent_id", None),
                thread_id=getattr(message, "thread_id", None),
                attachments=attachments,
            )
            logger.info(
                "Received message chat={} sender={} type={} text={} attachments={}",
                inbound.chat_id,
                inbound.sender_id,
                inbound.message_type,
                inbound.text,
                len(inbound.attachments),
            )
            if self._message_handler:
                await self._message_handler(inbound)
        except Exception as exc:
            logger.error("Error processing message: {}", exc)

    @staticmethod
    def _extract_post_text(content_json: dict[str, Any]) -> str:
        root = content_json
        if isinstance(root.get("post"), dict):
            root = root["post"]
        if not isinstance(root, dict):
            return ""

        def parse_block(block: dict[str, Any]) -> str:
            if not isinstance(block, dict) or not isinstance(block.get("content"), list):
                return ""
            parts: list[str] = []
            title = block.get("title")
            if isinstance(title, str) and title.strip():
                parts.append(title.strip())
            for row in block["content"]:
                if not isinstance(row, list):
                    continue
                for entry in row:
                    if not isinstance(entry, dict):
                        continue
                    tag = entry.get("tag")
                    if tag in {"text", "a"}:
                        parts.append(str(entry.get("text", "")))
                    elif tag == "at":
                        parts.append(f"@{entry.get('user_name', 'user')}")
            return " ".join(part for part in parts if part).strip()

        if "content" in root:
            parsed = parse_block(root)
            if parsed:
                return parsed
        for key in ("zh_cn", "en_us", "ja_jp"):
            if key in root and isinstance(root[key], dict):
                parsed = parse_block(root[key])
                if parsed:
                    return parsed
        return ""

    def _send_message_sync(self, receive_id_type: str, receive_id: str, msg_type: str, content: str) -> str | bool:
        try:
            request = (
                CreateMessageRequest.builder()
                .receive_id_type(receive_id_type)
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(receive_id)
                    .msg_type(msg_type)
                    .content(content)
                    .build()
                )
                .build()
            )
            response = self._client.im.v1.message.create(request)
            if not response.success():
                logger.error("Send failed: code={}, msg={}", response.code, response.msg)
                return False
            return (
                response.data.message_id
                if response.data is not None and isinstance(response.data.message_id, str)
                else True
            )
        except Exception as exc:
            logger.error("Error sending message: {}", exc)
            return False

    def _update_message_sync(self, message_id: str, content: str) -> bool:
        if self._client is None:
            raise RuntimeError("Feishu client is not initialized")
        try:
            request = (
                PatchMessageRequest.builder()
                .message_id(message_id)
                .request_body(
                    PatchMessageRequestBody.builder()
                    .content(content)
                    .build()
                )
                .build()
            )
            response = self._client.im.v1.message.patch(request)
            if not response.success():
                logger.error("Patch failed: code={}, msg={}", response.code, response.msg)
                return False
            return True
        except Exception as exc:
            logger.error("Error patching message: {}", exc)
            return False

    @staticmethod
    def _card_payload(title: str, content: str, template: str | None = None) -> str:
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": template or ("green" if "completed" in title.lower() or "done" in title.lower() else "blue"),
            },
            "elements": [{"tag": "markdown", "content": content}],
        }
        return json.dumps(card, ensure_ascii=False)

    async def send_text(self, chat_id: str, text: str) -> None:
        receive_id_type = "chat_id" if chat_id.startswith("oc_") else "open_id"
        content = json.dumps({"text": text}, ensure_ascii=False)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._send_message_sync, receive_id_type, chat_id, "text", content)

    async def send_card_message(self, chat_id: str, title: str, content: str, template: str | None = None) -> str | None:
        receive_id_type = "chat_id" if chat_id.startswith("oc_") else "open_id"
        loop = asyncio.get_running_loop()
        payload = self._card_payload(title, content, template)
        result = await loop.run_in_executor(
            None,
            self._send_message_sync,
            receive_id_type,
            chat_id,
            "interactive",
            payload,
        )
        return result if isinstance(result, str) and result.strip() else None

    async def update_card_message(
        self,
        message_id: str,
        title: str,
        content: str,
        template: str | None = None,
    ) -> bool:
        loop = asyncio.get_running_loop()
        payload = self._card_payload(title, content, template)
        return bool(await loop.run_in_executor(None, self._update_message_sync, message_id, payload))

    async def send_card(self, chat_id: str, title: str, content: str) -> None:
        chunks = self._split_content(content, MAX_CARD_CONTENT_LEN)
        total = len(chunks)
        for index, chunk in enumerate(chunks, start=1):
            card_title = title if total == 1 else f"{title} ({index}/{total})"
            await self.send_card_message(chat_id, card_title, chunk)

    async def download_attachment(self, attachment: FeishuAttachment, target_dir: str) -> Path:
        if self._client is None:
            raise RuntimeError("Feishu client is not initialized")
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        request = (
            GetMessageResourceRequest.builder()
            .message_id(attachment.message_id)
            .file_key(attachment.file_key)
            .type("image" if attachment.kind == "image" else "file")
            .build()
        )
        response = self._client.im.v1.message_resource.get(request)
        if not response.success():
            raise RuntimeError(f"Download failed: code={response.code}, msg={response.msg}")
        raw_name = attachment.file_name or f"{attachment.kind}-{attachment.message_id}{attachment.suffix_hint or ''}"
        safe_name = self._safe_file_name(raw_name)
        file_path = self._unique_path(Path(target_dir) / safe_name)
        data = response.file.read() if response.file else b""
        with open(file_path, "wb") as file:
            file.write(data)
        return file_path

    async def send_image(self, chat_id: str, image_path: str) -> None:
        if self._client is None:
            raise RuntimeError("Feishu client is not initialized")
        receive_id_type = "chat_id" if chat_id.startswith("oc_") else "open_id"
        loop = asyncio.get_running_loop()

        def _upload_and_send() -> None:
            with open(image_path, "rb") as image_file:
                upload_request = (
                    CreateImageRequest.builder()
                    .request_body(
                        CreateImageRequestBody.builder()
                        .image_type("message")
                        .image(image_file)
                        .build()
                    )
                    .build()
                )
                upload_response = self._client.im.v1.image.create(upload_request)
                if not upload_response.success() or not upload_response.data or not upload_response.data.image_key:
                    raise RuntimeError(
                        f"Image upload failed: code={upload_response.code}, msg={upload_response.msg}"
                    )
                content = json.dumps({"image_key": upload_response.data.image_key}, ensure_ascii=False)
                ok = self._send_message_sync(receive_id_type, chat_id, "image", content)
                if not ok:
                    raise RuntimeError("Send image message failed")

        await loop.run_in_executor(None, _upload_and_send)

    async def send_file(self, chat_id: str, file_path: str, display_name: str | None = None) -> None:
        if self._client is None:
            raise RuntimeError("Feishu client is not initialized")
        receive_id_type = "chat_id" if chat_id.startswith("oc_") else "open_id"
        loop = asyncio.get_running_loop()

        def _upload_and_send() -> None:
            actual_name = display_name or Path(file_path).name
            file_type = self._infer_feishu_file_type(actual_name)
            with open(file_path, "rb") as file_obj:
                upload_request = (
                    CreateFileRequest.builder()
                    .request_body(
                        CreateFileRequestBody.builder()
                        .file_type(file_type)
                        .file_name(actual_name)
                        .file(file_obj)
                        .build()
                    )
                    .build()
                )
                upload_response = self._client.im.v1.file.create(upload_request)
                if not upload_response.success() or not upload_response.data or not upload_response.data.file_key:
                    raise RuntimeError(
                        f"File upload failed: code={upload_response.code}, msg={upload_response.msg}"
                    )
                content = json.dumps({"file_key": upload_response.data.file_key}, ensure_ascii=False)
                ok = self._send_message_sync(receive_id_type, chat_id, "file", content)
                if not ok:
                    raise RuntimeError("Send file message failed")

        await loop.run_in_executor(None, _upload_and_send)

    @staticmethod
    def _split_content(text: str, max_len: int) -> list[str]:
        if len(text) <= max_len:
            return [text]
        chunks: list[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= max_len:
                chunks.append(remaining)
                break
            split_at = remaining.rfind("\n", 0, max_len)
            if split_at < max_len // 2:
                split_at = max_len
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip("\n")
        return chunks

    @staticmethod
    def _safe_file_name(name: str) -> str:
        candidate = re.sub(r"[\\/\r\n\t]+", "_", name).strip()
        return candidate or f"file-{int(time.time())}"

    @staticmethod
    def _unique_path(path_obj: Path) -> Path:
        if not path_obj.exists():
            return path_obj
        stem = path_obj.stem
        suffix = path_obj.suffix
        parent = path_obj.parent
        for index in range(2, 10000):
            candidate = parent / f"{stem}-{index}{suffix}"
            if not candidate.exists():
                return candidate
        raise RuntimeError(f"Unable to allocate unique file path for {path_obj}")

    @staticmethod
    def _infer_feishu_file_type(file_name: str) -> str:
        suffix = Path(file_name).suffix.lower()
        if suffix in {".opus", ".ogg"}:
            return "opus"
        if suffix in {".mp4", ".mov", ".m4v"}:
            return "mp4"
        if suffix == ".pdf":
            return "pdf"
        if suffix in {".doc", ".docx"}:
            return "doc"
        if suffix in {".xls", ".xlsx"}:
            return "xls"
        if suffix in {".ppt", ".pptx"}:
            return "ppt"
        return "stream"
