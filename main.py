# Copyright (C) @TheSmartBisnu
# Channel: https://t.me/itsSmartDev

import os
import shutil
import psutil
import asyncio
import sqlite3
from time import time

from pyleaves import Leaves
from pyrogram.enums import ParseMode
from pyrogram import Client, filters, idle
from pyrogram.errors import PeerIdInvalid, BadRequest, FloodWait, SessionPasswordNeeded
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand

# Initialize user session database
def init_db():
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id INTEGER PRIMARY KEY,
            session_string TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def get_user_session(user_id):
    try:
        conn = sqlite3.connect("users.db")
        cursor = conn.cursor()
        cursor.execute("SELECT session_string FROM user_sessions WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        LOGGER(__name__).error(f"Database get error: {e}")
        return None

def set_user_session(user_id, session_string):
    try:
        conn = sqlite3.connect("users.db")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO user_sessions (user_id, session_string)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET session_string = excluded.session_string
        """, (user_id, session_string))
        conn.commit()
        conn.close()
    except Exception as e:
        LOGGER(__name__).error(f"Database set error: {e}")

def delete_user_session(user_id):
    try:
        conn = sqlite3.connect("users.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        LOGGER(__name__).error(f"Database delete error: {e}")

from helpers.utils import (
    processMediaGroup,
    progressArgs,
    send_media
)

from helpers.forward import check_forward_permission, resolve_forward_chat_id

from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download,
    cleanup_downloads_root
)

from helpers.msg import (
    getChatMsgID,
    get_file_name,
    get_raw_text
)

from config import PyroConf
from logger import LOGGER

# Initialize the bot client
bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=100,
    parse_mode=ParseMode.MARKDOWN,
    max_concurrent_transmissions=12, # ✅ HIGH SPEED
    sleep_threshold=30,
)

# Client for user session
user = Client(
    "user_session",
    workers=100,
    session_string=PyroConf.SESSION_STRING,
    max_concurrent_transmissions=12, # ✅ HIGH SPEED
    sleep_threshold=30,
)

RUNNING_TASKS = set()
download_semaphore = None
forward_chat_id = None

# States, active tasks, and live progress for interactive batch downloads
USER_STATES = {}
ACTIVE_BATCHES = {}
DOWNLOAD_PROGRESS = {}
FORWARDED_REPORTS = {}

ACTIVE_CLIENTS = {}

async def get_client_for_user(user_id):
    # Check if cached and connected
    if user_id in ACTIVE_CLIENTS:
        client = ACTIVE_CLIENTS[user_id]
        if client.is_connected:
            return client
        else:
            try:
                await client.start()
                return client
            except Exception as e:
                LOGGER(__name__).error(f"Failed to start cached client for user {user_id}: {e}")
                ACTIVE_CLIENTS.pop(user_id, None)

    # Check database
    session_str = get_user_session(user_id)
    if not session_str:
        return user

    try:
        client = Client(
            name=f"session_{user_id}",
            api_id=PyroConf.API_ID,
            api_hash=PyroConf.API_HASH,
            session_string=session_str,
            workers=20,
            max_concurrent_transmissions=12,
            sleep_threshold=30,
            in_memory=True
        )
        await client.start()
        ACTIVE_CLIENTS[user_id] = client
        return client
    except Exception as e:
        LOGGER(__name__).error(f"Failed to start client for user {user_id}: {e}")
        return user

def track_task(coro):
    task = asyncio.create_task(coro)
    RUNNING_TASKS.add(task)
    def _remove(_):
        RUNNING_TASKS.discard(task)
    task.add_done_callback(_remove)
    return task


@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "👋 **Welcome to Media Downloader Bot!**\n\n"
        "I can grab photos, videos, audio, and documents from any Telegram post.\n"
        "Just send me a link (paste it directly or use `/dl <link>`),\n"
        "or reply to a message with `/dl`.\n\n"
        "ℹ️ Use `/help` to view all commands and examples.\n"
        "🔒 Make sure the user client is part of the chat.\n\n"
        "Ready? Send me a Telegram post link or click below to start a batch!"
    )

    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Interactive Batch Download", callback_data="start_batch")],
            [InlineKeyboardButton("🚨 Report / Contact", callback_data="start_report")]
        ]
    )
    await message.reply(welcome_text, reply_markup=markup, disable_web_page_preview=True)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **Media Downloader Bot Help**\n\n"
        "➤ **Download Media**\n"
        "   – Send `/dl <post_URL>` **or** just paste a Telegram post link to fetch photos, videos, audio, or documents.\n\n"
        "➤ **Interactive Batch Download** (Recommended)\n"
        "   – Click the button below **or** send `/batch` to start the wizard.\n"
        "   – It will ask for the starting link, then the file count, and let you cancel anytime.\n\n"
        "➤ **Classic Batch Download**\n"
        "   – Send `/bdl start_link end_link` to grab a series of posts in one go.\n"
        "     💡 Example: `/bdl https://t.me/mychannel/100 https://t.me/mychannel/120`\n\n"
        "➤ **Requirements**\n"
        "   – Make sure the user client is part of the chat.\n\n"
        "➤ **Cancel active batch/wizard**\n"
        "   – Send `/cancel` to instantly stop any running batch download or wizard setup.\n\n"
        "➤ **If the bot hangs**\n"
        "   – Send `/killall` to cancel all pending background downloads.\n\n"
        "➤ **Logs**\n"
        "   – Send `/logs` to download the bot’s logs file.\n\n"
        "➤ **Cleanup**\n"
        "   – Send `/cleanup` to remove temporary downloaded files from disk.\n\n"
        "➤ **Stats**\n"
        "   – Send `/stats` to view current status."
    )
    
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Start Batch Download", callback_data="start_batch")],
            [InlineKeyboardButton("🚨 Report / Contact", callback_data="start_report")]
        ]
    )
    await message.reply(help_text, reply_markup=markup, disable_web_page_preview=True)


@bot.on_message(filters.command("cleanup") & filters.private)
async def cleanup_storage(_, message: Message):
    try:
        files_removed, bytes_freed = cleanup_downloads_root()
        if files_removed == 0:
            return await message.reply("🧹 **Cleanup complete:** no local downloads found.")
        return await message.reply(
            f"🧹 **Cleanup complete:** removed `{files_removed}` file(s), "
            f"freed `{get_readable_file_size(bytes_freed)}`."
        )
    except Exception as e:
        LOGGER(__name__).error(f"Cleanup failed: {e}")
        return await message.reply("❌ **Cleanup failed.** Check logs for details.")


@bot.on_callback_query()
async def callback_handlers(bot: Client, callback_query):
    data = callback_query.data
    user_id = callback_query.from_user.id
    LOGGER(__name__).info(f"Callback received: {data} from user_id: {user_id}")
    
    if data == "start_batch":
        # Initialize user state
        USER_STATES[user_id] = {"state": "waiting_start_link"}
        try:
            await callback_query.message.edit_text(
                "⚡️ **Interactive Batch Setup**\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "🔗 **Please send the starting link of the batch:**\n"
                "Example: `https://t.me/mychannel/100` or `https://t.me/c/1234567890/100`\n\n"
                "💬 _Send `/cancel` to abort._",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]
                ])
            )
        except Exception:
            await callback_query.message.reply(
                "🔗 **Please send the starting link of the batch:**\n\n"
                "Example: `https://t.me/mychannel/100` or `https://t.me/c/1234567890/100`\n\n"
                "💬 _Send `/cancel` to abort._"
            )
        await callback_query.answer()
        
    elif data == "cancel_setup":
        USER_STATES.pop(user_id, None)
        try:
            await callback_query.message.edit_text(
                "👋 **Welcome to Media Downloader Bot!**\n\n"
                "I can grab photos, videos, audio, and documents from any Telegram post.\n"
                "Just send me a link (paste it directly or use `/dl <link>`),\n"
                "or reply to a message with `/dl`.\n\n"
                "ℹ️ Use `/help` to view all commands and examples.\n"
                "🔒 Make sure the user client is part of the chat.\n\n"
                "Ready? Send me a Telegram post link or click below to start a batch!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📥 Interactive Batch Download", callback_data="start_batch")],
                    [InlineKeyboardButton("🚨 Report / Contact", callback_data="start_report")]
                ])
            )
        except Exception:
            pass
        await callback_query.answer("Setup cancelled.", show_alert=True)
        
    elif data == "start_report":
        USER_STATES[user_id] = {"state": "waiting_report_message"}
        try:
            await callback_query.message.edit_text(
                "🚨 **Report Bug / Contact Support**\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Please type and send your message below. It will be securely delivered to the administrator to protect your privacy.\n\n"
                "💬 _Send `/cancel` to abort._",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Setup", callback_data="cancel_setup")]
                ])
            )
        except Exception:
            await callback_query.message.reply(
                "🚨 **Report Bug / Contact Support**\n\n"
                "Please send your message now."
            )
        await callback_query.answer()
        
    elif data.startswith("cancel_batch_"):
        batch_user_id = int(data.split("_")[-1])
        if batch_user_id in ACTIVE_BATCHES:
            task = ACTIVE_BATCHES[batch_user_id]
            if not task.done():
                task.cancel()
            try:
                await callback_query.message.edit_text("❌ **Batch download cancelled by user!**")
            except Exception:
                try:
                    await callback_query.message.reply("❌ **Batch download cancelled by user!**")
                except Exception:
                    pass
        else:
            try:
                await callback_query.answer("⚠️ No active batch found to cancel.", show_alert=True)
            except Exception:
                pass


@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(_, message: Message):
    user_id = message.from_user.id
    
    # 1. Cancel active batch task
    if user_id in ACTIVE_BATCHES:
        task = ACTIVE_BATCHES[user_id]
        if not task.done():
            task.cancel()
        await message.reply("⚡️ **Cancelling your active batch download...**")
        return
        
    # 2. Cancel wizard state
    if user_id in USER_STATES:
        state_data = USER_STATES.pop(user_id, None)
        if state_data and "temp_client" in state_data:
            temp_client = state_data["temp_client"]
            try:
                await temp_client.disconnect()
            except Exception:
                pass
        await message.reply("❌ **Setup cancelled.**")
        return
        
    await message.reply("ℹ️ **There is no active batch download or setup to cancel.**")


@bot.on_message(filters.command("batch") & filters.private)
async def batch_command_handler(_, message: Message):
    user_id = message.from_user.id
    USER_STATES[user_id] = {"state": "waiting_start_link"}
    await message.reply(
        "🔗 **Please send the starting link of the batch:**\n\n"
        "Example: `https://t.me/mychannel/100` or `https://t.me/c/1234567890/100`\n\n"
        "💬 _Send `/cancel` to abort._"
    )


@bot.on_message(filters.command("login") & filters.private)
async def login_command_handler(_, message: Message):
    user_id = message.from_user.id
    
    # Check if already logged in
    if get_user_session(user_id):
        await message.reply("ℹ️ **You are already logged in!**\n\nIf you want to switch accounts, please log out first using `/logout`.")
        return
        
    USER_STATES[user_id] = {"state": "waiting_phone_number"}
    await message.reply(
        "🔑 **Telegram Session Login Wizard**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Please send your **Telegram Phone Number** including the country code (e.g. `+1234567890`):\n\n"
        "💬 _Send `/cancel` to abort._"
    )


@bot.on_message(filters.command("logout") & filters.private)
async def logout_handler(_, message: Message):
    user_id = message.from_user.id
    
    # Check if user is logged in
    session_str = get_user_session(user_id)
    if not session_str:
        await message.reply("ℹ️ **You are not currently logged in.**")
        return
        
    # Delete from DB
    delete_user_session(user_id)
    
    # Stop cached client if any
    if user_id in ACTIVE_CLIENTS:
        client = ACTIVE_CLIENTS.pop(user_id, None)
        if client and client != user and client.is_connected:
            try:
                await client.stop()
            except Exception as e:
                LOGGER(__name__).error(f"Error stopping client on logout for user {user_id}: {e}")
                
    await message.reply("👋 **Logged out successfully!**\n\nYour session has been completely wiped from the database. The bot will now fallback to the admin's session for your requests.")


async def prefetch_worker(start_chat, msg_ids_queue, downloaded_data, status_tracker, message):
    user_id = message.from_user.id
    user_client = await get_client_for_user(user_id)
    while not msg_ids_queue.empty():
        try:
            msg_id = msg_ids_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
            
        status_tracker[msg_id] = "downloading"
        
        try:
            chat_msg = await user_client.get_messages(chat_id=start_chat, message_ids=msg_id)
            if not chat_msg:
                downloaded_data[msg_id] = {"status": "skipped"}
                status_tracker[msg_id] = "skipped"
                msg_ids_queue.task_done()
                continue
                
            # If it's a media group, let the uploader handle it sequentially
            if chat_msg.media_group_id:
                downloaded_data[msg_id] = {"status": "media_group", "chat_msg": chat_msg}
                status_tracker[msg_id] = "completed"
                msg_ids_queue.task_done()
                continue
                
            has_media = bool(chat_msg.media)
            has_text  = bool(chat_msg.text or chat_msg.caption)
            if not (has_media or has_text):
                downloaded_data[msg_id] = {"status": "skipped"}
                status_tracker[msg_id] = "skipped"
                msg_ids_queue.task_done()
                continue
                
            # If it's only text
            if not has_media:
                raw_text, raw_text_entities = get_raw_text(chat_msg.text, chat_msg.entities)
                downloaded_data[msg_id] = {
                    "status": "text",
                    "text": raw_text,
                    "entities": raw_text_entities,
                }
                status_tracker[msg_id] = "completed"
                msg_ids_queue.task_done()
                continue
                
            # It has single media
            filename = get_file_name(msg_id, chat_msg)
            download_path = get_download_path(f"{message.id}/{msg_id}", filename)
            
            # Progress callback for this specific message
            def progress(current, total):
                percent = (current / total) * 100 if total > 0 else 0
                DOWNLOAD_PROGRESS[msg_id] = f"`{percent:.1f}%` (`{get_readable_file_size(current)}` / `{get_readable_file_size(total)}`)"
            
            DOWNLOAD_PROGRESS[msg_id] = "`0.0%`"
            
            media_path = await chat_msg.download(
                file_name=download_path,
                progress=progress
            )
            if not media_path or not os.path.exists(media_path) or os.path.getsize(media_path) == 0:
                if media_path:
                    cleanup_download(media_path)
                downloaded_data[msg_id] = {"status": "failed"}
                status_tracker[msg_id] = "failed"
            else:
                raw_caption, raw_caption_entities = get_raw_text(chat_msg.caption, chat_msg.caption_entities)
                media_type = (
                    "photo"
                    if chat_msg.photo
                    else "video"
                    if chat_msg.video
                    else "audio"
                    if chat_msg.audio
                    else "document"
                )
                downloaded_data[msg_id] = {
                    "status": "completed",
                    "media_path": media_path,
                    "media_type": media_type,
                    "caption": raw_caption,
                    "caption_entities": raw_caption_entities,
                }
                status_tracker[msg_id] = "completed"
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOGGER(__name__).error(f"Prefetch error at message {msg_id}: {e}")
            downloaded_data[msg_id] = {"status": "failed"}
            status_tracker[msg_id] = "failed"
            
        msg_ids_queue.task_done()


async def run_interactive_batch(bot: Client, message: Message, start_link: str, count: int):
    user_id = message.from_user.id
    user_client = await get_client_for_user(user_id)
    global forward_chat_id
    
    try:
        start_chat, start_id = getChatMsgID(start_link)
    except Exception as e:
        return await message.reply(f"**❌ Error parsing starting link:**\n`{e}`")
        
    try:
        await user_client.get_chat(start_chat)
    except Exception:
        pass
        
    end_id = start_id + count - 1
    prefix = start_link.rsplit("/", 1)[0]
    
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancel Batch", callback_data=f"cancel_batch_{user_id}")]]
    )
    
    status_msg = await message.reply(
        f"⏳ **Initializing High-Speed Batch...**\n\n"
        f"📁 **Total files requested:** `{count}`\n"
        f"🔢 **Range:** `{start_id}` to `{end_id}`\n"
        f"🔄 **Downloaded to cache:** `0/{count}`\n"
        f"📤 **Sent to you:** `0/{count}`",
        reply_markup=keyboard
    )
    
    downloaded_data = {}
    status_tracker = {msg_id: "pending" for msg_id in range(start_id, end_id + 1)}
    
    # Fill message queue for background downloaders
    msg_queue = asyncio.Queue()
    for msg_id in range(start_id, end_id + 1):
        await msg_queue.put(msg_id)
        
    # Start 1 background prefetch worker for sequential downloading at maximum single-file speed
    workers = [
        asyncio.create_task(prefetch_worker(start_chat, msg_queue, downloaded_data, status_tracker, message))
        for _ in range(1)
    ]
    
    downloaded = skipped = failed = 0
    processed_media_groups = set()
    
    try:
        for idx, msg_id in enumerate(range(start_id, end_id + 1)):
            if asyncio.current_task().cancelled():
                raise asyncio.CancelledError()
                
            url = f"{prefix}/{msg_id}"
            
            # Wait until prefetch finishes for this file
            while status_tracker[msg_id] in ["pending", "downloading"]:
                active_downloads = []
                for mid, s in status_tracker.items():
                    if s == "downloading":
                        prog = DOWNLOAD_PROGRESS.get(mid, "`0.0%`")
                        active_downloads.append(f"• **Msg {mid}**: {prog}")
                
                downloads_str = "\n".join(active_downloads) if active_downloads else "• None"
                completed_downloads = sum(1 for s in status_tracker.values() if s in ["completed", "skipped", "failed"])
                try:
                    await status_msg.edit_text(
                        f"📥 **Downloading Batch (High Speed)...**\n\n"
                        f"⏳ **Active Downloads Progress:**\n{downloads_str}\n\n"
                        f"📥 **Downloaded to Cache:** `{completed_downloads}/{count}` files\n"
                        f"📤 **Sent to User:** `{downloaded}/{count}` files\n\n"
                        f"✅ **Sent:** `{downloaded}` | ⏭️ **Skipped:** `{skipped}` | ❌ **Failed:** `{failed}`",
                        reply_markup=keyboard
                    )
                except Exception:
                    pass
                await asyncio.sleep(1.0)
                
            status = status_tracker[msg_id]
            data = downloaded_data.get(msg_id, {})
            
            if status == "skipped" or data.get("status") == "skipped":
                skipped += 1
                continue
            elif status == "failed" or data.get("status") == "failed":
                failed += 1
                continue
                
            # Perform sequential uploads
            status_type = data.get("status")
            
            if status_type == "text":
                try:
                    sent_text = await message.reply(data["text"], entities=data["entities"] or None)
                    if forward_chat_id and sent_text:
                        await bot.copy_message(
                            chat_id=forward_chat_id,
                            from_chat_id=sent_text.chat.id,
                            message_id=sent_text.id,
                        )
                    downloaded += 1
                except Exception as e:
                    failed += 1
                    LOGGER(__name__).error(f"Failed to send text: {e}")
                    
            elif status_type == "media_group":
                chat_msg = data["chat_msg"]
                if chat_msg.media_group_id in processed_media_groups:
                    skipped += 1
                    continue
                processed_media_groups.add(chat_msg.media_group_id)
                
                try:
                    await status_msg.edit_text(
                        f"📥 **Downloading Media Group {chat_msg.media_group_id}...**\n\n"
                        f"📤 **Sent to User:** `{downloaded}/{count}` files",
                        reply_markup=keyboard
                    )
                    await processMediaGroup(chat_msg, bot, message, forward_chat_id=forward_chat_id)
                    downloaded += 1
                except Exception as e:
                    failed += 1
                    LOGGER(__name__).error(f"Failed to send media group: {e}")
                    
            elif status_type == "completed":
                media_path = data["media_path"]
                media_type = data["media_type"]
                caption = data["caption"]
                caption_entities = data["caption_entities"]
                
                try:
                    await status_msg.edit_text(
                        f"📤 **Uploading file {idx + 1}/{count}...**\n\n"
                        f"✅ **Sent:** `{downloaded}` | ⏭️ **Skipped:** `{skipped}` | ❌ **Failed:** `{failed}`",
                        reply_markup=keyboard
                    )
                    
                    await send_media(
                        bot,
                        message,
                        media_path,
                        media_type,
                        caption,
                        caption_entities,
                        status_msg,
                        time(),
                        forward_chat_id=forward_chat_id,
                    )
                    downloaded += 1
                except Exception as e:
                    failed += 1
                    LOGGER(__name__).error(f"Failed to upload media: {e}")
                finally:
                    cleanup_download(media_path)
                    
            await asyncio.sleep(0.5)
            
        await status_msg.delete()
        await message.reply(
            "**✅ High-Speed Batch Process Complete!**\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"📥 **Downloaded** : `{downloaded}` file(s)\n"
            f"⏭️ **Skipped**    : `{skipped}` (no content)\n"
            f"❌ **Failed**     : `{failed}` error(s)"
        )
        
    except asyncio.CancelledError:
        try:
            await status_msg.delete()
        except Exception:
            pass
        await message.reply(
            f"❌ **Batch Process Cancelled!**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📥 **Downloaded before cancel:** `{downloaded}` file(s)"
        )
    finally:
        # Cancel all background prefetch workers
        for worker in workers:
            if not worker.done():
                worker.cancel()
        # Cleanup cached files that were downloaded but not uploaded
        for d in downloaded_data.values():
            if d.get("status") == "completed" and d.get("media_path"):
                cleanup_download(d["media_path"])
        # Stop and remove user's custom client from cache to free resources
        if user_id in ACTIVE_CLIENTS:
            custom_client = ACTIVE_CLIENTS.pop(user_id, None)
            if custom_client and custom_client != user and custom_client.is_connected:
                try:
                    await custom_client.stop()
                except Exception as e:
                    LOGGER(__name__).error(f"Error stopping custom client for user {user_id}: {e}")


async def handle_download(bot: Client, message: Message, post_url: str):
    global forward_chat_id
    async with download_semaphore:
        if "?" in post_url:
            post_url = post_url.split("?", 1)[0]

        try:
            effective_forward_chat_id = None
            if forward_chat_id:
                ok, err_msg = await check_forward_permission(bot, forward_chat_id)
                if not ok:
                    await message.reply(
                        f"⚠️ **Forward chat misconfigured:** {err_msg}\n\n"
                        "The file will be sent to you only."
                    )
                else:
                    effective_forward_chat_id = forward_chat_id

            chat_id, message_id = getChatMsgID(post_url)
            user_client = await get_client_for_user(message.from_user.id)
            chat_message = await user_client.get_messages(chat_id=chat_id, message_ids=message_id)

            LOGGER(__name__).info(f"Downloading media from URL: {post_url}")

            if chat_message.document or chat_message.video or chat_message.audio:
                file_size = (
                    chat_message.document.file_size
                    if chat_message.document
                    else chat_message.video.file_size
                    if chat_message.video
                    else chat_message.audio.file_size
                )

                is_premium = False
                if user_client.me:
                    is_premium = user_client.me.is_premium
                else:
                    try:
                        me = await user_client.get_me()
                        is_premium = me.is_premium
                    except Exception:
                        pass

                if not await fileSizeLimit(
                    file_size, message, "download", is_premium
                ):
                    return

            raw_caption, raw_caption_entities = get_raw_text(
                chat_message.caption, chat_message.caption_entities
            )
            raw_text, raw_text_entities = get_raw_text(
                chat_message.text, chat_message.entities
            )

            if chat_message.media_group_id:
                if not await processMediaGroup(chat_message, bot, message, forward_chat_id=effective_forward_chat_id):
                    await message.reply(
                        "**Could not extract any valid media from the media group.**"
                    )
                return

            has_downloadable_media = (
                chat_message.photo
                or chat_message.video
                or chat_message.audio
                or chat_message.document
                or chat_message.voice
                or chat_message.video_note
                or chat_message.animation
                or chat_message.sticker
            )

            if has_downloadable_media:
                start_time = time()
                progress_message = await message.reply("**📥 Downloading Progress...**")

                filename = get_file_name(message_id, chat_message)
                download_path = get_download_path(message.id, filename)

                media_path = None
                for attempt in range(2):
                    try:
                        media_path = await chat_message.download(
                            file_name=download_path,
                            progress=Leaves.progress_for_pyrogram,
                            progress_args=progressArgs(
                                "📥 Downloading Progress", progress_message, start_time
                            ),
                        )
                        break
                    except FloodWait as e:
                        wait_s = int(getattr(e, "value", 0) or 0)
                        LOGGER(__name__).warning(f"FloodWait while downloading media: {wait_s}s")
                        if wait_s > 0 and attempt == 0:
                            await asyncio.sleep(wait_s + 1)
                            continue
                        raise

                if not media_path or not os.path.exists(media_path):
                    await progress_message.edit("**❌ Download failed: File not saved properly**")
                    return

                file_size = os.path.getsize(media_path)
                if file_size == 0:
                    await progress_message.edit("**❌ Download failed: File is empty**")
                    cleanup_download(media_path)
                    return

                LOGGER(__name__).info(f"Downloaded media: {media_path} (Size: {file_size} bytes)")

                media_type = (
                    "photo"
                    if chat_message.photo
                    else "video"
                    if chat_message.video
                    else "audio"
                    if chat_message.audio
                    else "document"
                )
                await send_media(
                    bot,
                    message,
                    media_path,
                    media_type,
                    raw_caption,
                    raw_caption_entities,
                    progress_message,
                    start_time,
                    forward_chat_id=effective_forward_chat_id,
                )

                cleanup_download(media_path)
                await progress_message.delete()

            elif chat_message.poll:
                await message.reply("**This post contains a poll which cannot be downloaded.**")

            elif chat_message.text or chat_message.caption:
                txt = raw_text or raw_caption
                ents = raw_text_entities if raw_text else raw_caption_entities
                sent_text = None
                try:
                    sent_text = await message.reply(txt, entities=ents or None)
                except BadRequest as e:
                    if "ENTITY_TEXT_INVALID" in str(e):
                        LOGGER(__name__).warning(f"ENTITY_TEXT_INVALID in text reply, retrying without entities: {e}")
                        sent_text = await message.reply(txt)
                    else:
                        raise
                if effective_forward_chat_id and sent_text:
                    try:
                        await bot.copy_message(
                            chat_id=effective_forward_chat_id,
                            from_chat_id=sent_text.chat.id,
                            message_id=sent_text.id,
                        )
                        LOGGER(__name__).info(f"Copied text message to chat: {effective_forward_chat_id}")
                    except Exception as e:
                        LOGGER(__name__).error(f"Failed to copy text message to {effective_forward_chat_id}: {e}")
            else:
                await message.reply("**No media or text found in the post URL.**")

        except FloodWait as e:
            wait_s = int(getattr(e, "value", 0) or 0)
            LOGGER(__name__).warning(f"FloodWait in handle_download: {wait_s}s")
            if wait_s > 0:
                await asyncio.sleep(wait_s + 1)
            return
        except PeerIdInvalid as e:
            LOGGER(__name__).error(f"PeerIdInvalid for {post_url}: {e}")
            await message.reply(
                "**❌ Access Denied**\n\n"
                "The user client cannot access this chat.\n"
                "Make sure the user account has joined the channel/group.\n\n"
                f"**Details:** `{e}`"
            )
        except BadRequest as e:
            LOGGER(__name__).error(f"BadRequest for {post_url}: {e}")
            await message.reply(
                "**❌ Bad Request**\n\n"
                f"Telegram returned an error: `{e}`\n\n"
                "This may happen if the message ID is invalid or the chat is inaccessible."
            )
        except KeyError as e:
            LOGGER(__name__).error(f"KeyError for {post_url}: {e}")
            await message.reply(f"**❌ Invalid URL format:** `{e}`")
        except Exception as e:
            LOGGER(__name__).error(f"Unexpected error for {post_url}: {e}")
            await message.reply(f"**❌ Unexpected error:** `{e}`")


@bot.on_message(filters.command("dl") & filters.private)
async def download_media(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command.**")
        return

    post_url = message.command[1]
    await track_task(handle_download(bot, message, post_url))


@bot.on_message(filters.command("bdl") & filters.private)
async def download_range(bot: Client, message: Message):
    args = message.text.split()

    if len(args) != 3 or not all(arg.startswith("https://t.me/") for arg in args[1:]):
        await message.reply(
            "🚀 **Batch Download Process**\n"
            "`/bdl start_link end_link`\n\n"
            "💡 **Example:**\n"
            "`/bdl https://t.me/mychannel/100 https://t.me/mychannel/120`"
        )
        return

    try:
        start_chat, start_id = getChatMsgID(args[1])
        end_chat,   end_id   = getChatMsgID(args[2])
    except Exception as e:
        return await message.reply(f"**❌ Error parsing links:\n{e}**")

    if start_chat != end_chat:
        return await message.reply("**❌ Both links must be from the same channel.**")
    if start_id > end_id:
        return await message.reply("**❌ Invalid range: start ID cannot exceed end ID.**")

    user_client = await get_client_for_user(message.from_user.id)
    try:
        await user_client.get_chat(start_chat)
    except Exception:
        pass

    prefix = args[1].rsplit("/", 1)[0]
    loading = await message.reply(f"📥 **Downloading posts {start_id}–{end_id}…**")

    downloaded = skipped = failed = 0
    processed_media_groups = set()
    batch_tasks = []
    BATCH_SIZE = PyroConf.BATCH_SIZE

    for msg_id in range(start_id, end_id + 1):
        url = f"{prefix}/{msg_id}"
        try:
            chat_msg = await user_client.get_messages(chat_id=start_chat, message_ids=msg_id)
            if not chat_msg:
                skipped += 1
                continue

            if chat_msg.media_group_id:
                if chat_msg.media_group_id in processed_media_groups:
                    skipped += 1
                    continue
                processed_media_groups.add(chat_msg.media_group_id)

            has_media = bool(chat_msg.media_group_id or chat_msg.media)
            has_text  = bool(chat_msg.text or chat_msg.caption)
            if not (has_media or has_text):
                skipped += 1
                continue

            task = track_task(handle_download(bot, message, url))
            batch_tasks.append(task)

            if len(batch_tasks) >= BATCH_SIZE:
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, asyncio.CancelledError):
                        await loading.delete()
                        return await message.reply(
                            f"**❌ Batch canceled** after downloading `{downloaded}` posts."
                        )
                    elif isinstance(result, Exception):
                        failed += 1
                        LOGGER(__name__).error(f"Error: {result}")
                    else:
                        downloaded += 1

                batch_tasks.clear()
                await asyncio.sleep(PyroConf.FLOOD_WAIT_DELAY)

        except Exception as e:
            failed += 1
            LOGGER(__name__).error(f"Error at {url}: {e}")

    if batch_tasks:
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                failed += 1
            else:
                downloaded += 1

    await loading.delete()
    await message.reply(
        "**✅ Batch Process Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📥 **Downloaded** : `{downloaded}` post(s)\n"
        f"⏭️ **Skipped**    : `{skipped}` (no content)\n"
        f"❌ **Failed**     : `{failed}` error(s)"
    )


@bot.on_message(filters.command(["contact", "report"]) & filters.private)
async def contact_command_handler(_, message: Message):
    user_id = message.from_user.id
    USER_STATES[user_id] = {"state": "waiting_report_message"}
    await message.reply(
        "🚨 **Report Bug / Contact Support**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Please type and send your message below. It will be securely delivered to the administrator to protect your privacy.\n\n"
        "💬 _Send `/cancel` to abort._"
    )


@bot.on_message(filters.private & ~filters.command(["start", "help", "dl", "bdl", "stats", "logs", "killall", "cleanup", "cancel", "batch", "contact", "report", "login", "logout"]))
async def handle_any_message(bot: Client, message: Message):
    user_id = message.from_user.id
    
    # 1. Two-way Admin Support Reply Routing
    try:
        admin = await user.get_users("Retro511")
        admin_id = admin.id
    except Exception:
        admin_id = None
        
    if user_id == admin_id and message.reply_to_message:
        reply_to_id = message.reply_to_message.id
        if reply_to_id in FORWARDED_REPORTS:
            target_user_id = FORWARDED_REPORTS[reply_to_id]
            try:
                await bot.send_message(
                    chat_id=target_user_id,
                    text="✉️ **Support Reply from Administrator**\n"
                         "━━━━━━━━━━━━━━━━━━━━\n"
                )
                await message.copy(chat_id=target_user_id)
                await message.reply(f"✅ **Your reply has been successfully delivered to the user (ID: `{target_user_id}`)!**")
            except Exception as e:
                await message.reply(f"❌ **Failed to deliver reply to user:** `{e}`")
            return
            
    # 2. User Wizard State Handling
    if user_id in USER_STATES:
        state_data = USER_STATES[user_id]
        current_state = state_data["state"]
        
        if current_state == "waiting_phone_number":
            phone_number = message.text.strip() if message.text else ""
            if not phone_number.startswith("+") or not phone_number[1:].replace(" ", "").isdigit():
                await message.reply(
                    "❌ **Invalid Phone Number!**\n\n"
                    "Please send your phone number starting with `+` followed by country code and digits.\n"
                    "Example: `+1234567890`"
                )
                return
            
            # Start temporary login client
            loading = await message.reply("⏳ **Sending OTP verification code to your Telegram...**")
            
            try:
                temp_client = Client(
                    name=f"temp_login_{user_id}",
                    api_id=PyroConf.API_ID,
                    api_hash=PyroConf.API_HASH,
                    in_memory=True
                )
                await temp_client.connect()
                
                code_info = await temp_client.send_code(phone_number)
                
                USER_STATES[user_id] = {
                    "state": "waiting_otp_code",
                    "phone_number": phone_number,
                    "phone_code_hash": code_info.phone_code_hash,
                    "temp_client": temp_client
                }
                
                await loading.edit_text(
                    "📩 **OTP sent successfully!**\n\n"
                    "Please check your official Telegram app or SMS and send the **Verification Code** here.\n"
                    "💡 If the code has spaces, you can send it with or without spaces.\n\n"
                    "💬 _Send `/cancel` to abort._"
                )
            except Exception as e:
                LOGGER(__name__).error(f"Failed to send code to {phone_number}: {e}")
                await loading.edit_text(f"❌ **Failed to send OTP:** `{e}`\n\nPlease check the phone number and try again by sending `/login`.")
                USER_STATES.pop(user_id, None)
            return

        elif current_state == "waiting_otp_code":
            otp_code = message.text.strip().replace(" ", "") if message.text else ""
            if not otp_code:
                await message.reply("❌ **Invalid OTP!** Please send the numeric OTP code.")
                return
                
            state_data = USER_STATES[user_id]
            temp_client = state_data["temp_client"]
            phone_number = state_data["phone_number"]
            phone_code_hash = state_data["phone_code_hash"]
            
            loading = await message.reply("⏳ **Verifying OTP code...**")
            
            try:
                await temp_client.sign_in(
                    phone_number=phone_number,
                    phone_code_hash=phone_code_hash,
                    phone_code=otp_code
                )
                
                # If succeeds, export session string!
                session_str = await temp_client.export_session_string()
                set_user_session(user_id, session_str)
                
                try:
                    await temp_client.disconnect()
                except Exception:
                    pass
                    
                USER_STATES.pop(user_id, None)
                await loading.edit_text(
                    "🎉 **Login Successful!**\n━━━━━━━━━━━━━━━━━━━━\n\n"
                    "You are now securely logged in with your personal Telegram account!\n"
                    "All downloads and batches you request will use your session, giving you full access to all private channels you've joined."
                )
            except SessionPasswordNeeded:
                # 2FA required
                USER_STATES[user_id]["state"] = "waiting_2fa_password"
                await loading.edit_text(
                    "🔒 **2FA Password Required!**\n━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Your account has two-step verification enabled.\n"
                    "Please send your **2FA Password** now.\n\n"
                    "💬 _Send `/cancel` to abort._"
                )
            except Exception as e:
                LOGGER(__name__).error(f"OTP verification failed: {e}")
                await loading.edit_text(f"❌ **Verification failed:** `{e}`\n\nPlease try again by sending `/login`.")
                try:
                    await temp_client.disconnect()
                except Exception:
                    pass
                USER_STATES.pop(user_id, None)
            return

        elif current_state == "waiting_2fa_password":
            password = message.text.strip() if message.text else ""
            if not password:
                await message.reply("❌ **Invalid password!** Please send your 2FA password.")
                return
                
            state_data = USER_STATES[user_id]
            temp_client = state_data["temp_client"]
            
            loading = await message.reply("⏳ **Verifying 2FA password...**")
            
            try:
                await temp_client.check_password(password)
                
                # If succeeds, export session string!
                session_str = await temp_client.export_session_string()
                set_user_session(user_id, session_str)
                
                try:
                    await temp_client.disconnect()
                except Exception:
                    pass
                    
                USER_STATES.pop(user_id, None)
                await loading.edit_text(
                    "🎉 **Login Successful!**\n━━━━━━━━━━━━━━━━━━━━\n\n"
                    "You are now securely logged in with your personal Telegram account!\n"
                    "All downloads and batches you request will use your session, giving you full access to all private channels you've joined."
                )
            except Exception as e:
                LOGGER(__name__).error(f"2FA verification failed: {e}")
                await loading.edit_text(f"❌ **2FA password verification failed:** `{e}`\n\nPlease try again by sending `/login`.")
                try:
                    await temp_client.disconnect()
                except Exception:
                    pass
                USER_STATES.pop(user_id, None)
            return
        
        elif current_state == "waiting_start_link":
            link = message.text.strip() if message.text else ""
            if not link.startswith("https://t.me/"):
                await message.reply(
                    "❌ **Invalid Link!**\n\n"
                    "Please send a valid Telegram post link starting with `https://t.me/`.\n"
                    "Example: `https://t.me/mychannel/100`"
                )
                return
            
            USER_STATES[user_id] = {
                "state": "waiting_file_count",
                "start_link": link
            }
            await message.reply(
                "🔢 **Starting link received!**\n\n"
                "Now, please send the **number of files** you want to download (no limit, e.g. `10` or `50`):\n\n"
                "💬 _Send `/cancel` to abort._"
            )
            return
            
        elif current_state == "waiting_file_count":
            count_str = message.text.strip() if message.text else ""
            if not count_str.isdigit() or int(count_str) <= 0:
                await message.reply("❌ **Invalid count!** Please send a valid positive number.")
                return
                
            count = int(count_str)
            start_link = state_data["start_link"]
            
            # Clear state
            USER_STATES.pop(user_id, None)
            
            # Start batch task
            task = asyncio.create_task(run_interactive_batch(bot, message, start_link, count))
            ACTIVE_BATCHES[user_id] = task
            
            def _cleanup_batch(_):
                ACTIVE_BATCHES.pop(user_id, None)
            task.add_done_callback(_cleanup_batch)
            return
            
        elif current_state == "waiting_report_message":
            # Clear state first
            USER_STATES.pop(user_id, None)
            
            loading = await message.reply("⏳ **Sending report to administrator...**")
            
            try:
                # Resolve admin ID dynamically using user session client
                admin = await user.get_users("Retro511")
                admin_id = admin.id
                
                # Forward report details to admin
                header = (
                    f"📩 **New Support Report Received!**\n"
                    f"👤 **From User**: {message.from_user.mention} (ID: `{message.from_user.id}`)\n"
                    f"💬 **User Username**: @{message.from_user.username if message.from_user.username else 'None'}\n"
                    f"━━━━━━━━━━━━━━━━━━━━"
                )
                
                # Send header message to admin
                sent_header = await bot.send_message(chat_id=admin_id, text=header)
                
                # Copy user's actual message to the admin
                copied_msg = await message.copy(chat_id=admin_id)
                
                # Store mappings for two-way reply routing
                FORWARDED_REPORTS[sent_header.id] = message.from_user.id
                FORWARDED_REPORTS[copied_msg.id] = message.from_user.id
                
                await loading.edit_text(
                    "✅ **Your report has been successfully sent!**\n\n"
                    "The administrator has been notified. Thank you for your feedback!"
                )
                
            except Exception as e:
                LOGGER(__name__).error(f"Failed to send report to admin: {e}")
                await loading.edit_text(
                    "❌ **Failed to deliver your report.**\n\n"
                    "An unexpected error occurred. Please try again later."
                )
            return

    if message.text and not message.text.startswith("/"):
        await track_task(handle_download(bot, message, message.text))


@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    total, used, free = shutil.disk_usage(".")
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
    recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
    cpuUsage = psutil.cpu_percent(interval=0.5)
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage("/").percent
    process = psutil.Process(os.getpid())

    stats = (
        "**≧◉◡◉≦ Bot is Up and Running successfully.**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ Total Disk Space:** `{total}`\n"
        f"**➜ Used:** `{used}`\n"
        f"**➜ Free:** `{free}`\n"
        f"**➜ Memory Usage:** `{round(process.memory_info()[0] / 1024**2)} MiB`\n\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`\n\n"
        f"**➜ CPU:** `{cpuUsage}%` | "
        f"**➜ RAM:** `{memory}%` | "
        f"**➜ DISK:** `{disk}%`"
    )
    await message.reply(stats)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    if os.path.exists("logs.txt"):
        await message.reply_document(document="logs.txt", caption="**Logs**")
    else:
        await message.reply("**Not exists**")


@bot.on_message(filters.command("killall") & filters.private)
async def cancel_all_tasks(_, message: Message):
    cancelled = 0
    for task in list(RUNNING_TASKS):
        if not task.done():
            task.cancel()
            cancelled += 1
    await message.reply(f"**Cancelled {cancelled} running task(s).**")


async def initialize():
    global download_semaphore, forward_chat_id
    init_db()
    download_semaphore = asyncio.Semaphore(PyroConf.MAX_CONCURRENT_DOWNLOADS)

    if PyroConf.FORWARD_CHAT_ID:
        forward_chat_id = await resolve_forward_chat_id(PyroConf.FORWARD_CHAT_ID)
        LOGGER(__name__).info(f"Auto-forward enabled. Target chat: {forward_chat_id}")

async def start_bot():
    LOGGER(__name__).info("Bot Starting...")
    await user.start()
    await bot.start()
    await initialize()
    
    # Set bot commands for menu suggestions (since bot client is now started!)
    try:
        await bot.set_bot_commands([
            BotCommand("start", "Start the bot"),
            BotCommand("help", "Show help menu & commands"),
            BotCommand("login", "Log in to your Telegram account"),
            BotCommand("logout", "Log out and delete session securely"),
            BotCommand("batch", "Start interactive batch downloader"),
            BotCommand("cancel", "Cancel current active batch/wizard"),
            BotCommand("report", "Report a bug or contact support"),
            BotCommand("contact", "Contact the administrator"),
            BotCommand("dl", "Download a single post media by link"),
            BotCommand("bdl", "Download a range of posts (classic mode)"),
            BotCommand("cleanup", "Clean up temporary downloaded files from disk"),
            BotCommand("stats", "Show bot's system stats & uptime"),
            BotCommand("logs", "Get bot execution log file"),
            BotCommand("killall", "Force cancel all background downloads")
        ])
        LOGGER(__name__).info("Bot commands set successfully!")
    except Exception as e:
        LOGGER(__name__).error(f"Failed to set bot commands: {e}")
        
    LOGGER(__name__).info("Bot Started!")
    await idle()
    await user.stop()
    await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(start_bot())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        LOGGER(__name__).info("Bot Stopped")
