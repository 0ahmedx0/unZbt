# Copyright (c) 2022 - 2024 EDM115
import asyncio
import concurrent.futures
import os
import re
import shutil
import glob
import string
import random

import unzip_http

from aiofiles import open as openfile
from aiohttp import ClientSession, InvalidURL
from email.parser import Parser
from email.policy import default
from fnmatch import fnmatch
from pyrogram import Client
from pyrogram.errors import ReplyMarkupTooLong, FloodWait
from pyrogram.types import CallbackQuery, InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton
from time import time
from urllib.parse import unquote

from .bot_data import Buttons, ERROR_MSGS, Messages
from .commands import get_stats, https_url_regex, sufficient_disk_space
from .ext_script.custom_thumbnail import silent_del
from .ext_script.ext_helper import (
    _test_with_7z_helper,
    extr_files,
    get_files,
    make_keyboard,
    make_keyboard_empty,
    merge_files,
    split_files,
)
from .ext_script.up_helper import answer_query, get_size, send_file, send_url_logs
from config import Config
from unzipper import LOGGER, unzipperbot
from unzipper.helpers.database import (
    add_cancel_task,
    add_ongoing_task,
    count_ongoing_tasks,
    del_cancel_task,
    del_merge_task,
    del_ongoing_task,
    del_thumb_db,
    get_cancel_task,
    get_maintenance,
    get_merge_task_message_id,
    get_ongoing_tasks,
    set_upload_mode,
    update_thumb,
    update_uploaded,
)
from unzipper.helpers.unzip_help import (
    extentions_list,
    humanbytes,
    progress_for_pyrogram,
    TimeFormatter,
)

split_file_pattern = r"\.(?:z\d+|r\d{2})$"
rar_file_pattern = r"\.part\d+\.rar$"
telegram_url_pattern = r"(?:http[s]?:\/\/)?(?:www\.)?t\.me\/([a-zA-Z0-9_]+)\/(\d+)"


# ==========================================
# وظائف متقدمة للحماية وعدم إيقاف السيرفر
# ==========================================
class DummyMessage:
    async def edit(self, *args, **kwargs):
        pass

    async def reply(self, *args, **kwargs):
        pass

    async def delete(self, *args, **kwargs):
        pass

    async def forward(self, *args, **kwargs):
        return self


def _rmtree_sync(path):
    shutil.rmtree(path, ignore_errors=True)

async def async_rmtree(path):
    if os.path.exists(path):
        await asyncio.to_thread(_rmtree_sync, path)

async def async_remove(path):
    if os.path.exists(path):
        await asyncio.to_thread(os.remove, path)

async def async_makedirs(path):
    await asyncio.to_thread(os.makedirs, path, exist_ok=True)


# ==========================================
# إصلاح الأحرف التالفة (التي تعطل التيليجرام)
# ==========================================
def sanitize_filename(filepath):
    dir_name = os.path.dirname(filepath)
    base_name = os.path.basename(filepath)
    clean_name = base_name.encode('utf-8', 'ignore').decode('utf-8')
    if not clean_name or len(clean_name.strip()) == 0:
        ext = os.path.splitext(base_name)[1]
        random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        clean_name = f"recovered_file_{random_str}{ext}"
    if clean_name != base_name:
        new_path = os.path.join(dir_name, clean_name)
        try:
            os.rename(filepath.encode('utf-8', 'surrogateescape'), new_path.encode('utf-8', 'surrogateescape'))
            return new_path
        except:
            return filepath
    return filepath


async def download(url, path):
    try:
        async with ClientSession() as session, session.get(
            url, timeout=None, allow_redirects=True
        ) as resp, openfile(path, mode="wb") as file:
            async for chunk in resp.content.iter_chunked(Config.CHUNK_SIZE):
                await file.write(chunk)
    except InvalidURL:
        LOGGER.error(Messages.INVALID_URL)
    except Exception:
        LOGGER.error(Messages.ERR_DL.format(url))


async def download_with_progress(url, path, message, unzip_bot):
    try:
        async with ClientSession() as session, session.get(
            url, timeout=None, allow_redirects=True
        ) as resp:
            total_size = int(resp.headers.get("Content-Length", 0))
            current_size = 0
            start_time = time()

            async with openfile(path, mode="wb") as file:
                async for chunk in resp.content.iter_chunked(Config.CHUNK_SIZE):
                    if message.from_user is not None and await get_cancel_task(
                        message.from_user.id
                    ):
                        try:
                            await message.edit(text=Messages.DL_STOPPED)
                        except FloodWait as e:
                            await asyncio.sleep(e.value)
                            await message.edit(text=Messages.DL_STOPPED)
                        await del_cancel_task(message.from_user.id)
                        return False

                    await file.write(chunk)
                    current_size += len(chunk)
                    await progress_for_pyrogram(
                        current_size,
                        total_size,
                        Messages.DL_URL.format(url),
                        message,
                        start_time,
                        unzip_bot,
                    )

    except Exception:
        LOGGER.error(Messages.ERR_DL.format(url))


def get_zip_http(url):
    rzf = unzip_http.RemoteZipFile(url)
    paths = rzf.namelist()
    return rzf, paths


async def async_generator(iterable):
    for item in iterable:
        yield item


def chunk_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


async def send_album_batch(unzip_bot, chat_id, batch, caption_msg=""):
    media_group = []
    for i, file_path in enumerate(batch):
        ext = file_path.lower().split('.')[-1]
        caption = caption_msg if i == 0 else ""
        
        if ext in ['mp4', 'mov', 'mkv', 'avi']:
            media_group.append(InputMediaVideo(media=file_path, caption=caption))
        elif ext in ['jpg', 'jpeg', 'png', 'webp']:
            media_group.append(InputMediaPhoto(media=file_path, caption=caption))
        elif ext == 'gif':
            media_group.append(InputMediaVideo(media=file_path, caption=caption))
            
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await unzip_bot.send_media_group(chat_id=chat_id, media=media_group)
            return True
        except FloodWait as e:
            LOGGER.warning(f"FloodWait hit! Sleeping for {e.value + 2} seconds.")
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            LOGGER.error(f"Failed to send album on attempt {attempt+1}: {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(2)
            
    return False


async def process_album_pagination(query, unzip_bot, folder_id, c_id, current_type, index):
    user_id = query.from_user.id
    if "/" in str(folder_id):
        file_path = f"{folder_id}/extracted"
        base_folder = folder_id
        folder_name_only = os.path.basename(folder_id)
    else:
        file_path = f"{Config.DOWNLOAD_LOCATION}/{folder_id}/extracted"
        base_folder = f"{Config.DOWNLOAD_LOCATION}/{folder_id}"
        folder_name_only = folder_id
        
    raw_paths = await get_files(path=file_path)
    active_log_msg = DummyMessage()

    if not raw_paths:
        await async_rmtree(base_folder)
        await del_ongoing_task(user_id)
        try:
            await query.message.delete()
        except:
            pass
        try:
            await unzip_bot.send_message(chat_id=int(c_id), text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME)
        except:
            pass
        return

    paths = []
    for rp in raw_paths:
        safe_p = sanitize_filename(rp)
        paths.append(safe_p)

    vids = []
    imgs = []
    others = []
    for p in paths:
        ext = p.lower().split('.')[-1]
        if ext in ['mp4', 'mov', 'mkv', 'avi']:
            vids.append(p)
        elif ext in ['jpg', 'jpeg', 'png', 'webp', 'gif']:
            imgs.append(p)
        else:
            others.append(p)

    vid_chunks = chunk_list(vids, 10)
    img_chunks = chunk_list(imgs, 10)
    index = int(index)

    if current_type == "video" and index < len(vid_chunks):
        batch = vid_chunks[index]
        try:
            msg = await unzip_bot.send_message(
                chat_id=int(c_id), text=f"⚡️ Sending Video Album ({index + 1}/{len(vid_chunks)}) ..."
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            msg = await unzip_bot.send_message(
                chat_id=int(c_id), text=f"⚡️ Sending Video Album ({index + 1}/{len(vid_chunks)}) ..."
            )
        
        success = await send_album_batch(
            unzip_bot, int(c_id), batch, f"📁 Video Album {index+1} / {len(vid_chunks)}"
        )
        
        if not success:
            for file in batch:
                await send_file(
                    unzip_bot=unzip_bot,
                    c_id=int(c_id),
                    doc_f=file,
                    query=query,
                    full_path=base_folder,
                    log_msg=active_log_msg,
                    split=False
                )

        next_idx = index + 1
        if next_idx < len(vid_chunks):
            nxt_tp = "video"
            btn_text = f"Next Video Album ({next_idx + 1}) ⏭"
        elif imgs:
            nxt_tp = "image"
            next_idx = 0
            btn_text = f"Start Image Albums (Total: {len(img_chunks)}) 🖼"
        elif others:
            nxt_tp = "other"
            next_idx = 0
            btn_text = f"Upload Other Files ({len(others)}) 📄"
        else:
            nxt_tp = "done"
            next_idx = 0
            btn_text = "Finish Sequence ✅"

        buttons = [
            [InlineKeyboardButton(btn_text, callback_data=f"nxtalb|{folder_name_only}|{c_id}|{nxt_tp}|{next_idx}")]
        ]
        
        if len(vid_chunks) > 1 and current_type == "video":
            buttons.append(
                [InlineKeyboardButton("🔢 Jump to Album...", callback_data=f"jumpalb|{folder_name_only}|{c_id}|video|{len(vid_chunks)}")]
            )
            
        buttons.append([InlineKeyboardButton("Cancel ❌", callback_data=f"cancel_folder|{folder_name_only}")])
        markup = InlineKeyboardMarkup(buttons)
        
        try:
            await msg.delete()
        except:
            pass
            
        try:
            await unzip_bot.send_message(
                chat_id=int(c_id), 
                text=f"✅ Video Album **{index + 1}** out of **{len(vid_chunks)}** sent successfully.", 
                reply_markup=markup
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await unzip_bot.send_message(
                chat_id=int(c_id), 
                text=f"✅ Video Album **{index + 1}** out of **{len(vid_chunks)}** sent successfully.", 
                reply_markup=markup
            )
        return

    elif current_type == "image" and index < len(img_chunks):
        batch = img_chunks[index]
        try:
            msg = await unzip_bot.send_message(
                chat_id=int(c_id), text=f"⚡️ Sending Image Album ({index + 1}/{len(img_chunks)}) ..."
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            msg = await unzip_bot.send_message(
                chat_id=int(c_id), text=f"⚡️ Sending Image Album ({index + 1}/{len(img_chunks)}) ..."
            )
        
        success = await send_album_batch(
            unzip_bot, int(c_id), batch, f"📁 Image Album {index+1} / {len(img_chunks)}"
        )
        
        if not success:
            for file in batch:
                await send_file(
                    unzip_bot=unzip_bot,
                    c_id=int(c_id),
                    doc_f=file,
                    query=query,
                    full_path=base_folder,
                    log_msg=active_log_msg,
                    split=False
                )

        next_idx = index + 1
        if next_idx < len(img_chunks):
            nxt_tp = "image"
            btn_text = f"Next Image Album ({next_idx + 1}) ⏭"
        elif others:
            nxt_tp = "other"
            next_idx = 0
            btn_text = f"Upload Other Files ({len(others)}) 📄"
        else:
            nxt_tp = "done"
            next_idx = 0
            btn_text = "Finish Sequence ✅"

        buttons = [
            [InlineKeyboardButton(btn_text, callback_data=f"nxtalb|{folder_name_only}|{c_id}|{nxt_tp}|{next_idx}")]
        ]
        
        if len(img_chunks) > 1 and current_type == "image":
            buttons.append(
                [InlineKeyboardButton("🔢 Jump to Album...", callback_data=f"jumpalb|{folder_name_only}|{c_id}|image|{len(img_chunks)}")]
            )
            
        buttons.append([InlineKeyboardButton("Cancel ❌", callback_data=f"cancel_folder|{folder_name_only}")])
        markup = InlineKeyboardMarkup(buttons)
        
        try:
            await msg.delete()
        except:
            pass
            
        try:
            await unzip_bot.send_message(
                chat_id=int(c_id), 
                text=f"✅ Image Album **{index + 1}** out of **{len(img_chunks)}** sent successfully.", 
                reply_markup=markup
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await unzip_bot.send_message(
                chat_id=int(c_id), 
                text=f"✅ Image Album **{index + 1}** out of **{len(img_chunks)}** sent successfully.", 
                reply_markup=markup
            )
        return

    elif current_type == "other":
        try:
            await query.message.edit(f"⏳ Sending {len(others)} Other Files Individually... 📄")
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass
        
        for file in others:
            fsize = await get_size(file)
            split = False
            
            if fsize > Config.TG_MAX_SIZE:
                split = True
                
            if not split:
                await send_file(
                    unzip_bot=unzip_bot,
                    c_id=int(c_id),
                    doc_f=file,
                    query=query,
                    full_path=base_folder,
                    log_msg=active_log_msg,
                    split=False
                )
            else:
                fname = file.split("/")[-1].encode('utf-8', 'ignore').decode('utf-8')
                smessage = await unzip_bot.send_message(
                    chat_id=user_id, text=Messages.SPLITTING.format(fname)
                )
                splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}_{int(time())}"
                await async_makedirs(splitteddir)
                splittedfiles = await split_files(file, f"{splitteddir}/{fname}", Config.TG_MAX_SIZE)
                
                if not splittedfiles:
                    await async_rmtree(splitteddir)
                    continue
                    
                try:
                    await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                except:
                    pass
                    
                async for s_file in async_generator(splittedfiles):
                    await send_file(
                        unzip_bot=unzip_bot,
                        c_id=user_id,
                        doc_f=s_file,
                        query=query,
                        full_path=splitteddir,
                        log_msg=active_log_msg,
                        split=True
                    )
                
                await async_rmtree(splitteddir)
                try:
                    await smessage.delete()
                except:
                    pass
                    
        current_type = "done"

    if current_type == "done":
        await async_rmtree(base_folder)
        await update_uploaded(user_id, upload_count=1)
        await del_ongoing_task(user_id)
        try:
            await unzip_bot.send_message(
                chat_id=int(c_id), text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
            )
        except:
            pass


# Callbacks
@unzipperbot.on_callback_query()
async def unzipper_cb(unzip_bot: Client, query: CallbackQuery):
    uid = query.from_user.id
    
    if uid != Config.BOT_OWNER:  # skipcq: PTC-W0048
        ogtasks = await get_ongoing_tasks()
        user_tasks_count = 0
        for ogtask in ogtasks:
            if ogtask.get("user_id") == uid:
                user_tasks_count += 1
                
        if user_tasks_count >= 5:
            await unzip_bot.send_message(
                chat_id=uid,
                text="❌ You have reached the maximum limit of 5 concurrent tasks. Please wait for them to finish."
            )
            return

    if uid != Config.BOT_OWNER and await get_maintenance():
        await answer_query(query, Messages.MAINTENANCE_ON)
        return

    sent_files = 0
    global log_msg
    log_msg = DummyMessage()

    if query.data == "megoinhome":
        await query.edit_message_text(
            text=Messages.START_TEXT.format(query.from_user.mention),
            reply_markup=Buttons.START_BUTTON,
        )

    elif query.data == "helpcallback":
        await query.edit_message_text(
            text=Messages.HELP_TXT, reply_markup=Buttons.ME_GOIN_HOME
        )

    elif query.data == "aboutcallback":
        await query.edit_message_text(
            text=Messages.ABOUT_TXT,
            reply_markup=Buttons.ME_GOIN_HOME,
            disable_web_page_preview=True,
        )

    elif query.data == "donatecallback":
        await query.edit_message_text(
            text=Messages.DONATE_TEXT,
            reply_markup=Buttons.ME_GOIN_HOME,
            disable_web_page_preview=True,
        )

    elif query.data.startswith("statscallback"):
        if query.data.endswith("refresh"):
            await query.edit_message_text(text=Messages.REFRESH_STATS)
        text_stats = await get_stats(query.from_user.id)
        await query.edit_message_text(
            text=text_stats,
            reply_markup=Buttons.REFRESH_BUTTON,
        )

    elif query.data == "canceldownload":
        await add_cancel_task(query.from_user.id)

    elif query.data == "check_thumb":
        user_id = query.from_user.id
        thumb_location = Config.THUMB_LOCATION + "/" + str(user_id) + ".jpg"
        await unzip_bot.send_photo(
            chat_id=user_id, photo=thumb_location, caption=Messages.ACTUAL_THUMB
        )
        await unzip_bot.delete_messages(chat_id=user_id, message_ids=query.message.id)
        await unzip_bot.send_message(
            chat_id=user_id,
            text=Messages.EXISTING_THUMB,
            reply_markup=Buttons.THUMB_FINAL,
        )

    elif query.data == "check_before_del":
        user_id = query.from_user.id
        thumb_location = Config.THUMB_LOCATION + "/" + str(user_id) + ".jpg"
        await unzip_bot.send_photo(
            chat_id=user_id, photo=thumb_location, caption=Messages.ACTUAL_THUMB
        )
        await unzip_bot.delete_messages(chat_id=user_id, message_ids=query.message.id)
        await unzip_bot.send_message(
            chat_id=user_id,
            text=Messages.DEL_CONFIRM_THUMB_2,
            reply_markup=Buttons.THUMB_DEL_2,
        )

    elif query.data.startswith("save_thumb"):
        user_id = query.from_user.id
        replace = query.data.split("|")[1]
        if replace == "replace":
            await silent_del(user_id)
        thumb_location = Config.THUMB_LOCATION + "/" + str(user_id) + ".jpg"
        final_thumb = Config.THUMB_LOCATION + "/waiting_" + str(user_id) + ".jpg"
        try:
            shutil.move(final_thumb, thumb_location)
        except Exception as e:
            LOGGER.warning(Messages.ERROR_THUMB_RENAME)
            LOGGER.error(e)
        try:
            await update_thumb(query.from_user.id)
        except:
            LOGGER.error(Messages.ERROR_THUMB_UPDATE)
        await answer_query(query, Messages.SAVED_THUMBNAIL)

    elif query.data == "del_thumb":
        user_id = query.from_user.id
        thumb_location = Config.THUMB_LOCATION + "/" + str(user_id) + ".jpg"
        try:
            await del_thumb_db(user_id)
        except Exception as e:
            LOGGER.error(Messages.ERROR_THUMB_DEL.format(e))
        try:
            os.remove(thumb_location)
        except:
            pass
        await query.edit_message_text(text=Messages.DELETED_THUMB)

    elif query.data == "nope_thumb":
        user_id = query.from_user.id
        del_1 = Config.THUMB_LOCATION + "/not_resized_" + str(user_id) + ".jpg"
        del_2 = Config.THUMB_LOCATION + "/waiting_" + str(user_id) + ".jpg"
        try:
            os.remove(del_1)
        except:
            pass
        try:
            os.remove(del_2)
        except:
            pass
        await query.edit_message_text(
            text=Messages.CANCELLED_TXT.format(Messages.PROCESS_CANCELLED)
        )

    elif query.data.startswith("set_mode"):
        user_id = query.from_user.id
        mode = query.data.split("|")[1]
        await set_upload_mode(user_id, mode)
        await answer_query(query, Messages.CHANGED_UPLOAD_MODE_TXT.format(mode))


    # ================================================
    # Archive Action (Extraction Logic - Partial Retry Support)
    # ================================================
    elif query.data.startswith("archive_action"):
        data = query.data.split("|")
        action_type = data[1]
        target_uid = int(data[2])
        folder_id = data[3]
        
        needs_password = False
        if len(data) > 4 and data[4] == "P":
            needs_password = True
        
        download_path = f"{Config.DOWNLOAD_LOCATION}/{folder_id}"
        ext_files_dir = f"{download_path}/extracted"
        
        if query.from_user.id != target_uid:
            await answer_query(query, "❌ This task is not yours!", unzip_client=unzip_bot)
            return

        if action_type == "extract":
            try:
                files_in_dir = []
                for f in os.listdir(download_path):
                    if f != "extracted":
                        files_in_dir.append(f)
                        
                if not files_in_dir:
                    await query.message.edit("❌ File not found. It might be deleted.")
                    return
                    
                archive_name = files_in_dir[0]
                archive_path = f"{download_path}/{archive_name}"
            except Exception as e:
                LOGGER.error(f"Error finding archive: {e}")
                await query.message.edit("❌ Error locating the archive.")
                return

            password = None
            if needs_password:
                try:
                    pwd_msg = await unzip_bot.ask(
                        chat_id=query.message.chat.id, 
                        text="🔑 **This archive requires a password!**\nPlease send the password now (you have 60 seconds):", 
                        timeout=60
                    )
                    password = pwd_msg.text
                    try:
                        await pwd_msg.delete()
                    except:
                        pass
                except asyncio.TimeoutError:
                    await del_ongoing_task(target_uid)
                    retry_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Try Password Again", callback_data=f"archive_action|extract|{target_uid}|{folder_id}|P")],
                        [InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")]
                    ])
                    await query.message.edit("❌ **Timeout!** You didn't send the password in time.", reply_markup=retry_markup)
                    return

            try:
                await query.message.edit(Messages.PROCESSING2)
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except:
                pass
                
            await async_makedirs(ext_files_dir)
            
            ext_s_time = time()
            if password:
                extractor = await extr_files(path=ext_files_dir, archive_path=archive_path, password=password)
                ext_e_time = time()
            else:
                tested = await _test_with_7z_helper(archive_path)
                if tested:
                    extractor = await extr_files(path=ext_files_dir, archive_path=archive_path)
                    ext_e_time = time()
                else:
                    extractor = "Error"
                    ext_e_time = time()
            
            paths = await get_files(path=ext_files_dir)
            has_error = any(err in extractor for err in ERROR_MSGS)
            
            if not paths:  # تم الفشل ولم يتم استخراج أي شيء
                await async_rmtree(ext_files_dir) 
                await del_ongoing_task(target_uid) 
                
                retry_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Enter Password Again", callback_data=f"archive_action|extract|{target_uid}|{folder_id}|P")],
                    [InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")]
                ])
                try:
                    await query.message.edit("❌ **Extraction failed!**\nEither password was wrong, archive is encrypted, or files are fully corrupted.", reply_markup=retry_markup)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except:
                    pass
                return

            extrtime = TimeFormatter(round(ext_e_time - ext_s_time) * 1000)
            if extrtime == "":
                extrtime = "1s"

            await async_remove(archive_path)

            chat_id = query.message.chat.id
            
            upload_all_btn = InlineKeyboardButton("📤 Upload All (Albums) 📤", callback_data=f"ext_a|{folder_id}|{chat_id}|NONE|0")
            cancel_btn = InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")
            
            try:
                i_e_buttons = await make_keyboard(
                    paths=paths,
                    user_id=target_uid,
                    chat_id=query.message.chat.id,
                    unziphttp=False,
                )
            except ReplyMarkupTooLong:
                empty_buttons = await make_keyboard_empty(
                    user_id=target_uid, chat_id=query.message.chat.id, unziphttp=False
                )
            except Exception as e:
                pass
            
            simple_markup = InlineKeyboardMarkup([
                [upload_all_btn], 
                [cancel_btn]
            ])

            try:
                # إذا حدث خطأ لكن توجد ملفات (تم الاستخراج جزئياً)
                if has_error:
                    msg_text = f"⚠️ **Partial Extraction in {extrtime}!**\nSome files were missing or corrupted, but we rescued {len(paths)} files.\n\n**Ready to upload:**"
                else:
                    msg_text = f"✅ Extraction completed in {extrtime}!\n\n**Ready to upload:**"
                    
                await query.message.edit(msg_text, reply_markup=simple_markup)
            except FloodWait as e:
                await asyncio.sleep(e.value)

        elif action_type == "cancel":
            await async_rmtree(download_path)
            await del_ongoing_task(target_uid)
            try:
                await query.message.edit("❌ Operation Cancelled. Archive deleted.")
            except:
                pass


    elif query.data == "merge_this":
        user_id = query.from_user.id
        m_id = query.message.id
        start_time = time()
        await add_ongoing_task(user_id, start_time, "merge")
        s_id = await get_merge_task_message_id(user_id)
        
        try:
            merge_msg = await query.message.edit(Messages.PROCESSING_TASK)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            merge_msg = await query.message.edit(Messages.PROCESSING_TASK)
            
        folder_unique_id = f"merge_{user_id}_{int(time())}"
        download_path = f"{Config.DOWNLOAD_LOCATION}/{folder_unique_id}"
        
        if s_id and (m_id - s_id) > 1:
            files_array = list(range(s_id, m_id))
            try:
                messages_array = await unzip_bot.get_messages(user_id, files_array)
            except Exception as e:
                LOGGER.error(Messages.ERROR_GET_MSG.format(e))
                await answer_query(query, Messages.ERROR_TXT.format(e))
                await del_ongoing_task(user_id)
                await del_merge_task(user_id)
                await async_rmtree(download_path)
                return
                
            length = len(messages_array)
            await async_makedirs(download_path)
            rs_time = time()
            newarray = []
            
            try:
                await merge_msg.edit(Messages.PROCESS_MSGS.format(length))
            except FloodWait as e:
                await asyncio.sleep(e.value)
                
            for message in messages_array:
                if message.document is None:
                    pass
                else:
                    if message.from_user.id == user_id:
                        newarray.append(message)
                        
            length = len(newarray)
            if length == 0:
                await answer_query(query, Messages.NO_MERGE_TASK)
                await del_ongoing_task(user_id)
                await del_merge_task(user_id)
                await async_rmtree(download_path)
                return
                
            i = 0
            async_newarray = async_generator(newarray)
            async for message in async_newarray:
                i += 1
                fname = message.document.file_name
                location = f"{download_path}/{fname}"
                s_time = time()
                await message.download(
                    file_name=location,
                    progress=progress_for_pyrogram,
                    progress_args=(
                        Messages.DL_FILES.format(i, length),
                        merge_msg,
                        s_time,
                        unzip_bot,
                    ),
                )
                
            e_time = time()
            dltime = TimeFormatter(round(e_time - rs_time) * 1000)
            if dltime == "":
                dltime = "1 s"
            
            merge_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 Extract Merged Archive", callback_data=f"merged|no_pass|{folder_unique_id}")],
                [InlineKeyboardButton("🔐 Extract with Password", callback_data=f"merged|with_pass|{folder_unique_id}")],
                [InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_unique_id}")]
            ])
            
            try:
                await merge_msg.edit(
                    text=f"✅ **All parts downloaded in {dltime}.**\nWhat do you want to do?",
                    reply_markup=merge_markup
                )
            except FloodWait as e:
                await asyncio.sleep(e.value)
                
            await del_merge_task(user_id)
        else:
            await answer_query(query, Messages.NO_MERGE_TASK)
            await del_ongoing_task(user_id)
            await del_merge_task(user_id)
            await async_rmtree(download_path)

    elif query.data.startswith("merged"):
        user_id = query.from_user.id
        data_parts = query.data.split("|")
        pass_mode = data_parts[1]
        folder_id = data_parts[2] if len(data_parts) > 2 else f"merge_{user_id}_legacy"
        
        download_path = f"{Config.DOWNLOAD_LOCATION}/{folder_id}"
        ext_files_dir = f"{download_path}/extracted"
        await async_makedirs(ext_files_dir)
        
        try:
            files = await get_files(download_path)
            files.sort()
            file = files[0]
        except IndexError:
            await answer_query(query, Messages.NO_MERGE_TASK)
            await del_ongoing_task(user_id)
            await async_rmtree(download_path)
            return
            
        try:
            await query.message.edit(Messages.PROCESSING_TASK)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass
            
        password = None
        if pass_mode == "with_pass":
            try:
                pwd_msg = await unzip_bot.ask(
                    chat_id=query.message.chat.id,
                    text=Messages.PLS_SEND_PASSWORD,
                    timeout=60
                )
                password = pwd_msg.text
                try: 
                    await pwd_msg.delete() 
                except: 
                    pass
            except asyncio.TimeoutError:
                await del_ongoing_task(user_id)
                retry_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Password Again", callback_data=f"merged|with_pass|{folder_id}")],
                    [InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")]
                ])
                try:
                    await query.message.edit("❌ Timeout. Please try again.", reply_markup=retry_markup)
                except FloodWait as e:
                    pass
                return
        
        ext_s_time = time()
        if password:
            extractor = await merge_files(iinput=file, ooutput=ext_files_dir, password=password)
        else:
            extractor = await merge_files(iinput=file, ooutput=ext_files_dir)
        ext_e_time = time()
            
        paths = await get_files(path=ext_files_dir)
        has_error = any(err in extractor for err in ERROR_MSGS)
        
        if not paths:  # تم الفشل بشكل كامل بدون استخراج شيء
            await async_rmtree(ext_files_dir)
            await del_ongoing_task(user_id)
            
            retry_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Password Again", callback_data=f"merged|with_pass|{folder_id}")],
                [InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")]
            ])
            try:
                await query.message.edit(
                    "❌ **Merge/Extraction failed!**\nNo files extracted. Either password was wrong or files are fully corrupted.",
                    reply_markup=retry_markup
                )
            except FloodWait as e:
                pass
            return
            
        # إزالة أجزاء الضغط فقط
        for f in files:
            await async_remove(f)

        extrtime = TimeFormatter(round(ext_e_time - ext_s_time) * 1000)
        if extrtime == "":
            extrtime = "1s"
            
        chat_id = query.message.chat.id
        upload_all_btn = InlineKeyboardButton("📤 Upload All (Albums) 📤", callback_data=f"ext_a|{folder_id}|{chat_id}|NONE|0")
        cancel_btn = InlineKeyboardButton("❌ Cancel & Delete", callback_data=f"cancel_folder|{folder_id}")
        
        simple_markup = InlineKeyboardMarkup([
            [upload_all_btn], 
            [cancel_btn]
        ])

        try:
            # تم دمج الملفات مع وجود خطأ استخراج ولكن يوجد جزء منها صالح
            if has_error:
                msg_text = f"⚠️ **Partial Merge/Extraction in {extrtime}!**\nSome parts were missing/corrupted, but we rescued {len(paths)} files.\n\n**Ready to upload:**"
            else:
                msg_text = f"✅ **Merge & Extraction completed in {extrtime}!**\n\nReady to upload:"

            await query.message.edit(msg_text, reply_markup=simple_markup)
        except Exception as e:
            LOGGER.error(f"Merged output err: {e}")


    elif query.data.startswith("extract_file"):
        user_id = query.from_user.id
        start_time = time()
        await add_ongoing_task(user_id, start_time, "extract")
        
        folder_unique_id = f"{user_id}_{int(time())}"
        download_path = f"{Config.DOWNLOAD_LOCATION}/{folder_unique_id}"
        ext_files_dir = f"{download_path}/extracted"
        
        r_message = query.message.reply_to_message
        splitted_data = query.data.split("|")
        
        try:
            await query.message.edit(Messages.PROCESSING_TASK)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass

        global archive_msg

        try:
            if splitted_data[1] == "url":
                url = r_message.text
                if not re.match(https_url_regex, url):
                    await del_ongoing_task(user_id)
                    try:
                        await query.message.edit(Messages.INVALID_URL)
                    except: pass
                    return
                    
                if re.match(telegram_url_pattern, url):
                    r_message = await unzip_bot.get_messages(
                        chat_id=url.split("/")[-2], message_ids=int(url.split("/")[-1])
                    )
                    splitted_data[1] = "tg_file"
                    
                if splitted_data[1] == "url":
                    s = ClientSession()
                    async with s as session:
                        unzip_head = await session.head(url, allow_redirects=True)
                        f_size = unzip_head.headers.get("content-length")
                        u_file_size = f_size if f_size else "undefined"
                        
                        if u_file_size != "undefined" and not sufficient_disk_space(
                            int(u_file_size)
                        ):
                            await del_ongoing_task(user_id)
                            try:
                                await query.message.edit(Messages.NO_SPACE)
                            except: pass
                            return

                        unzip_resp = await session.get(
                            url, timeout=None, allow_redirects=True
                        )
                        if "application/" not in unzip_resp.headers.get("content-type"):
                            await del_ongoing_task(user_id)
                            try: await query.message.edit(Messages.NOT_AN_ARCHIVE)
                            except: pass
                            return
                            
                        content_disposition = unzip_head.headers.get(
                            "content-disposition"
                        )
                        rfnamebro = ""
                        real_filename = ""
                        
                        if content_disposition:
                            headers = Parser(policy=default).parsestr(
                                f"Content-Disposition: {content_disposition}"
                            )
                            real_filename = headers.get_filename()
                            if real_filename != "":
                                rfnamebro = unquote(real_filename)
                        if rfnamebro == "":
                            rfnamebro = unquote(url.split("/")[-1])
                            
                        if unzip_resp.status == 200:
                            await async_makedirs(download_path)
                            s_time = time()
                            
                            if real_filename:
                                archive = os.path.join(download_path, real_filename)
                                fext = real_filename.split(".")[-1].casefold()
                            else:
                                fname = unquote(os.path.splitext(url)[1])
                                fname = fname.split("?")[0]
                                fext = fname.split(".")[-1].casefold()
                                archive = f"{download_path}/{fname}"
                                
                            if (
                                splitted_data[2] not in ["thumb", "thumbrename"]
                                and fext not in extentions_list["archive"]
                            ):
                                await del_ongoing_task(user_id)
                                try: await query.message.edit(Messages.DEF_NOT_AN_ARCHIVE)
                                except: pass
                                await async_rmtree(download_path)
                                return
                                
                            await answer_query(
                                query, Messages.PROCESSING2, unzip_client=unzip_bot
                            )
                            
                            dled = await download_with_progress(
                                url, archive, query.message, unzip_bot
                            )
                            
                            if isinstance(dled, bool) and not dled:
                                return
                                
                            e_time = time()
                            dltime = TimeFormatter(round(e_time - s_time) * 1000)
                            if dltime == "":
                                dltime = "1s"

                            pass_flag = "P" if splitted_data[2] == "with_pass" else "N"

                            markup = InlineKeyboardMarkup([
                                [InlineKeyboardButton("📦 Extract Now", callback_data=f"archive_action|extract|{user_id}|{folder_unique_id}|{pass_flag}")],
                                [InlineKeyboardButton("❌ Cancel", callback_data=f"archive_action|cancel|{user_id}|{folder_unique_id}")]
                            ])
                            
                            try:
                                await query.message.edit(
                                    text=f"✅ Downloaded in {dltime}.\n**Choose action:**",
                                    reply_markup=markup
                                )
                            except FloodWait as e:
                                await asyncio.sleep(e.value)
                            return

            elif splitted_data[1] == "tg_file":
                if r_message.document is None:
                    await del_ongoing_task(user_id)
                    try: await query.message.edit(Messages.GIVE_ARCHIVE)
                    except: pass
                    return
                    
                fname = r_message.document.file_name
                rfnamebro = fname

                if splitted_data[2] not in ["thumb", "thumbrename"]:
                    fext = fname.split(".")[-1].casefold()
                    if (
                        fnmatch(fext, extentions_list["split"][0])
                        or fext in extentions_list["split"]
                        or bool(re.search(rar_file_pattern, fname))
                    ):
                        try: await query.message.edit(Messages.ITS_SPLITTED)
                        except: pass
                        return
                    if bool(re.search(split_file_pattern, fname)):
                        await del_ongoing_task(user_id)
                        try: await query.message.edit(Messages.SPL_RZ)
                        except: pass
                        return
                    if fext not in extentions_list["archive"]:
                        await del_ongoing_task(user_id)
                        try: await query.message.edit(Messages.DEF_NOT_AN_ARCHIVE)
                        except: pass
                        return
                        
                await async_makedirs(download_path)
                s_time = time()
                location = f"{download_path}/{fname}"
                LOGGER.info("location: %s", location)
                
                archive = await r_message.download(
                    file_name=location,
                    progress=progress_for_pyrogram,
                    progress_args=(
                        Messages.TRY_DL,
                        query.message,
                        s_time,
                        unzip_bot,
                    ),
                )
                e_time = time()
                
            else:
                await del_ongoing_task(user_id)
                await answer_query(query, Messages.QUERY_PARSE_ERR, answer_only=True, unzip_client=unzip_bot)
                return

            dltime = TimeFormatter(round(e_time - s_time) * 1000)
            if dltime == "":
                dltime = "1s"
            
            pass_flag = "P" if splitted_data[2] == "with_pass" else "N"
            
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 Extract Now", callback_data=f"archive_action|extract|{user_id}|{folder_unique_id}|{pass_flag}")],
                [InlineKeyboardButton("❌ Cancel / Delete", callback_data=f"archive_action|cancel|{user_id}|{folder_unique_id}")]
            ])
            
            try:
                await query.message.edit(
                    text=f"✅ **Downloaded successfully in {dltime}.**\n\nArchive saved temporarily.\nDo you want to extract it now?",
                    reply_markup=markup
                )
            except FloodWait as e:
                await asyncio.sleep(e.value)

        except Exception as e:
            await del_ongoing_task(user_id)
            LOGGER.error(e)
            try:
                await query.message.edit(f"Error: {e}")
            except:
                pass


    elif query.data.startswith("ext_f"):
        LOGGER.info(query.data)
        user_id = query.from_user.id
        spl_data = query.data.split("|")
        file_path = f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}/extracted"
        
        try:
            urled = spl_data[4] if isinstance(spl_data[4], bool) else False
        except:
            urled = False
            
        if urled:
            paths = spl_data[5].namelist()
        else:
            paths = await get_files(path=file_path)
            
        if not paths and not urled:
            await async_rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
            await del_ongoing_task(user_id)
            try:
                await query.message.edit(
                    text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME
                )
            except:
                pass
            return
            
        LOGGER.info("ext_f paths : " + str(paths))
        try:
            await query.answer(Messages.SENDING_FILE)
            await query.message.edit(text=Messages.UPLOADING_THIS_FILE)
        except:
            pass
            
        sent_files += 1
        if urled:
            file = spl_data[5].open(paths[int(spl_data[3])])
        else:
            file = paths[int(spl_data[3])]
            
        fsize = await get_size(file)
        split = False
        
        if fsize <= Config.TG_MAX_SIZE:
            await send_file(
                unzip_bot=unzip_bot,
                c_id=spl_data[2],
                doc_f=file,
                query=query,
                full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                log_msg=DummyMessage(),
                split=False,
            )
        else:
            split = True
            
        if split:
            fname = file.split("/")[-1]
            try:
                smessage = await unzip_bot.send_message(
                    chat_id=user_id, text=Messages.SPLITTING.format(fname)
                )
            except FloodWait as e:
                await asyncio.sleep(e.value)
                smessage = await unzip_bot.send_message(chat_id=user_id, text=Messages.SPLITTING.format(fname))

            splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}_{int(time())}"
            await async_makedirs(splitteddir)
            ooutput = f"{splitteddir}/{fname}"
            splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
            
            if not splittedfiles:
                await async_rmtree(splitteddir)
                await del_ongoing_task(user_id)
                try: await smessage.edit(Messages.ERR_SPLIT)
                except: pass
                return
                
            try: await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
            except: pass

            async_splittedfiles = async_generator(splittedfiles)
            async for sfile in async_splittedfiles:
                sent_files += 1
                await send_file(
                    unzip_bot=unzip_bot,
                    c_id=user_id,
                    doc_f=sfile,
                    query=query,
                    full_path=splitteddir,
                    log_msg=DummyMessage(),
                    split=True,
                )
            
            await async_rmtree(splitteddir)
            await async_remove(file)
            try:
                await smessage.delete()
            except:
                pass

        try:
            await query.message.edit(Messages.REFRESHING)
        except:
            pass
        
        if urled:
            rpaths = paths.remove(paths[int(spl_data[3])])
        else:
            rpaths = await get_files(path=file_path)
            
        if not rpaths:
            await async_rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
            await del_ongoing_task(user_id)
            try:
                await query.message.edit(
                    text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME
                )
            except:
                pass
            return
            
        if urled:
            try:
                i_e_buttons = await make_keyboard(
                    paths=rpaths,
                    user_id=query.from_user.id,
                    chat_id=query.message.chat.id,
                    unziphttp=True,
                    rzfile=spl_data[5],
                )
                await query.message.edit(
                    Messages.SELECT_FILES, reply_markup=i_e_buttons
                )
            except ReplyMarkupTooLong:
                empty_buttons = await make_keyboard_empty(
                    user_id=user_id,
                    chat_id=query.message.chat.id,
                    unziphttp=True,
                    rzfile=spl_data[5],
                )
                await query.message.edit(
                    Messages.UNABLE_GATHER_FILES,
                    reply_markup=empty_buttons,
                )
        else:
            try:
                i_e_buttons = await make_keyboard(
                    paths=rpaths,
                    user_id=query.from_user.id,
                    chat_id=query.message.chat.id,
                    unziphttp=False,
                )
                await query.message.edit(
                    Messages.SELECT_FILES, reply_markup=i_e_buttons
                )
            except ReplyMarkupTooLong:
                empty_buttons = await make_keyboard_empty(
                    user_id=user_id, chat_id=query.message.chat.id, unziphttp=False
                )
                await query.message.edit(
                    Messages.UNABLE_GATHER_FILES,
                    reply_markup=empty_buttons,
                )
                
        await update_uploaded(user_id, upload_count=sent_files)

    elif query.data.startswith("ext_a"):
        LOGGER.info(query.data)
        user_id = query.from_user.id
        spl_data = query.data.split("|")
        folder_id = spl_data[1]
        c_id = spl_data[2]
        file_path = f"{Config.DOWNLOAD_LOCATION}/{folder_id}/extracted"
        
        try:
            urled = spl_data[4] if isinstance(spl_data[4], bool) else False
        except Exception:
            urled = False

        if urled:
            paths = spl_data[4].namelist()
        else:
            paths = await get_files(path=file_path)

        if not paths and not urled:
            await async_rmtree(f"{Config.DOWNLOAD_LOCATION}/{folder_id}")
            await del_ongoing_task(user_id)
            try:
                await query.message.edit(text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME)
            except: pass
            return

        if urled:
            try: await query.message.edit(Messages.SEND_ALL_FILES)
            except: pass
            async_paths = async_generator(paths)
            async for file in async_paths:
                sent_files += 1
                file = spl_data[4].open(file)
                fsize = Config.TG_MAX_SIZE + 1
                fname = str(file).split("/")[-1]
                smessage = await unzip_bot.send_message(chat_id=user_id, text=Messages.SPLITTING.format(fname))
                splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}_{int(time())}"
                await async_makedirs(splitteddir)
                splittedfiles = await split_files(file, f"{splitteddir}/{fname}", Config.TG_MAX_SIZE)
                if not splittedfiles:
                    await async_rmtree(splitteddir)
                    continue
                async for s_file in async_generator(splittedfiles):
                    await send_file(unzip_bot=unzip_bot, c_id=user_id, doc_f=s_file, query=query, full_path=splitteddir, log_msg=DummyMessage(), split=True)
                await async_rmtree(splitteddir)
            
            await del_ongoing_task(user_id)
            try:
                await query.message.edit(text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME)
            except: pass
            return

        stats_msg = "⌛ *Initializing process...*"
        try:
            await query.message.edit(text=stats_msg)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except:
            pass
        
        await process_album_pagination(query, unzip_bot, folder_id, c_id, "video", 0)


    elif query.data.startswith("nxtalb"):
        data = query.data.split("|")
        f_id = data[1]
        c_id = data[2]
        tp = data[3]
        idx = int(data[4])
        
        await process_album_pagination(query, unzip_bot, f_id, c_id, tp, idx)


    elif query.data.startswith("jumpalb"):
        data = query.data.split("|")
        f_id = data[1]
        c_id = data[2]
        tp = data[3]
        max_chunks = int(data[4])
        user_id = query.from_user.id

        try:
            await query.message.delete()
        except:
            pass

        try:
            ask_msg = await unzip_bot.ask(
                chat_id=user_id,
                text=f"🔢 **Reply with the album number you want to jump to!**\n\n(Choose a number between **1** and **{max_chunks}**)\n*Send '0' to cancel*",
                timeout=120
            )
            
            if not ask_msg.text or not ask_msg.text.isdigit():
                await unzip_bot.send_message(chat_id=user_id, text="❌ Invalid Input. Please click the Jump button again.")
                return
            
            input_num = int(ask_msg.text)
            
            if input_num == 0:
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Resume Sequence", callback_data=f"nxtalb|{f_id}|{c_id}|{tp}|0")]])
                await unzip_bot.send_message(user_id, "🚫 Jump cancelled.", reply_markup=btn)
                return
                
            elif input_num < 1 or input_num > max_chunks:
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Sequence", callback_data=f"nxtalb|{f_id}|{c_id}|{tp}|0")]])
                await unzip_bot.send_message(user_id, f"❌ Out of range! Must be between 1 and {max_chunks}.", reply_markup=btn)
                return
            
            target_idx = input_num - 1
            await process_album_pagination(query, unzip_bot, f_id, c_id, tp, target_idx)
            
        except asyncio.TimeoutError:
            await unzip_bot.send_message(chat_id=user_id, text="⏱ Timeout! You took too long to send the number. Click the Jump button again if you still want to skip.")

    # ================================================
    # Cancellation Logics (Safe cleanup with async)
    # ================================================
    elif query.data.startswith("cancel_folder"):
        data = query.data.split("|")
        folder_id = data[1]
        user_id = query.from_user.id
        
        await async_rmtree(f"{Config.DOWNLOAD_LOCATION}/{folder_id}")
        await del_ongoing_task(user_id)
        
        try:
            await query.message.edit("❌ **Process Cancelled & Files deleted.**")
        except FloodWait as e:
            pass


    elif query.data == "cancel_dis":
        uid = query.from_user.id
        await del_ongoing_task(uid)
        await del_merge_task(uid)
        try:
            await query.message.edit(Messages.CANCELLED_TXT.format(Messages.PROCESS_CANCELLED))
            
            await async_rmtree(f"{Config.DOWNLOAD_LOCATION}/{uid}")
                
            for folder in glob.glob(f"{Config.DOWNLOAD_LOCATION}/{uid}_*"):
                await async_rmtree(folder)
                    
            await update_uploaded(user_id=uid, upload_count=sent_files)
        except:
            await unzip_bot.send_message(
                chat_id=uid, text=Messages.CANCELLED_TXT.format(Messages.PROCESS_CANCELLED)
            )
            return

    elif query.data == "nobully":
        try:
            await query.message.edit(Messages.CANCELLED)
        except:
            pass
