# Copyright (c) 2022 - 2024 EDM115
import asyncio
import concurrent.futures
import os
import re
import shutil
import unzip_http
from aiofiles import open as openfile
from aiohttp import ClientSession, InvalidURL
from email.parser import Parser
from email.policy import default
from fnmatch import fnmatch
from pyrogram import Client
from pyrogram.errors import ReplyMarkupTooLong
from pyrogram.types import CallbackQuery, InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup
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

# --- الوظائف الأصلية ---
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
# --- نهاية الوظائف الأصلية ---

# --- التعديلات الجديدة ---
def chunk_list(lst, n):
    """تقسيم القائمة إلى مجموعات بحجم n"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def send_album_batch(unzip_bot, chat_id, batch, log_msg=None):
    """إرسال مجموعة من الملفات كأحد ألبومات"""
    media_group = []
    for file_path in batch:
        if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
            media_group.append(InputMediaPhoto(media=file_path))
        elif file_path.lower().endswith(('.mp4', '.mov', '.mkv', '.avi', '.gif')):
            media_group.append(InputMediaVideo(media=file_path))
        # لا تُضف ملفات أخرى إلى الألبوم
    
    if len(media_group) >= 1:  # الألبوم يمكن أن يحتوي على ملف واحد فقط
        # أول عنصر يحمل التسمية التوضيحية
        if log_msg:
            media_group[0].caption = f"📦 Extracted by @UnzipBot\n📊 Files: {len(media_group)}"
        else:
            media_group[0].caption = f"📦 Extracted by @UnzipBot\n📊 Files: {len(media_group)}"
        try:
            await unzip_bot.send_media_group(chat_id=chat_id, media=media_group)
            if log_msg:
                await log_msg.reply(f"✅ Sent {len(media_group)} files as album.")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to send album: {e}")
            # إذا فشل الألبوم، أعد إرسال الملفات فرديًا
            for media_item in media_group:
                try:
                    if isinstance(media_item, InputMediaPhoto):
                        await unzip_bot.send_photo(chat_id, media_item.media)
                    elif isinstance(media_item, InputMediaVideo):
                        await unzip_bot.send_video(chat_id, media_item.media)
                except Exception as e2:
                    LOGGER.error(f"Failed to send individual file: {e2}")
            return False
    return False
# --- نهاية التعديلات الجديدة ---

# Callbacks
@unzipperbot.on_callback_query()
async def unzipper_cb(unzip_bot: Client, query: CallbackQuery):
    uid = query.from_user.id
    if uid != Config.BOT_OWNER:  # skipcq: PTC-W0048
        if await count_ongoing_tasks() >= Config.MAX_CONCURRENT_TASKS:
            ogtasks = await get_ongoing_tasks()
            if not any(ogtask.get("user_id") == uid for ogtask in ogtasks):
                await unzip_bot.send_message(
                    chat_id=uid,
                    text=Messages.MAX_TASKS.format(Config.MAX_CONCURRENT_TASKS),
                )
                return
    if uid != Config.BOT_OWNER and await get_maintenance():
        await answer_query(query, Messages.MAINTENANCE_ON)
        return

    sent_files = 0
    global log_msg

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
    elif query.data == "merge_this":
        user_id = query.from_user.id
        m_id = query.message.id
        start_time = time()
        await add_ongoing_task(user_id, start_time, "merge")
        s_id = await get_merge_task_message_id(user_id)
        merge_msg = await query.message.edit(Messages.PROCESSING_TASK)
        download_path = f"{Config.DOWNLOAD_LOCATION}/{user_id}/merge"
        if s_id and (m_id - s_id) > 1:
            files_array = list(range(s_id, m_id))
            try:
                messages_array = await unzip_bot.get_messages(user_id, files_array)
            except Exception as e:
                LOGGER.error(Messages.ERROR_GET_MSG.format(e))
                await answer_query(query, Messages.ERROR_TXT.format(e))
                await del_ongoing_task(user_id)
                await del_merge_task(user_id)
                try:
                    shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
                except:
                    pass
                return
            length = len(messages_array)
            os.makedirs(download_path, exist_ok=True)
            rs_time = time()
            newarray = []
            await merge_msg.edit(Messages.PROCESS_MSGS.format(length))
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
                try:
                    shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
                except:
                    pass
                return
            i = 0
            async_newarray = async_generator(newarray)
            async for message in async_newarray:
                i += 1
                fname = message.document.file_name
                await message.forward(chat_id=Config.LOGS_CHANNEL)
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
                await merge_msg.edit(Messages.AFTER_OK_MERGE_DL_TXT.format(i, dltime))
            await merge_msg.edit(
                text=Messages.CHOOSE_EXT_MODE_MERGE,
                reply_markup=Buttons.CHOOSE_E_F_M__BTNS,
            )
            await del_merge_task(user_id)
        else:
            await answer_query(query, Messages.NO_MERGE_TASK)
            await del_ongoing_task(user_id)
            await del_merge_task(user_id)
            try:
                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
            except:
                pass
    elif query.data.startswith("merged"):
        user_id = query.from_user.id
        download_path = f"{Config.DOWNLOAD_LOCATION}/{user_id}/merge"
        ext_files_dir = f"{Config.DOWNLOAD_LOCATION}/{user_id}/extracted"
        os.makedirs(ext_files_dir, exist_ok=True)
        try:
            files = await get_files(download_path)
            file = files[0]
        except IndexError:
            await answer_query(query, Messages.NO_MERGE_TASK)
            await del_ongoing_task(user_id)
            await del_merge_task(user_id)
            try:
                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
            except:
                pass
            return
        splitted_data = query.data.split("|")
        log_msg = await unzip_bot.send_message(
            chat_id=Config.LOGS_CHANNEL,
            text=Messages.PROCESS_MERGE.format(
                user_id, ".".join(file.split("/")[-1].split(".")[:-1])
            ),
        )
        try:
            await query.message.edit(Messages.PROCESSING_TASK)
        except:
            pass
        if splitted_data[1] == "with_pass":
            password = await unzip_bot.ask(
                chat_id=query.message.chat.id,
                text=Messages.PLS_SEND_PASSWORD,
            )
            ext_s_time = time()
            extractor = await merge_files(
                iinput=file,
                ooutput=ext_files_dir,
                password=password.text,
            )
            ext_e_time = time()
        else:
            # Can't test the archive apparently
            ext_s_time = time()
            extractor = await merge_files(iinput=file, ooutput=ext_files_dir)
            ext_e_time = time()
        # Checks if there is an error happened while extracting the archive
        if any(err in extractor for err in ERROR_MSGS):
            try:
                await query.message.edit(Messages.EXT_FAILED_TXT)
                shutil.rmtree(ext_files_dir)
                shutil.rmtree(download_path)
                await del_ongoing_task(user_id)
            except:
                try:
                    await query.message.delete()
                except:
                    pass
            await unzip_bot.send_message(
                chat_id=query.message.chat.id, text=Messages.EXT_FAILED_TXT
            )
            shutil.rmtree(ext_files_dir)
            await del_ongoing_task(user_id)
            return
        # Check if user was dumb 😐
        paths = await get_files(path=ext_files_dir)
        if not paths:
            await unzip_bot.send_message(
                chat_id=query.message.chat.id,
                text=Messages.PASSWORD_PROTECTED,
            )
            await answer_query(query, Messages.EXT_FAILED_TXT, unzip_client=unzip_bot)
            shutil.rmtree(ext_files_dir)
            shutil.rmtree(download_path)
            await del_ongoing_task(user_id)
            return
        try:
            shutil.rmtree(download_path)
        except:
            pass
        # Upload extracted files
        extrtime = TimeFormatter(round(ext_e_time - ext_s_time) * 1000)
        if extrtime == "":
            extrtime = "1s"
        await answer_query(
            query, Messages.EXT_OK_TXT.format(extrtime), unzip_client=unzip_bot
        )
        try:
            i_e_buttons = await make_keyboard(
                paths=paths,
                user_id=user_id,
                chat_id=query.message.chat.id,
                unziphttp=False,
            )
            try:
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
        except:
            try:
                await query.message.delete()
                i_e_buttons = await make_keyboard(
                    paths=paths,
                    user_id=user_id,
                    chat_id=query.message.chat.id,
                    unziphttp=False,
                )
                await unzip_bot.send_message(
                    chat_id=query.message.chat.id,
                    text=Messages.SELECT_FILES,
                    reply_markup=i_e_buttons,
                )
            except:
                try:
                    await query.message.delete()
                    empty_buttons = await make_keyboard_empty(
                        user_id=user_id, chat_id=query.message.chat.id, unziphttp=False
                    )
                    await unzip_bot.send_message(
                        chat_id=query.message.chat.id,
                        text=Messages.UNABLE_GATHER_FILES,
                        reply_markup=empty_buttons,
                    )
                except:
                    await answer_query(
                        query, Messages.EXT_FAILED_TXT, unzip_client=unzip_bot
                    )
                    shutil.rmtree(ext_files_dir)
                    LOGGER.error(Messages.FATAL_ERROR)
                    await del_ongoing_task(user_id)
                    return
    elif query.data.startswith("extract_file"):
        user_id = query.from_user.id
        start_time = time()
        await add_ongoing_task(user_id, start_time, "extract")
        download_path = f"{Config.DOWNLOAD_LOCATION}/{user_id}"
        ext_files_dir = f"{Config.DOWNLOAD_LOCATION}/{user_id}/extracted"
        r_message = query.message.reply_to_message
        splitted_data = query.data.split("|")
        try:
            await query.message.edit(Messages.PROCESSING_TASK)
        except:
            pass
        log_msg = await unzip_bot.send_message(
            chat_id=Config.LOGS_CHANNEL, text=Messages.USER_QUERY.format(user_id)
        )
        global archive_msg
        try:
            if splitted_data[1] == "url":
                url = r_message.text
                # Double check
                if not re.match(https_url_regex, url):
                    await del_ongoing_task(user_id)
                    await query.message.edit(Messages.INVALID_URL)
                    return
                if re.match(telegram_url_pattern, url):
                    r_message = await unzip_bot.get_messages(
                        chat_id=url.split("/")[-2], message_ids=int(url.split("/")[-1])
                    )
                    splitted_data[1] = "tg_file"
            if splitted_data[1] == "url":
                s = ClientSession()
                async with s as session:
                    # Get the file size
                    unzip_head = await session.head(url, allow_redirects=True)
                    f_size = unzip_head.headers.get("content-length")
                    u_file_size = f_size if f_size else "undefined"
                    if u_file_size != "undefined" and not sufficient_disk_space(
                        int(u_file_size)
                    ):
                        await del_ongoing_task(user_id)
                        await query.message.edit(Messages.NO_SPACE)
                        return
                    await log_msg.edit(
                        Messages.LOG_TXT.format(user_id, url, u_file_size)
                    )
                    archive_msg = log_msg
                    unzip_resp = await session.get(
                        url, timeout=None, allow_redirects=True
                    )
                    if "application/" not in unzip_resp.headers.get("content-type"):
                        await del_ongoing_task(user_id)
                        await query.message.edit(Messages.NOT_AN_ARCHIVE)
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
                        os.makedirs(download_path, exist_ok=True)
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
                            await query.message.edit(Messages.DEF_NOT_AN_ARCHIVE)
                            try:
                                shutil.rmtree(
                                    f"{Config.DOWNLOAD_LOCATION}/{user_id}"
                                )
                            except:
                                pass
                            return
                        await answer_query(
                            query, Messages.PROCESSING2, unzip_client=unzip_bot
                        )
                        if (
                            fext == "zip"
                            and "accept-ranges" in unzip_resp.headers
                            and "content-length" in unzip_resp.headers
                        ):
                            try:
                                loop = asyncio.get_event_loop()
                                with concurrent.futures.ThreadPoolExecutor() as pool:
                                    rzf, paths = await loop.run_in_executor(
                                        pool, get_zip_http, url
                                    )
                                try:
                                    i_e_buttons = await make_keyboard(
                                        paths=paths,
                                        user_id=user_id,
                                        chat_id=query.message.chat.id,
                                        unziphttp=True,
                                        rzfile=rzf,
                                    )
                                    try:
                                        await query.message.edit(
                                            Messages.SELECT_FILES,
                                            reply_markup=i_e_buttons,
                                        )
                                    except ReplyMarkupTooLong:
                                        empty_buttons = await make_keyboard_empty(
                                            user_id=user_id,
                                            chat_id=query.message.chat.id,
                                            unziphttp=True,
                                            rzfile=rzf,
                                        )
                                        await query.message.edit(
                                            Messages.UNABLE_GATHER_FILES,
                                            reply_markup=empty_buttons,
                                        )
                                except:
                                    try:
                                        await query.message.delete()
                                        i_e_buttons = await make_keyboard(
                                            paths=paths,
                                            user_id=user_id,
                                            chat_id=query.message.chat.id,
                                            unziphttp=True,
                                            rzfile=rzf,
                                        )
                                        await unzip_bot.send_message(
                                            chat_id=query.message.chat.id,
                                            text=Messages.SELECT_FILES,
                                            reply_markup=i_e_buttons,
                                        )
                                    except:
                                        try:
                                            await query.message.delete()
                                            empty_buttons = (
                                                await make_keyboard_empty(
                                                    user_id=user_id,
                                                    chat_id=query.message.chat.id,
                                                    unziphttp=True,
                                                    rzfile=rzf,
                                                )
                                            )
                                            await unzip_bot.send_message(
                                                chat_id=query.message.chat.id,
                                                text=Messages.UNABLE_GATHER_FILES,
                                                reply_markup=empty_buttons,
                                            )
                                        except:
                                            pass
                            except Exception as e:
                                LOGGER.error(Messages.UNZIP_HTTP.format(url, e))
                            try:
                                dled = await download_with_progress(
                                    url, archive, query.message, unzip_bot
                                )
                            except Exception as e:
                                dled = False
                                LOGGER.error(Messages.ERR_DL.format(e))
                            if isinstance(dled, bool) and not dled:
                                return
                            e_time = time()
                            await send_url_logs(
                                unzip_bot=unzip_bot,
                                c_id=Config.LOGS_CHANNEL,
                                doc_f=archive,
                                source=url,
                                message=query.message,
                            )
                        else:
                            await del_ongoing_task(user_id)
                            await query.message.edit(Messages.CANT_DL_URL)
                            try:
                                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
                            except:
                                pass
                            return
            elif splitted_data[1] == "tg_file":
                if r_message.document is None:
                    await del_ongoing_task(user_id)
                    await query.message.edit(Messages.GIVE_ARCHIVE)
                    return
                fname = r_message.document.file_name
                rfnamebro = fname
                archive_msg = await r_message.forward(chat_id=Config.LOGS_CHANNEL)
                await log_msg.edit(
                    Messages.LOG_TXT.format(
                        user_id, fname, humanbytes(r_message.document.file_size)
                    )
                )
                if splitted_data[2] not in ["thumb", "thumbrename"]:
                    fext = fname.split(".")[-1].casefold()
                    if (
                        fnmatch(fext, extentions_list["split"][0])
                        or fext in extentions_list["split"]
                        or bool(re.search(rar_file_pattern, fname))
                    ):
                        await query.message.edit(Messages.ITS_SPLITTED)
                        return
                    if bool(re.search(split_file_pattern, fname)):
                        await del_ongoing_task(user_id)
                        await query.message.edit(Messages.SPL_RZ)
                        return
                    if fext not in extentions_list["archive"]:
                        await del_ongoing_task(user_id)
                        await query.message.edit(Messages.DEF_NOT_AN_ARCHIVE)
                        return
                os.makedirs(download_path, exist_ok=True)
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
                await answer_query(
                    query,
                    Messages.QUERY_PARSE_ERR,
                    answer_only=True,
                    unzip_client=unzip_bot,
                )
                return
            if splitted_data[2].startswith("thumb"):
                await query.message.edit(Messages.PROCESSING2)
                archive_name = location.split("/")[-1]
                if "rename" in splitted_data[2]:
                    newname = await unzip_bot.ask(
                        chat_id=user_id,
                        text=Messages.GIVE_NEW_NAME.format(rfnamebro),
                    )
                    renamed = location.replace(archive_name, newname.text)
                else:
                    renamed = location.replace(archive_name, rfnamebro)
                try:
                    shutil.move(location, renamed)
                except OSError as e:
                    await del_ongoing_task(user_id)
                    LOGGER.error(e)
                    return
                newfname = renamed.split("/")[-1]
                fsize = await get_size(renamed)
                if fsize <= Config.TG_MAX_SIZE:
                    await send_file(
                        unzip_bot=unzip_bot,
                        c_id=user_id,
                        doc_f=renamed,
                        query=query,
                        full_path=renamed,
                        log_msg=log_msg,
                        split=False,
                    )
                    await query.message.delete()
                    await del_ongoing_task(user_id)
                    return shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{user_id}")
                await query.message.edit(Messages.SPLITTING.format(newfname))
                splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                os.makedirs(splitteddir, exist_ok=True)
                ooutput = f"{splitteddir}/{newfname}"
                splittedfiles = await split_files(renamed, ooutput, Config.TG_MAX_SIZE)
                if not splittedfiles:
                    try:
                        shutil.rmtree(splitteddir)
                    except:
                        pass
                    await del_ongoing_task(user_id)
                    await query.message.edit(Messages.ERR_SPLIT)
                    return
                await query.message.edit(Messages.SEND_ALL_PARTS.format(newfname))
                async_splittedfiles = async_generator(splittedfiles)
                async for file in async_splittedfiles:
                    sent_files += 1
                    await send_file(
                        unzip_bot=unzip_bot,
                        c_id=user_id,
                        doc_f=file,
                        query=query,
                        full_path=splitteddir,
                        log_msg=log_msg,
                        split=True,
                    )
                try:
                    shutil.rmtree(splitteddir)
                    shutil.rmtree(renamed.replace(newfname, ""))
                except:
                    pass
                await del_ongoing_task(user_id)
                try:
                    await unzip_bot.send_message(
                        chat_id=user_id,
                        text=Messages.UPLOADED,
                        reply_markup=Buttons.RATE_ME,
                    )
                    await query.message.edit(
                        text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                    )
                except:
                    pass
                return
            dltime = TimeFormatter(round(e_time - s_time) * 1000)
            if dltime == "":
                dltime = "1s"
            await answer_query(
                query, Messages.AFTER_OK_DL_TXT.format(dltime), unzip_client=unzip_bot
            )
            # Attempt to fetch password protected archives
            if splitted_data[2] == "with_pass":
                password = await unzip_bot.ask(
                    chat_id=query.message.chat.id, text=Messages.PLS_SEND_PASSWORD
                )
                ext_s_time = time()
                extractor = await extr_files(
                    path=ext_files_dir,
                    archive_path=archive,
                    password=password.text,
                )
                ext_e_time = time()
                await archive_msg.reply(Messages.PASS_TXT.format(password.text))
            else:
                ext_s_time = time()
                tested = await _test_with_7z_helper(archive)
                ext_t_time = time()
                testtime = TimeFormatter(round(ext_t_time - ext_s_time) * 1000)
                if testtime == "":
                    testtime = "1s"
                await answer_query(
                    query,
                    Messages.AFTER_OK_TEST_TXT.format(testtime),
                    unzip_client=unzip_bot,
                )
                if tested:
                    extractor = await extr_files(
                        path=ext_files_dir, archive_path=archive
                    )
                    ext_e_time = time()
                else:
                    LOGGER.info("Error on test")
                    extractor = "Error"
                    ext_e_time = time()
            # Checks if there is an error happened while extracting the archive
            if any(err in extractor for err in ERROR_MSGS):
                try:
                    await query.message.edit(Messages.EXT_FAILED_TXT)
                    shutil.rmtree(ext_files_dir)
                    await del_ongoing_task(user_id)
                    await log_msg.reply(Messages.EXT_FAILED_TXT)
                    return
                except:
                    try:
                        await query.message.delete()
                    except:
                        pass
                await unzip_bot.send_message(
                    chat_id=query.message.chat.id, text=Messages.EXT_FAILED_TXT
                )
                shutil.rmtree(ext_files_dir)
                await del_ongoing_task(user_id)
                await archive_msg.reply(Messages.EXT_FAILED_TXT)
                return
            # Check if user was dumb 😐
            paths = await get_files(path=ext_files_dir)
            if not paths:
                await archive_msg.reply(Messages.PASSWORD_PROTECTED)
                await unzip_bot.send_message(
                    chat_id=query.message.chat.id,
                    text=Messages.PASSWORD_PROTECTED,
                )
                await answer_query(
                    query, Messages.EXT_FAILED_TXT, unzip_client=unzip_bot
                )
                shutil.rmtree(ext_files_dir)
                await del_ongoing_task(user_id)
                return
            # Upload extracted files
            extrtime = TimeFormatter(round(ext_e_time - ext_s_time) * 1000)
            if extrtime == "":
                extrtime = "1s"
            await answer_query(
                query, Messages.EXT_OK_TXT.format(extrtime), unzip_client=unzip_bot
            )
            try:
                i_e_buttons = await make_keyboard(
                    paths=paths,
                    user_id=user_id,
                    chat_id=query.message.chat.id,
                    unziphttp=False,
                )
                try:
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
            except:
                try:
                    await query.message.delete()
                    i_e_buttons = await make_keyboard(
                        paths=paths,
                        user_id=user_id,
                        chat_id=query.message.chat.id,
                        unziphttp=False,
                    )
                    await unzip_bot.send_message(
                        chat_id=query.message.chat.id,
                        text=Messages.SELECT_FILES,
                        reply_markup=i_e_buttons,
                    )
                except:
                    try:
                        await query.message.delete()
                        empty_buttons = await make_keyboard_empty(
                            user_id=user_id,
                            chat_id=query.message.chat.id,
                            unziphttp=False,
                        )
                        await unzip_bot.send_message(
                            chat_id=query.message.chat.id,
                            text=Messages.UNABLE_GATHER_FILES,
                            reply_markup=empty_buttons,
                        )
                    except:
                        await answer_query(
                            query, Messages.EXT_FAILED_TXT, unzip_client=unzip_bot
                        )
                        await archive_msg.reply(Messages.EXT_FAILED_TXT)
                        shutil.rmtree(ext_files_dir)
                        LOGGER.error(Messages.FATAL_ERROR)
                        await del_ongoing_task(user_id)
                        return
        except Exception as e:
            await del_ongoing_task(user_id)
            try:
                try:
                    await query.message.edit(Messages.ERROR_TXT.format(e))
                except:
                    await unzip_bot.send_message(
                        chat_id=query.message.chat.id, text=Messages.ERROR_TXT.format(e)
                    )
                await archive_msg.reply(Messages.ERROR_TXT.format(e))
                shutil.rmtree(ext_files_dir)
                try:
                    await ClientSession().close()
                except:
                    pass
                LOGGER.error(e)
            except Exception as err:
                LOGGER.error(err)
                await archive_msg.reply(err)
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
            if os.path.isdir(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}"):
                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
            await del_ongoing_task(user_id)
            await query.message.edit(
                text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME
            )
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
                log_msg=log_msg,
                split=False,
            )
        else:
            split = True
        if split:
            fname = file.split("/")[-1]
            smessage = await unzip_bot.send_message(
                chat_id=user_id, text=Messages.SPLITTING.format(fname)
            )
            splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
            os.makedirs(splitteddir, exist_ok=True)
            ooutput = f"{splitteddir}/{fname}"
            splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
            LOGGER.info(splittedfiles)
            if not splittedfiles:
                try:
                    shutil.rmtree(splitteddir)
                except:
                    pass
                await del_ongoing_task(user_id)
                await smessage.edit(Messages.ERR_SPLIT)
                return
            await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
            async_splittedfiles = async_generator(splittedfiles)
            async for file in async_splittedfiles:
                sent_files += 1
                await send_file(
                    unzip_bot=unzip_bot,
                    c_id=user_id,
                    doc_f=file,
                    query=query,
                    full_path=splitteddir,
                    log_msg=log_msg,
                    split=True,
                )
            try:
                shutil.rmtree(splitteddir)
                os.remove(file)
            except:
                pass
        try:
            await smessage.delete()
        except:
            pass
        await query.message.edit(Messages.REFRESHING)
        if urled:
            rpaths = paths.remove(paths[int(spl_data[3])])
        else:
            rpaths = await get_files(path=file_path)
        if not rpaths:
            try:
                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
            except:
                pass
            await del_ongoing_task(user_id)
            await query.message.edit(
                text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME
            )
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
        file_path = f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}/extracted"
        try:
            urled = spl_data[4] if isinstance(spl_data[3], bool) else False
        except:
            urled = False
        if urled:
            paths = spl_data[4].namelist()
        else:
            paths = await get_files(path=file_path)
        LOGGER.info("ext_a paths : " + str(paths))
        if not paths and not urled:
            try:
                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
            except:
                pass
            await del_ongoing_task(user_id)
            await query.message.edit(
                text=Messages.NO_FILE_LEFT, reply_markup=Buttons.RATE_ME
            )
            return

        # --- التعديل الجديد ---
        # جمع أنواع الملفات
        image_files = []
        video_files = []
        other_files = []

        async_paths = async_generator(paths)
        async for file in async_paths:
            if not urled: # فقط إذا لم يكن من رابط
                if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    image_files.append(file)
                elif file.lower().endswith(('.mp4', '.mov', '.mkv', '.avi', '.gif')):
                    video_files.append(file)
                else:
                    other_files.append(file)
            else:
                # لا نستخدم الحجم من رابط، فقط نرتب حسب الامتداد
                if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    image_files.append(file)
                elif file.lower().endswith(('.mp4', '.mov', '.mkv', '.avi', '.gif')):
                    video_files.append(file)
                else:
                    other_files.append(file)

        # حساب الإحصائيات
        total_images = len(image_files)
        total_videos = len(video_files)
        total_others = len(other_files)

        # تقسيم إلى ألبومات (10 كحد أقصى لكل)
        video_albums = list(chunk_list(video_files, 10))
        image_albums = list(chunk_list(image_files, 10))

        total_video_albums = len(video_albums)
        total_image_albums = len(image_albums)

        # عرض الإحصائيات
        stats_text = f"""
📊 **Upload Statistics:**

🎬 **Videos:** {total_videos} in {total_video_albums} albums
🖼️ **Images:** {total_images} in {total_image_albums} albums
📄 **Others:** {total_others} files

Starting to send videos first...
        """
        await query.message.edit(stats_text)
        # الانتظار قليلاً قبل البدء
        await asyncio.sleep(2)

        # تعريف sent_files في بداية هذا المقطع (مهم!)
        sent_files = 0

        # دالة لإرسال الألبومات تدريجيًا
        async def send_album_batches(albums_list, media_type="video"):
            nonlocal sent_files  # <-- هذا السطر مهم
            current_index = 0
            total_albums = len(albums_list)
            while current_index < total_albums:
                album_batch = albums_list[current_index]
                success = await send_album_batch(unzip_bot, user_id, album_batch, log_msg)
                if success:
                    sent_files += len(album_batch)
                
                # حساب الاسم الصحيح للنوع
                type_name = "Videos" if media_type == "video" else "Images"
                # إعداد الزر التالي أو إنهاء الإرسال
                if current_index + 1 < total_albums:
                    # زر "Next Album"
                    next_button = InlineKeyboardButton(f"Next {type_name} Album 📀", callback_data=f"next_album|{media_type}|{current_index + 1}|{query.data}")
                    cancel_button = InlineKeyboardButton("Cancel ❌", callback_data="cancel_dis")
                    keyboard = InlineKeyboardMarkup([[next_button, cancel_button]])
                    await unzip_bot.send_message(
                        chat_id=user_id,
                        text=f"Sent {type_name} Album #{current_index + 1}. Press Next to continue.",
                        reply_markup=keyboard
                    )
                    # الانتظار لرد المستخدم
                    # نستخدم break هنا، ونعتمد على next_album callback
                    break # نخرج من الحلقة الحالية وننتظر الضغطة التالية
                else:
                    # انتهت ألبومات هذا النوع
                    if media_type == "video":
                        # ننتقل إلى صور الألبومات
                        if total_image_albums > 0:
                            await unzip_bot.send_message(
                                chat_id=user_id,
                                text="Now starting to send image albums...",
                            )
                            await send_album_batches(image_albums, "image")
                        else:
                            # لا توجد صور، نرسل الملفات الأخرى
                            for file in other_files:
                                sent_files += 1
                                fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                                split = False
                                if fsize <= Config.TG_MAX_SIZE:
                                    await send_file(
                                        unzip_bot=unzip_bot,
                                        c_id=spl_data[2],
                                        doc_f=file,
                                        query=query,
                                        full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                                        log_msg=log_msg,
                                        split=False,
                                    )
                                else:
                                    split = True
                                if split:
                                    fname = file.split("/")[-1]
                                    smessage = await unzip_bot.send_message(
                                        chat_id=user_id, text=Messages.SPLITTING.format(fname)
                                    )
                                    splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                                    os.makedirs(splitteddir, exist_ok=True)
                                    ooutput = f"{splitteddir}/{fname}"
                                    splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                                    LOGGER.info(splittedfiles)
                                    if not splittedfiles:
                                        try:
                                            shutil.rmtree(splitteddir)
                                        except:
                                            pass
                                        await del_ongoing_task(user_id)
                                        await smessage.edit(Messages.ERR_SPLIT)
                                        return
                                    await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                                    async_splittedfiles = async_generator(splittedfiles)
                                    async for s_file in async_splittedfiles:
                                        sent_files += 1
                                        await send_file(
                                            unzip_bot=unzip_bot,
                                            c_id=user_id,
                                            doc_f=s_file,
                                            query=query,
                                            full_path=splitteddir,
                                            log_msg=log_msg,
                                            split=True,
                                        )
                            try:
                                await unzip_bot.send_message(
                                    chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                                )
                                await query.message.edit(
                                    text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                                )
                            except:
                                pass
                            await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                            await update_uploaded(user_id, upload_count=sent_files)
                            await del_ongoing_task(user_id)
                            try:
                                shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                            except Exception as e:
                                await query.message.edit(Messages.ERROR_TXT.format(e))
                                await archive_msg.reply(Messages.ERROR_TXT.format(e))
                    else: # media_type == "image"
                        # انتهت الصور، نرسل الملفات الأخرى
                        for file in other_files:
                            sent_files += 1
                            fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                            split = False
                            if fsize <= Config.TG_MAX_SIZE:
                                await send_file(
                                    unzip_bot=unzip_bot,
                                    c_id=spl_data[2],
                                    doc_f=file,
                                    query=query,
                                    full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                                    log_msg=log_msg,
                                    split=False,
                                )
                            else:
                                split = True
                            if split:
                                fname = file.split("/")[-1]
                                smessage = await unzip_bot.send_message(
                                    chat_id=user_id, text=Messages.SPLITTING.format(fname)
                                )
                                splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                                os.makedirs(splitteddir, exist_ok=True)
                                ooutput = f"{splitteddir}/{fname}"
                                splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                                LOGGER.info(splittedfiles)
                                if not splittedfiles:
                                    try:
                                        shutil.rmtree(splitteddir)
                                    except:
                                        pass
                                    await del_ongoing_task(user_id)
                                    await smessage.edit(Messages.ERR_SPLIT)
                                    return
                                await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                                async_splittedfiles = async_generator(splittedfiles)
                                async for s_file in async_splittedfiles:
                                    sent_files += 1
                                    await send_file(
                                        unzip_bot=unzip_bot,
                                        c_id=user_id,
                                        doc_f=s_file,
                                        query=query,
                                        full_path=splitteddir,
                                        log_msg=log_msg,
                                        split=True,
                                    )
                        try:
                            await unzip_bot.send_message(
                                chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                            )
                            await query.message.edit(
                                text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                            )
                        except:
                            pass
                        await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                        await update_uploaded(user_id, upload_count=sent_files)
                        await del_ongoing_task(user_id)
                        try:
                            shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                        except Exception as e:
                            await query.message.edit(Messages.ERROR_TXT.format(e))
                            await archive_msg.reply(Messages.ERROR_TXT.format(e))
                    break # خروج من الحلقة
                current_index += 1

        # بدء إرسال ألبومات الفيديو أولاً
        if total_video_albums > 0:
            await send_album_batches(video_albums, "video")
        else:
            # لا توجد فيديوهات، ننتقل إلى الصور مباشرة
            if total_image_albums > 0:
                await send_album_batches(image_albums, "image")
            else:
                # لا توجد صور أو فيديوهات، نرسل الملفات الأخرى فقط
                for file in other_files:
                    sent_files += 1
                    fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                    split = False
                    if fsize <= Config.TG_MAX_SIZE:
                        await send_file(
                            unzip_bot=unzip_bot,
                            c_id=spl_data[2],
                            doc_f=file,
                            query=query,
                            full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                            log_msg=log_msg,
                            split=False,
                        )
                    else:
                        split = True
                    if split:
                        fname = file.split("/")[-1]
                        smessage = await unzip_bot.send_message(
                            chat_id=user_id, text=Messages.SPLITTING.format(fname)
                        )
                        splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                        os.makedirs(splitteddir, exist_ok=True)
                        ooutput = f"{splitteddir}/{fname}"
                        splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                        LOGGER.info(splittedfiles)
                        if not splittedfiles:
                            try:
                                shutil.rmtree(splitteddir)
                            except:
                                pass
                            await del_ongoing_task(user_id)
                            await smessage.edit(Messages.ERR_SPLIT)
                            return
                        await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                        async_splittedfiles = async_generator(splittedfiles)
                        async for s_file in async_splittedfiles:
                            sent_files += 1
                            await send_file(
                                unzip_bot=unzip_bot,
                                c_id=user_id,
                                doc_f=s_file,
                                query=query,
                                full_path=splitteddir,
                                log_msg=log_msg,
                                split=True,
                            )
                try:
                    await unzip_bot.send_message(
                        chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                    )
                    await query.message.edit(
                        text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                    )
                except:
                    pass
                await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                await update_uploaded(user_id, upload_count=sent_files)
                await del_ongoing_task(user_id)
                try:
                    shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                except Exception as e:
                    await query.message.edit(Messages.ERROR_TXT.format(e))
                    await archive_msg.reply(Messages.ERROR_TXT.format(e))
        # --- نهاية التعديل الجديد ---
        
    elif query.data.startswith("next_album"):
        # مثال: "next_album|video|2|ext_a|..."
        parts = query.data.split("|")
        media_type = parts[1]
        index = int(parts[2])
        original_query_data = "|".join(parts[3:])
        # إعادة بناء spl_data من original_query_data
        spl_data = original_query_data.split("|")
        user_id = query.from_user.id
        file_path = f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}/extracted"
        try:
            urled = spl_data[4] if isinstance(spl_data[3], bool) else False
        except:
            urled = False
        if urled:
            paths = spl_data[4].namelist()
        else:
            paths = await get_files(path=file_path)

        # إعادة جمع الملفات كما فعلنا في ext_a
        image_files = []
        video_files = []
        other_files = []

        async_paths = async_generator(paths)
        async for file in async_paths:
            if not urled:
                if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    image_files.append(file)
                elif file.lower().endswith(('.mp4', '.mov', '.mkv', '.avi', '.gif')):
                    video_files.append(file)
                else:
                    other_files.append(file)
            else:
                if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    image_files.append(file)
                elif file.lower().endswith(('.mp4', '.mov', '.mkv', '.avi', '.gif')):
                    video_files.append(file)
                else:
                    other_files.append(file)

        # تقسيم إلى ألبومات
        video_albums = list(chunk_list(video_files, 10))
        image_albums = list(chunk_list(image_files, 10))

        # حسب النوع، نختار القائمة الصحيحة
        if media_type == "video":
            albums_list = video_albums
        else: # image
            albums_list = image_albums

        # التحقق من المؤشر
        if index < len(albums_list):
            album_batch = albums_list[index]
            success = await send_album_batch(unzip_bot, user_id, album_batch, log_msg)
            # --- التعديل: نحتاج تعريف sent_files وتحديثه ---
            # في هذه الحالة، نحتاج فقط إلى معرفة عدد الملفات المرسلة من هذا الألبوم
            sent_files = len(album_batch)
            # ---
            total_albums = len(albums_list)
            type_name = "Videos" if media_type == "video" else "Images"
            if index + 1 < total_albums:
                # زر "Next Album"
                next_button = InlineKeyboardButton(f"Next {type_name} Album 📀", callback_data=f"next_album|{media_type}|{index + 1}|{original_query_data}")
                cancel_button = InlineKeyboardButton("Cancel ❌", callback_data="cancel_dis")
                keyboard = InlineKeyboardMarkup([[next_button, cancel_button]])
                await unzip_bot.send_message(
                    chat_id=user_id,
                    text=f"Sent {type_name} Album #{index + 1}. Press Next to continue.",
                    reply_markup=keyboard
                )
            else:
                # انتهى هذا النوع
                if media_type == "video":
                    # ننتقل إلى صور الألبومات
                    total_image_albums = len(image_albums)
                    if total_image_albums > 0:
                        await unzip_bot.send_message(
                            chat_id=user_id,
                            text="Now starting to send image albums...",
                        )
                        # نبدأ من الألبوم الأول من الصور
                        if len(image_albums) > 0:
                            first_image_batch = image_albums[0]
                            success = await send_album_batch(unzip_bot, user_id, first_image_batch, log_msg)
                            # --- التعديل: نحتاج تعريف sent_files وتحديثه ---
                            sent_files = len(first_image_batch)
                            # ---
                            if len(image_albums) > 1:
                                # زر "Next Album" لصور
                                next_img_button = InlineKeyboardButton("Next Image Album 📀", callback_data=f"next_album|image|1|{original_query_data}")
                                cancel_button = InlineKeyboardButton("Cancel ❌", callback_data="cancel_dis")
                                keyboard = InlineKeyboardMarkup([[next_img_button, cancel_button]])
                                await unzip_bot.send_message(
                                    chat_id=user_id,
                                    text=f"Sent Image Album #1. Press Next to continue.",
                                    reply_markup=keyboard
                                )
                            else:
                                # انتهت صور الألبومات، نرسل الملفات الأخرى
                                for file in other_files:
                                    sent_files += 1
                                    fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                                    split = False
                                    if fsize <= Config.TG_MAX_SIZE:
                                        await send_file(
                                            unzip_bot=unzip_bot,
                                            c_id=spl_data[2],
                                            doc_f=file,
                                            query=query,
                                            full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                                            log_msg=log_msg,
                                            split=False,
                                        )
                                    else:
                                        split = True
                                    if split:
                                        fname = file.split("/")[-1]
                                        smessage = await unzip_bot.send_message(
                                            chat_id=user_id, text=Messages.SPLITTING.format(fname)
                                        )
                                        splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                                        os.makedirs(splitteddir, exist_ok=True)
                                        ooutput = f"{splitteddir}/{fname}"
                                        splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                                        LOGGER.info(splittedfiles)
                                        if not splittedfiles:
                                            try:
                                                shutil.rmtree(splitteddir)
                                            except:
                                                pass
                                            await del_ongoing_task(user_id)
                                            await smessage.edit(Messages.ERR_SPLIT)
                                            return
                                        await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                                        async_splittedfiles = async_generator(splittedfiles)
                                        async for s_file in async_splittedfiles:
                                            sent_files += 1
                                            await send_file(
                                                unzip_bot=unzip_bot,
                                                c_id=user_id,
                                                doc_f=s_file,
                                                query=query,
                                                full_path=splitteddir,
                                                log_msg=log_msg,
                                                split=True,
                                            )
                                try:
                                    await unzip_bot.send_message(
                                        chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                                    )
                                    await query.message.edit(
                                        text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                                    )
                                except:
                                    pass
                                await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                                await update_uploaded(user_id, upload_count=sent_files)
                                await del_ongoing_task(user_id)
                                try:
                                    shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                                except Exception as e:
                                    await query.message.edit(Messages.ERROR_TXT.format(e))
                                    await archive_msg.reply(Messages.ERROR_TXT.format(e))
                    else:
                        # لا توجد صور، نرسل الملفات الأخرى
                        for file in other_files:
                            sent_files += 1
                            fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                            split = False
                            if fsize <= Config.TG_MAX_SIZE:
                                await send_file(
                                    unzip_bot=unzip_bot,
                                    c_id=spl_data[2],
                                    doc_f=file,
                                    query=query,
                                    full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                                    log_msg=log_msg,
                                    split=False,
                                )
                            else:
                                split = True
                            if split:
                                fname = file.split("/")[-1]
                                smessage = await unzip_bot.send_message(
                                    chat_id=user_id, text=Messages.SPLITTING.format(fname)
                                )
                                splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                                os.makedirs(splitteddir, exist_ok=True)
                                ooutput = f"{splitteddir}/{fname}"
                                splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                                LOGGER.info(splittedfiles)
                                if not splittedfiles:
                                    try:
                                        shutil.rmtree(splitteddir)
                                    except:
                                        pass
                                    await del_ongoing_task(user_id)
                                    await smessage.edit(Messages.ERR_SPLIT)
                                    return
                                await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                                async_splittedfiles = async_generator(splittedfiles)
                                async for s_file in async_splittedfiles:
                                    sent_files += 1
                                    await send_file(
                                        unzip_bot=unzip_bot,
                                        c_id=user_id,
                                        doc_f=s_file,
                                        query=query,
                                        full_path=splitteddir,
                                        log_msg=log_msg,
                                        split=True,
                                    )
                        try:
                            await unzip_bot.send_message(
                                chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                            )
                            await query.message.edit(
                                text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                            )
                        except:
                            pass
                        await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                        await update_uploaded(user_id, upload_count=sent_files)
                        await del_ongoing_task(user_id)
                        try:
                            shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                        except Exception as e:
                            await query.message.edit(Messages.ERROR_TXT.format(e))
                            await archive_msg.reply(Messages.ERROR_TXT.format(e))
                else: # media_type == "image"
                    # انتهت صور الألبومات، نرسل الملفات الأخرى
                    for file in other_files:
                        sent_files += 1
                        fsize = Config.TG_MAX_SIZE + 1 if urled else await get_size(file)
                        split = False
                        if fsize <= Config.TG_MAX_SIZE:
                            await send_file(
                                unzip_bot=unzip_bot,
                                c_id=spl_data[2],
                                doc_f=file,
                                query=query,
                                full_path=f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}",
                                log_msg=log_msg,
                                split=False,
                            )
                        else:
                            split = True
                        if split:
                            fname = file.split("/")[-1]
                            smessage = await unzip_bot.send_message(
                                chat_id=user_id, text=Messages.SPLITTING.format(fname)
                            )
                            splitteddir = f"{Config.DOWNLOAD_LOCATION}/splitted/{user_id}"
                            os.makedirs(splitteddir, exist_ok=True)
                            ooutput = f"{splitteddir}/{fname}"
                            splittedfiles = await split_files(file, ooutput, Config.TG_MAX_SIZE)
                            LOGGER.info(splittedfiles)
                            if not splittedfiles:
                                try:
                                    shutil.rmtree(splitteddir)
                                except:
                                    pass
                                await del_ongoing_task(user_id)
                                await smessage.edit(Messages.ERR_SPLIT)
                                return
                            await smessage.edit(Messages.SEND_ALL_PARTS.format(fname))
                            async_splittedfiles = async_generator(splittedfiles)
                            async for s_file in async_splittedfiles:
                                sent_files += 1
                                await send_file(
                                    unzip_bot=unzip_bot,
                                    c_id=user_id,
                                    doc_f=s_file,
                                    query=query,
                                    full_path=splitteddir,
                                    log_msg=log_msg,
                                    split=True,
                                )
                    try:
                        await unzip_bot.send_message(
                            chat_id=user_id, text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                        )
                        await query.message.edit(
                            text=Messages.UPLOADED, reply_markup=Buttons.RATE_ME
                        )
                    except:
                        pass
                    await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
                    await update_uploaded(user_id, upload_count=sent_files)
                    await del_ongoing_task(user_id)
                    try:
                        shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{spl_data[1]}")
                    except Exception as e:
                        await query.message.edit(Messages.ERROR_TXT.format(e))
                        await archive_msg.reply(Messages.ERROR_TXT.format(e))

    elif query.data == "cancel_dis":
        uid = query.from_user.id
        await del_ongoing_task(uid)
        await del_merge_task(uid)
        try:
            await query.message.edit(
                Messages.CANCELLED_TXT.format(Messages.PROCESS_CANCELLED)
            )
            shutil.rmtree(f"{Config.DOWNLOAD_LOCATION}/{uid}")
            await update_uploaded(user_id=uid, upload_count=sent_files)
            try:
                await log_msg.reply(Messages.HOW_MANY_UPLOADED.format(sent_files))
            except:
                return
        except:
            await unzip_bot.send_message(
                chat_id=uid,
                text=Messages.CANCELLED_TXT.format(Messages.PROCESS_CANCELLED),
            )
            return
    elif query.data == "nobully":
        await query.message.edit(Messages.CANCELLED)
