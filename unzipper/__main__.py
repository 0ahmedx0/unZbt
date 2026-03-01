# Copyright (c) 2022 - 2024 EDM115
import os
import signal
import time
import asyncio

from pyrogram import idle

from config import Config
from . import LOGGER, unzipperbot
from .helpers.start import (
    dl_thumbs,
    set_boot_time,
    start_cron_jobs,
    removal,
)
from .modules.bot_data import Messages

# دالة التعامل مع إشارات إيقاف النظام
def handler_stop_signals(signum, frame):
    LOGGER.info(
        "Received stop signal (%s, %s, %s). Exiting...",
        signal.Signals(signum).name,
        signum,
        frame,
    )
    shutdown_bot()

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)

# دالة إغلاق البوت المعدلة (بدون إرسال رسائل للقناة)
def shutdown_bot():
    stoptime = time.strftime("%Y/%m/%d - %H:%M:%S")
    LOGGER.info(Messages.STOP_TXT.format(stoptime))
    # تم حذف محاولة الإرسال لقناة السجلات لتجنب الأخطاء
    LOGGER.info("Bot stopped 😪")
    try:
        unzipperbot.stop(block=False)
    except:
        pass

if __name__ == "__main__":
    try:
        # التأكد من وجود المجلدات الضرورية
        os.makedirs(Config.DOWNLOAD_LOCATION, exist_ok=True)
        os.makedirs(Config.THUMB_LOCATION, exist_ok=True)
        
        LOGGER.info(Messages.STARTING_BOT)
        
        # تشغيل البوت
        unzipperbot.start()
        
        # إعداد وقت الإقلاع
        set_boot_time()
        
        # تحميل الصور المصغرة (إذا وجدت)
        dl_thumbs()
        
        # تنظيف الملفات المؤقتة القديمة
        removal(True)
        
        # بدء المهام المجدولة
        start_cron_jobs()
        
        LOGGER.info(Messages.BOT_RUNNING)
        LOGGER.info("✅ Bot has started successfully (Logs Channel disabled).")
        
        # إبقاء البوت يعمل
        idle()

    except Exception as e:
        LOGGER.error("Error in main loop : %s", e)
    finally:
        shutdown_bot()
