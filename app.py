import asyncio
import csv
import io
import ipaddress
import logging
import os
import secrets
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

BOT_TOKEN = os.getenv('BOT_TOKEN', '')
ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
BASE_URL = (os.getenv('BASE_URL') or os.getenv('RENDER_EXTERNAL_URL', '')).rstrip('/')
DB_PATH = Path(os.getenv('DB_PATH', str(Path(__file__).resolve().parent / 'data.sqlite3')))
ON_RENDER = os.getenv('RENDER') == 'true'
HOST = '0.0.0.0' if ON_RENDER else '127.0.0.1'
PORT = int(os.getenv('PORT', '10000' if ON_RENDER else '8000'))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('stream_bot')
app = FastAPI()
dp = Dispatcher()
bot = None
stream_url = None
active_admin_id = 0
pending_transfer = None
admin_lock = asyncio.Lock()


def valid_url(url):
    if not url or any(c.isspace() or ord(c) < 32 for c in url):
        return False
    try:
        p = urlsplit(url)
        return p.scheme.lower() in ('http', 'https') and bool(p.hostname) and p.username is None and p.password is None and (p.port is None or 1 <= p.port <= 65535)
    except ValueError:
        return False


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute('PRAGMA busy_timeout=10000')
        with conn:
            yield conn
    finally:
        conn.close()


def initialize():
    global active_admin_id
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE IF NOT EXISTS users (tg_id INTEGER PRIMARY KEY, username TEXT NOT NULL, token TEXT NOT NULL UNIQUE)')
        conn.execute('CREATE TABLE IF NOT EXISTS ip_logs (id INTEGER PRIMARY KEY, tg_id INTEGER NOT NULL, username TEXT NOT NULL, ip_address TEXT NOT NULL, timestamp TEXT NOT NULL)')
        conn.execute('CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        conn.execute("INSERT INTO config(key,value) VALUES('admin_id',?) ON CONFLICT(key) DO NOTHING", (str(ADMIN_ID),))
        active_admin_id = int(conn.execute("SELECT value FROM config WHERE key='admin_id'").fetchone()[0])
        row = conn.execute("SELECT value FROM config WHERE key='stream_url'").fetchone()
        return row[0] if row and valid_url(row[0]) else None


def save_user(tg_id, username):
    with db() as conn:
        conn.execute('INSERT INTO users (tg_id, username, token) VALUES (?, ?, ?) ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username', (tg_id, username, secrets.token_urlsafe(24)))
        return conn.execute('SELECT token FROM users WHERE tg_id=?', (tg_id,)).fetchone()[0]


def lookup_user(token):
    with db() as conn:
        return conn.execute('SELECT tg_id, username FROM users WHERE token=?', (token,)).fetchone()


def save_url(url):
    with db() as conn:
        conn.execute("INSERT INTO config(key,value) VALUES('stream_url',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (url,))


def store_click(tg_id, username, ip, timestamp):
    with db() as conn:
        conn.execute('INSERT INTO ip_logs (tg_id,username,ip_address,timestamp) VALUES (?,?,?,?)', (tg_id, username, ip, timestamp))


def read_logs():
    with db() as conn:
        return conn.execute('SELECT tg_id,username,ip_address,timestamp FROM ip_logs ORDER BY id DESC').fetchall()


def admin(message):
    return bool(message.from_user and message.from_user.id == active_admin_id and message.chat.type == 'private')


def save_admin(tg_id):
    with db() as conn:
        conn.execute("UPDATE config SET value=? WHERE key='admin_id'", (str(tg_id),))


@dp.message(Command('myid'))
async def myid(message: Message):
    if message.from_user and message.chat.type == 'private':
        await message.answer(f'Ваш Telegram ID: {message.from_user.id}')


@dp.message(Command('setadmin'))
async def setadmin(message: Message, command: CommandObject):
    global pending_transfer
    async with admin_lock:
        if not admin(message):
            return
        value = (command.args or '').strip()
        if not value.isascii() or not value.isdecimal() or not 0 < int(value) < 2**63:
            await message.answer('Укажите ID друга: /setadmin 123456789')
            return
        target = int(value)
        if target == active_admin_id:
            await message.answer('Этот пользователь уже администратор.')
            return
        try:
            await bot.send_message(target, 'Вам предлагают стать единственным администратором этого бота. Если вы согласны, отправьте /acceptadmin в течение 15 минут.')
        except Exception:
            log.exception('Cannot invite new administrator')
            await message.answer('Не удалось отправить приглашение. Другу нужно сначала открыть бота и нажать Start; проверьте ID.')
            return
        pending_transfer = (target, time.monotonic() + 900)
        await message.answer('Приглашение отправлено. Права перейдут другу только после его подтверждения.')


@dp.message(Command('acceptadmin'))
async def acceptadmin(message: Message):
    global active_admin_id, pending_transfer
    if not message.from_user or message.chat.type != 'private':
        return
    async with admin_lock:
        if not pending_transfer or pending_transfer[0] != message.from_user.id or time.monotonic() > pending_transfer[1]:
            await message.answer('Нет действующего приглашения для этого аккаунта.')
            return
        try:
            await asyncio.to_thread(save_admin, message.from_user.id)
        except Exception:
            log.exception('Cannot transfer administrator')
            await message.answer('Не удалось сохранить права. Попробуйте ещё раз.')
            return
        active_admin_id = message.from_user.id
        pending_transfer = None
        await message.answer('Готово. Теперь вы единственный администратор бота.')


@dp.message(Command('start'))
async def start(message: Message):
    if not message.from_user or message.chat.type != 'private':
        return
    user = message.from_user
    try:
        token = await asyncio.to_thread(save_user, user.id, user.username or '')
    except Exception:
        log.exception('Cannot save user')
        await message.answer('Не удалось подготовить ссылку. Попробуйте позже.')
        return
    link = BASE_URL + '/go?' + urlencode({'token': token})
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='Перейти на стрим', url=link)]])
    await message.answer('🔴 Стрим уже ждёт!\n\nЗалетай по кнопке ниже 👇')
    await message.answer('🔴 The stream is live!\n\nJoin using the button below 👇', reply_markup=keyboard)


@dp.message(Command('setlink'))
async def setlink(message: Message, command: CommandObject):
    global stream_url
    async with admin_lock:
        if not admin(message):
            return
        url = (command.args or '').strip()
        if not valid_url(url):
            await message.answer('Укажите полный адрес: /setlink https://example.com/stream')
            return
        try:
            await asyncio.to_thread(save_url, url)
            stream_url = url
        except Exception:
            log.exception('Cannot save URL')
            await message.answer('Не удалось сохранить ссылку.')
            return
        await message.answer('Ссылка на стрим обновлена.')


@dp.message(Command('getlogs'))
async def getlogs(message: Message):
    async with admin_lock:
        if not admin(message):
            return
        try:
            rows = await asyncio.to_thread(read_logs)
        except Exception:
            log.exception('Cannot read logs')
            await message.answer('Не удалось прочитать журнал.')
            return
        if not rows:
            await message.answer('Переходов пока нет.')
            return
        out = io.StringIO(newline='')
        writer = csv.writer(out, delimiter=';')
        writer.writerow(['Telegram ID', 'Username', 'IP', 'Время UTC'])
        writer.writerows(rows)
        await message.answer_document(BufferedInputFile(out.getvalue().encode('utf-8-sig'), filename='logs.csv'), caption=f'Переходов: {len(rows)}. Время указано в UTC.')


async def record_and_notify(tg_id, username, ip, timestamp):
    try:
        await asyncio.to_thread(store_click, tg_id, username, ip, timestamp)
    except Exception:
        log.exception('Cannot save click')
    async with admin_lock:
        try:
            await bot.send_message(
                active_admin_id,
                f'🔔 Переход на стрим\n'
                f'Юзернейм: {"@" + username if username else "не указан"}\n'
                f'Telegram ID: {tg_id}\n'
                f'IP: {ip}\n\n'
                'Перешёл по ссылке на стрим.',
            )
        except Exception:
            log.exception('Cannot notify administrator')


@app.get('/go')
async def go(request: Request, background_tasks: BackgroundTasks, token: str = ''):
    if not stream_url:
        raise HTTPException(status_code=503, detail='Ссылка на стрим пока не настроена.')
    if len(token) > 128 or not token:
        raise HTTPException(status_code=404, detail='Ссылка недействительна.')
    user = await asyncio.to_thread(lookup_user, token)
    if not user:
        raise HTTPException(status_code=404, detail='Ссылка недействительна.')
    ip = client_ip(request)
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    background_tasks.add_task(record_and_notify, user[0], user[1], ip, now)
    return RedirectResponse(stream_url, status_code=302, headers={'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'})


def client_ip(request: Request) -> str:
    if ON_RENDER:
        # Render's public edge provides CF-Connecting-IP. Do not use this
        # deployment mode behind a different proxy or expose the app directly.
        raw = request.headers.get('cf-connecting-ip', '')
        try:
            return str(ipaddress.ip_address(raw.strip()))
        except ValueError:
            return 'неизвестен'
    return request.client.host if request.client else 'неизвестен'


@app.get('/health')
async def health():
    return {'status': 'ok'}


async def main():
    global bot, stream_url
    if not BOT_TOKEN or ADMIN_ID <= 0 or not valid_url(BASE_URL):
        raise RuntimeError('Укажите BOT_TOKEN, ADMIN_ID и BASE_URL в переменных окружения.')
    stream_url = await asyncio.to_thread(initialize)
    bot = Bot(BOT_TOKEN)
    server = uvicorn.Server(uvicorn.Config(app, host=HOST, port=PORT, proxy_headers=not ON_RENDER, forwarded_allow_ips='127.0.0.1,::1', access_log=False))
    async def poll():
        try:
            await dp.start_polling(bot, handle_signals=False)
        finally:
            server.should_exit = True
    async def serve():
        try:
            await server.serve()
        finally:
            try:
                await dp.stop_polling()
            except RuntimeError:
                pass
    try:
        await asyncio.gather(poll(), serve())
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())

