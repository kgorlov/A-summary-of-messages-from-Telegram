# -*- coding: utf-8 -*-
"""
Экспорт Telegram → сообщения одного пользователя из выбранного чата ИЛИ из всех чатов.
Фильтр по user-id (например 305696040) ИЛИ по части имени (--user).
Выбор чата по названию (--chat) ИЛИ по числовому ID (--chat-id).
Если чат не указан, и задан --user/--user-id, поиск идёт по всем чатам.

Особенности:
- Устойчив к битым JSON-хвостам, нормализует Unicode пробелы и BOM.
- Корректно обходит файлы без 'chats.list' (склейка нескольких чатов подряд).
- Для мульти-поиска пишет единые *_ALL.txt / *_ALL.csv с колонкой chat.

Примеры:
    # 1. Один чат по id
    python tg_self_analyze.py --in result.json --user-id 305696040 --chat-id 1863959277

    # 2. Один чат по названию (частичный матч)
    python tg_self_analyze.py --in result.json --user "Татьяна" --chat "42 ИП Куратор"

    # 3. По всем чатам (только user или только user-id)
    python tg_self_analyze.py --in Kirill.json --user "Михаил"
    python tg_self_analyze.py --in Kirill.json --user-id 305696040
"""

import argparse, json, re, csv, unicodedata
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

# ---------- утилки ----------
def u_norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("\u00A0", " ").replace("\u2009", " ").replace("\u202F", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def extract_text(msg: Dict[str, Any]) -> str:
    t = msg.get("text", "")
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        parts: List[str] = []
        for x in t:
            if isinstance(x, str):
                parts.append(x)
            elif isinstance(x, dict):
                parts.append(x.get("text", ""))
        return "".join(parts)
    return ""

def strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

def norm_sender(m: Dict[str, Any]) -> str:
    s = m.get("from") or m.get("actor") or m.get("from_id") or ""
    return u_norm(s)

def norm_from_id(m: Dict[str, Any]) -> str:
    """Возвращает from_id как строку вида 'user123...' или ''."""
    fid = m.get("from_id")
    if fid is None:
        return ""
    return str(fid)

def want_user_id_string(uid: Optional[int]) -> Optional[str]:
    """Для 305696040 вернет 'user305696040'."""
    if uid is None:
        return None
    return f"user{int(uid)}"

# ---------- потоковый парсер объектов { ... } ----------
def stream_next_object(s: str, idx: int) -> Tuple[Optional[str], int]:
    n = len(s)
    # пропускаем пробелы, запятые и всякий юникод-мусор, включая BOM и узкие пробелы
    SKIP = " \r\n\t,\ufeff\u00A0\u2009\u202F"
    while idx < n and s[idx] in SKIP:
        idx += 1
    if idx >= n or s[idx] != "{":
        return None, idx
    start = idx
    depth = 0
    in_str = False
    esc = False
    while idx < n:
        ch = s[idx]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    idx += 1
                    return s[start:idx], idx
        idx += 1
    return None, idx

# ---------- распознавание формата ----------
def has_chats_list(raw: str) -> bool:
    return re.search(r'"chats"\s*:\s*\{\s*"list"\s*:\s*\[', raw) is not None

def find_chats_list_start(raw: str) -> int:
    m = re.search(r'"chats"\s*:\s*\{\s*"list"\s*:\s*\[', raw)
    if not m:
        raise ValueError("В файле не найден блок 'chats.list'.")
    return m.end()

def chat_name_from_obj_str(chat_obj_str: str) -> str:
    m = re.search(r'"name"\s*:\s*"([^"]*)"', chat_obj_str)
    return m.group(1) if m else ""

def chat_id_from_obj_str(chat_obj_str: str) -> Optional[int]:
    m = re.search(r'"id"\s*:\s*([0-9]+)', chat_obj_str)
    return int(m.group(1)) if m else None

def find_messages_array_in(container: str) -> Optional[int]:
    m = re.search(r'"messages"\s*:\s*\[', container)
    return m.end() if m else None

def stream_parse_messages_from_container(container_str: str) -> List[Dict[str, Any]]:
    start = find_messages_array_in(container_str)
    if not start:
        return []
    after = container_str[start:]
    msgs: List[Dict[str, Any]] = []
    idx = 0
    while idx < len(after):
        obj_str, idx2 = stream_next_object(after, idx)
        if not obj_str:
            # не ломаемся, ползем дальше
            idx += 1
            continue
        try:
            obj = json.loads(obj_str)
            msgs.append(obj)
        except Exception:
            # битый хвост — идем дальше
            pass
        idx = idx2 if idx2 > idx else idx + 1
    return msgs

# ---------- выбор чата ----------
def is_saved_chat(name: str) -> bool:
    n = u_norm(name).lower()
    return "saved messages" in n or "избранное" in n or n == "саня"

def list_all_chats(raw: str, cap: int = 10000) -> List[Tuple[str, Optional[int], str]]:
    out = []
    if has_chats_list(raw):
        i = find_chats_list_start(raw)
        k = 0
        while i < len(raw) and k < cap:
            obj_str, i2 = stream_next_object(raw, i)
            if not obj_str:
                i += 1
                continue
            if find_messages_array_in(obj_str):
                out.append((
                    obj_str,
                    chat_id_from_obj_str(obj_str),
                    chat_name_from_obj_str(obj_str) or "(без названия)"
                ))
                k += 1
            i = i2 if i2 > i else i + 1
        return out

    # Нет chats.list → файл может быть «склейкой» из нескольких чатов.
    i = 0
    k = 0
    n = len(raw)
    while i < n and k < cap:
        obj_str, i2 = stream_next_object(raw, i)
        if not obj_str:
            i += 1   # главное — не ломаться
            continue
        if find_messages_array_in(obj_str):
            out.append((
                obj_str,
                chat_id_from_obj_str(obj_str),
                chat_name_from_obj_str(obj_str) or "(без названия)"
            ))
            k += 1
        i = i2 if i2 > i else i + 1

    # Если ничего не нашли, падаем обратно на «один чат»
    return out or [(raw, chat_id_from_obj_str(raw), chat_name_from_obj_str(raw) or "(single chat export)")]

def pick_chat_container(raw: str, chat_query: Optional[str], chat_id: Optional[int]) -> Tuple[str, str]:
    chats = list_all_chats(raw)
    print("\nНайденные чаты:")
    for i, (_, cid, nm) in enumerate(chats[:200], 1):
        print(f"{i:2d}. {nm} [id={cid}]")
    if not chats:
        raise ValueError("Не найден ни один чат в файле.")

    if chat_id is not None:
        for obj_str, cid, name in chats:
            if cid == chat_id:
                return obj_str, name
        sample = "\n".join(f"- {nm} [id={ci}]" for _, ci, nm in chats[:30])
        raise ValueError(f"Чат с id={chat_id} не найден. Примеры:\n{sample}")

    if chat_query:
        q = u_norm(chat_query).lower()
        # точное
        for obj_str, cid, name in chats:
            if u_norm(name).lower() == q:
                return obj_str, name
        # содержит
        for obj_str, cid, name in chats:
            if q in u_norm(name).lower():
                return obj_str, name
        # начинается с
        for obj_str, cid, name in chats:
            if u_norm(name).lower().startswith(q):
                return obj_str, name
        sample = "\n".join(f"- {nm} [id={ci}]" for _, ci, nm in chats[:30])
        raise ValueError(f"Чат по маске '{chat_query}' не найден. Примеры:\n{sample}")

    # По умолчанию — первый не «Избранное»
    for obj_str, cid, name in chats:
        if not is_saved_chat(name):
            return obj_str, name
    return chats[0][0], chats[0][2]

# ---------- индекс ----------
def build_id_index(messages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    idx: Dict[int, Dict[str, Any]] = {}
    for m in messages:
        if m.get("type") != "message":
            continue
        mid = m.get("id")
        if mid is None:
            continue
        try:
            mid = int(mid)
        except Exception:
            continue
        idx[mid] = m
    return idx

# ---------- сборка строк из одного контейнера ----------
def collect_rows_from_container(container_str: str,
                                chat_name: str,
                                user_id: Optional[int],
                                user_query: Optional[str],
                                exact: bool) -> List[Dict[str, Any]]:
    messages = stream_parse_messages_from_container(container_str)
    id_index = build_id_index(messages)

    want_uid_str = want_user_id_string(user_id) if user_id is not None else None
    uq = (user_query or "").lower() if user_query else ""
    rows: List[Dict[str, Any]] = []

    for msg in messages:
        if msg.get("type") != "message":
            continue

        # фильтрация по автору
        if want_uid_str:
            if norm_from_id(msg) != want_uid_str:
                continue
        else:
            sender = norm_sender(msg)
            s_norm = sender.lower()
            if exact:
                if sender != (user_query or ""):
                    continue
            else:
                if uq not in s_norm:
                    continue

        text = extract_text(msg).strip()
        if not text:
            continue

        mid = msg.get("id")
        date = msg.get("date")
        reply_id = msg.get("reply_to_message_id")

        r_from = r_date = r_text = ""
        if reply_id is not None:
            try:
                rmsg = id_index.get(int(reply_id))
            except Exception:
                rmsg = None
            if rmsg:
                r_from = norm_sender(rmsg)
                r_date = rmsg.get("date") or ""
                r_text = extract_text(rmsg).strip()

        rows.append({
            "chat": chat_name,
            "id": mid,
            "date": date,
            "from": norm_sender(msg),
            "from_id": norm_from_id(msg),
            "text": text,
            "reply_to_id": reply_id,
            "reply_from": r_from,
            "reply_date": r_date,
            "reply_text": r_text
        })
    return rows

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="result.json", help="Путь к Telegram export JSON")
    # фильтр автора: ЛИБО user-id, ЛИБО часть имени
    ap.add_argument("--user-id", type=int, default=None, help="Числовой ID пользователя (например 305696040)")
    ap.add_argument("--user", default=None, help="Часть имени автора (без регистра). Игнорируется, если задан --user-id")
    ap.add_argument("--exact", action="store_true", help="Точный матч имени (только для --user)")
    # выбор чата
    ap.add_argument("--chat", default=None, help="Название чата (или часть). Unicode-нормализация включена.")
    ap.add_argument("--chat-id", type=int, default=None, help="Числовой ID чата из экспорта (надёжнее, чем имя)")
    # вывод
    ap.add_argument("--txt-out", dest="txt_out", default=None, help="Путь TXT")
    ap.add_argument("--csv-out", dest="csv_out", default=None, help="Путь CSV")
    args = ap.parse_args()

    raw = strip_bom(open(args.in_path, "r", encoding="utf-8").read())

    # валидация фильтра автора
    if args.user_id is None and not args.user:
        raise SystemExit("Укажи --user-id ИЛИ --user. Примеры: --user-id 305696040  или  --user \"Кирилл\"")

    user_query = u_norm(args.user) if args.user else None

    # режим 1: указан конкретный чат => как раньше, один контейнер
    if args.chat or args.chat_id is not None:
        container, chat_name = pick_chat_container(raw, args.chat, args.chat_id)
        rows = collect_rows_from_container(container, chat_name, args.user_id, user_query, args.exact)

        txt_path = args.txt_out or f"messages_{args.user_id or (user_query or 'user')}.txt"
        csv_path = args.csv_out or f"messages_{args.user_id or (user_query or 'user')}.csv"

        with open(txt_path, "w", encoding="utf-8") as f:
            who = f"user-id={args.user_id}" if args.user_id is not None else f"user~'{user_query}'"
            f.write(f"# Чат: {chat_name}\n# Фильтр: {who}\n\n")
            for r in rows:
                f.write(f"[{r['date']}] {r['from']} ({r['from_id']}) id={r['id']}:\n{r['text']}\n")
                if r["reply_to_id"]:
                    f.write(f"\n  ↳ В ответ на (id={r['reply_to_id']}) [{r['reply_date']}] {r['reply_from']}:\n")
                    f.write(("    > " + r["reply_text"].replace('\n', '\n    > ') + "\n") if r["reply_text"] else "    > [без текста или медиа]\n")
                f.write("\n" + "-"*80 + "\n\n")

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "chat","id","date","from","from_id","text",
                "reply_to_id","reply_from","reply_date","reply_text"
            ])
            w.writeheader(); w.writerows(rows)

        print(f"Чат: {chat_name}")
        print(f"Найдено сообщений: {len(rows)}")
        print(f"TXT: {txt_path}")
        print(f"CSV: {csv_path}")
        return

    # режим 2: НЕ указан чат => обходим ВСЕ чаты
    chats = list_all_chats(raw)
    print("\nНайденные чаты:")
    for i, (_, cid, nm) in enumerate(chats[:200], 1):
        print(f"{i:2d}. {nm} [id={cid}]")

    all_rows: List[Dict[str, Any]] = []
    per_chat_counts: List[Tuple[str, int]] = []

    for obj_str, cid, name in chats:
        rows = collect_rows_from_container(obj_str, name, args.user_id, user_query, args.exact)
        if rows:
            per_chat_counts.append((f"{name} [id={cid}]", len(rows)))
            all_rows.extend(rows)

    txt_path = args.txt_out or f"messages_{args.user_id or (user_query or 'user')}_ALL.txt"
    csv_path = args.csv_out or f"messages_{args.user_id or (user_query or 'user')}_ALL.csv"

    # TXT: группируем по чату для удобства
    by_chat: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in all_rows:
        by_chat[r["chat"]].append(r)

    with open(txt_path, "w", encoding="utf-8") as f:
        who = f"user-id={args.user_id}" if args.user_id is not None else f"user~'{user_query}'"
        f.write(f"# Все чаты\n# Фильтр: {who}\n\n")
        for chat in sorted(by_chat.keys()):
            f.write(f"## Чат: {chat}\n\n")
            for r in by_chat[chat]:
                f.write(f"[{r['date']}] {r['from']} ({r['from_id']}) id={r['id']}:\n{r['text']}\n")
                if r["reply_to_id"]:
                    f.write(f"\n  ↳ В ответ на (id={r['reply_to_id']}) [{r['reply_date']}] {r['reply_from']}:\n")
                    f.write(("    > " + r["reply_text"].replace('\n', '\n    > ') + "\n") if r["reply_text"] else "    > [без текста или медиа]\n")
                f.write("\n" + "-"*80 + "\n\n")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "chat","id","date","from","from_id","text",
            "reply_to_id","reply_from","reply_date","reply_text"
        ])
        w.writeheader(); w.writerows(all_rows)

    print("\nИтоги по чатам (где что-то нашлось):")
    for name, cnt in per_chat_counts:
        print(f"- {name}: {cnt}")

    print(f"\nВсего найдено сообщений: {len(all_rows)}")
    print(f"TXT: {txt_path}")
    print(f"CSV: {csv_path}")

if __name__ == "__main__":
    main()
