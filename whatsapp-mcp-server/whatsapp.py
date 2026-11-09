import os
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple
import requests
import json
import audio
import psycopg

DATABASE_URL = os.environ.get(
    "WHATSAPP_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:5432/whatsapp?sslmode=disable",
)
WHATSAPP_API_BASE_URL = "http://localhost:8080/api"
# Bridge container only mounts this folder at /app/store.
_HOST_BRIDGE_STORE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "whatsapp-bridge", "store")
)
_CONTAINER_BRIDGE_STORE = "/app/store"


def media_path_for_bridge(media_path: str) -> str:
    """Map a host media path to a path the Docker bridge can open."""
    abs_path = os.path.abspath(media_path)
    store = os.path.abspath(_HOST_BRIDGE_STORE)
    try:
        common = os.path.commonpath([abs_path, store])
    except ValueError:
        common = ""
    if common == store:
        rel = os.path.relpath(abs_path, store).replace("\\", "/")
        return f"{_CONTAINER_BRIDGE_STORE}/{rel}"
    # Copy into the mounted store so the bridge can read it.
    os.makedirs(store, exist_ok=True)
    dest = os.path.join(store, os.path.basename(abs_path))
    if os.path.normcase(abs_path) != os.path.normcase(os.path.abspath(dest)):
        import shutil
        shutil.copy2(abs_path, dest)
    return f"{_CONTAINER_BRIDGE_STORE}/{os.path.basename(abs_path)}"


def bridge_path_to_host(path: Optional[str]) -> Optional[str]:
    """Map a bridge/container media path back to a host path Cursor can Read."""
    if not path:
        return path
    normalized = path.replace("\\", "/")
    store = os.path.abspath(_HOST_BRIDGE_STORE)
    prefixes = (
        _CONTAINER_BRIDGE_STORE + "/",
        "/app/store/",
        "store/",
    )
    for prefix in prefixes:
        if normalized.startswith(prefix):
            rel = normalized[len(prefix):].lstrip("/")
            return os.path.abspath(os.path.join(store, *rel.split("/")))
    # Absolute container path that already includes store segment
    marker = "/store/"
    if marker in normalized:
        rel = normalized.split(marker, 1)[1]
        return os.path.abspath(os.path.join(store, *rel.split("/")))
    if os.path.isfile(path):
        return os.path.abspath(path)
    return path


def connect():
    return psycopg.connect(DATABASE_URL)

def as_dt(value):
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))

def like_pattern(query: str) -> str:
    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"

def phone_variants(query: str) -> List[str]:
    """Normalize a phone or JID into comparable digit forms (62... and 0...)."""
    raw = (query or "").strip()
    userpart = raw.split("@", 1)[0]
    variants = set()
    for source in (raw, userpart):
        digits = re.sub(r"\D", "", source)
        if len(digits) < 6:
            continue
        variants.add(digits)
        if digits.startswith("0"):
            variants.add("62" + digits[1:])
        if digits.startswith("62") and len(digits) > 2:
            variants.add("0" + digits[2:])
    return sorted(variants)

def lookup_parties(cursor, query: str) -> Tuple[List[str], List[str]]:
    """Map a phone, LID, or JID to every sender id and direct-chat JID for that person."""
    raw = (query or "").strip()
    variants = phone_variants(raw)
    userpart = raw.split("@", 1)[0] if raw else ""
    seeds = list(dict.fromkeys([*variants, userpart] if userpart else variants))
    if not seeds:
        return [], []
    cursor.execute(
        """
        SELECT DISTINCT sender_id, chat_jid
        FROM (
            SELECT lm.pn AS sender_id, lm.pn || '@s.whatsapp.net' AS chat_jid
            FROM whatsmeow_lid_map lm
            WHERE lm.pn = ANY(%s::text[]) OR lm.lid = ANY(%s::text[])
            UNION
            SELECT lm.lid, lm.lid || '@lid'
            FROM whatsmeow_lid_map lm
            WHERE lm.pn = ANY(%s::text[]) OR lm.lid = ANY(%s::text[])
            UNION
            SELECT split_part(ct.their_jid, '@', 1), ct.their_jid
            FROM whatsmeow_contacts ct
            WHERE split_part(ct.their_jid, '@', 1) = ANY(%s::text[])
               OR ct.their_jid = %s
        ) ids
        WHERE coalesce(sender_id, '') <> ''
        """,
        (seeds, seeds, seeds, seeds, seeds, raw),
    )
    senders = set(seeds)
    chats = set()
    if "@" in raw:
        chats.add(raw)
    for sender_id, chat_jid in cursor.fetchall():
        if sender_id:
            senders.add(sender_id)
        if chat_jid:
            chats.add(chat_jid)
    for sender_id in list(senders):
        if sender_id and "@" not in sender_id:
            chats.add(f"{sender_id}@lid")
            chats.add(f"{sender_id}@s.whatsapp.net")
    return sorted(senders), sorted(chats)

def resolve_chat_jids(cursor, chat_ref: str) -> List[str]:
    """Turn a JID, phone number, or chat/contact name into chat JIDs."""
    raw = (chat_ref or "").strip()
    if not raw:
        return []
    found: List[str] = []
    if "@" in raw:
        found.append(raw)
    _, direct_chats = lookup_parties(cursor, raw)
    found.extend(direct_chats)
    if "@" not in raw:
        pattern = like_pattern(raw)
        cursor.execute(
            """
            SELECT jid
            FROM chats
            WHERE name ILIKE %s ESCAPE '\\' OR jid = %s
            ORDER BY last_message_time DESC NULLS LAST
            LIMIT 8
            """,
            (pattern, raw),
        )
        found.extend(row[0] for row in cursor.fetchall())
        for item in search_contacts(raw, limit=8):
            if item.get("chat_jid"):
                found.append(item["chat_jid"])
    return list(dict.fromkeys(jid for jid in found if jid))[:8]

@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None
    media_type: Optional[str] = None

@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

@dataclass
class Contact:
    phone_number: str
    name: Optional[str]
    jid: str

@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]

def get_sender_name(sender_jid: str) -> str:
    try:
        conn = connect()
        cursor = conn.cursor()
        userpart = (sender_jid or "").split("@", 1)[0]

        cursor.execute("""
            SELECT COALESCE(
                NULLIF(ct.full_name, ''),
                NULLIF(ct.first_name, ''),
                NULLIF(ct.push_name, ''),
                NULLIF(ct.business_name, ''),
                NULLIF(ch.name, '')
            )
            FROM whatsmeow_lid_map lm
            LEFT JOIN whatsmeow_contacts ct
              ON split_part(ct.their_jid, '@', 1) IN (lm.pn, lm.lid)
            LEFT JOIN chats ch
              ON ch.jid IN (lm.lid || '@lid', lm.pn || '@s.whatsapp.net')
            WHERE lm.pn = %s OR lm.lid = %s
            ORDER BY
              (NULLIF(ct.full_name, '') IS NOT NULL) DESC,
              (NULLIF(ct.first_name, '') IS NOT NULL) DESC,
              (NULLIF(ct.push_name, '') IS NOT NULL) DESC
            LIMIT 1
        """, (userpart, userpart))
        result = cursor.fetchone()
        if result and result[0] and result[0] != userpart:
            return result[0]

        cursor.execute("""
            SELECT COALESCE(
                NULLIF(full_name, ''),
                NULLIF(first_name, ''),
                NULLIF(push_name, ''),
                NULLIF(business_name, '')
            )
            FROM whatsmeow_contacts
            WHERE their_jid = %s OR split_part(their_jid, '@', 1) = %s
            ORDER BY
              (NULLIF(full_name, '') IS NOT NULL) DESC,
              (NULLIF(push_name, '') IS NOT NULL) DESC
            LIMIT 1
        """, (sender_jid, userpart))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        return sender_jid
        
    except psycopg.Error as e:
        print(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if 'conn' in locals():
            conn.close()

def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""
    
    if show_chat_info and message.chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {message.chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "
        
    content_prefix = ""
    if hasattr(message, 'media_type') and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "
    
    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {content_prefix}{message.content}\n"
    except Exception as e:
        print(f"Error formatting message: {e}")
    return output

def format_messages_list(messages: List[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output
    
    for message in messages:
        output += format_message(message, show_chat_info)
    return output

def _message_dict(row, role: str = "match") -> dict:
    """Build one message dict.

    Expected row layout:
      0 id, 1 chat_jid, 2 sender, 3 content, 4 timestamp, 5 is_from_me,
      6 media_type, 7 chat_name, [8 filename], [9 file_length]
    Older callers may omit filename/file_length.
    """
    timestamp = row[4]
    media_type = row[6] or None
    filename = row[8] if len(row) > 8 else None
    file_length = row[9] if len(row) > 9 else None
    content = row[3] or ""
    # Make media-only rows obvious when caption is empty
    if media_type and not content:
        label = filename or media_type
        content = f"[{media_type}] {label}".strip()
    return {
        "id": row[0],
        "chat_jid": row[1],
        "sender": row[2],
        "content": content,
        "timestamp": timestamp.isoformat() if timestamp else None,
        "is_from_me": bool(row[5]) if row[5] is not None else None,
        "media_type": media_type,
        "filename": filename or None,
        "file_length": int(file_length) if file_length not in (None, "") else None,
        "chat_name": row[7],
        "sender_name": "Me" if row[5] else row[2],
        "role": role,
        "has_media": bool(media_type),
    }

def _fill_sender_names(cursor, messages: List[dict]) -> None:
    sender_ids = sorted({msg["sender"] for msg in messages if msg.get("sender") and not msg.get("is_from_me")})
    if not sender_ids:
        return
    cursor.execute(
        """
        SELECT sender_id, sender_name
        FROM (
            SELECT lm.pn AS sender_id,
                COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.first_name, ''), NULLIF(ct.push_name, ''), NULLIF(ct.business_name, '')) AS sender_name
            FROM whatsmeow_lid_map lm
            LEFT JOIN whatsmeow_contacts ct
              ON split_part(ct.their_jid, '@', 1) IN (lm.pn, lm.lid)
            WHERE lm.pn = ANY(%s::text[]) OR lm.lid = ANY(%s::text[])
            UNION ALL
            SELECT lm.lid,
                COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.first_name, ''), NULLIF(ct.push_name, ''), NULLIF(ct.business_name, ''))
            FROM whatsmeow_lid_map lm
            LEFT JOIN whatsmeow_contacts ct
              ON split_part(ct.their_jid, '@', 1) IN (lm.pn, lm.lid)
            WHERE lm.pn = ANY(%s::text[]) OR lm.lid = ANY(%s::text[])
            UNION ALL
            SELECT split_part(ct.their_jid, '@', 1),
                COALESCE(NULLIF(ct.full_name, ''), NULLIF(ct.first_name, ''), NULLIF(ct.push_name, ''), NULLIF(ct.business_name, ''))
            FROM whatsmeow_contacts ct
            WHERE split_part(ct.their_jid, '@', 1) = ANY(%s::text[])
        ) names
        WHERE coalesce(sender_name, '') <> ''
        """,
        (sender_ids, sender_ids, sender_ids, sender_ids, sender_ids),
    )
    names = {}
    for sender_id, sender_name in cursor.fetchall():
        names.setdefault(sender_id, sender_name)
    for msg in messages:
        if not msg.get("is_from_me") and names.get(msg.get("sender")):
            msg["sender_name"] = names[msg["sender"]]

def _attach_message_context(cursor, hits: List[dict], context_before: int, context_after: int) -> List[dict]:
    """Load neighbor lines for keyword hits in one query."""
    ids = [hit["id"] for hit in hits]
    chats = [hit["chat_jid"] for hit in hits]
    times = [as_dt(hit["timestamp"]) for hit in hits]
    cursor.execute(
        """
        WITH hits AS (
            SELECT * FROM unnest(%s::text[], %s::text[], %s::timestamptz[]) AS t(id, chat_jid, ts)
        )
        SELECT
            m.id,
            m.chat_jid,
            m.sender,
            m.content,
            m.timestamp,
            m.is_from_me,
            m.media_type,
            c.name,
            m.filename,
            m.file_length,
            CASE
                WHEN m.id = h.id THEN 'match'
                WHEN m.timestamp < h.ts THEN 'before'
                ELSE 'after'
            END AS role
        FROM hits h
        JOIN LATERAL (
            SELECT id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, file_length
            FROM messages
            WHERE chat_jid = h.chat_jid AND id = h.id
            UNION ALL
            SELECT id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, file_length
            FROM (
                SELECT id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, file_length
                FROM messages
                WHERE chat_jid = h.chat_jid AND timestamp < h.ts
                ORDER BY timestamp DESC
                LIMIT %s
            ) older
            UNION ALL
            SELECT id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, file_length
            FROM (
                SELECT id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, file_length
                FROM messages
                WHERE chat_jid = h.chat_jid AND timestamp > h.ts
                ORDER BY timestamp ASC
                LIMIT %s
            ) newer
        ) m ON true
        JOIN chats c ON c.jid = m.chat_jid
        ORDER BY h.ts DESC, m.timestamp ASC
        """,
        (ids, chats, times, max(0, context_before), max(0, context_after)),
    )
    messages = [_message_dict(row[:10], row[10]) for row in cursor.fetchall()]
    _fill_sender_names(cursor, messages)
    return messages

def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 30,
    page: int = 0,
    include_context: bool = False,
    context_before: int = 1,
    context_after: int = 1,
    media_type: Optional[str] = None,
    media_only: bool = False,
) -> dict:
    """Search message text and read older history in one call.

    chat_jid may be a chat JID, a phone number, or a contact/group name.
    query matches every word, in any order, against content OR filename.
    Pass before=next_before to read the next older page without calculating offsets.
    media_type filters image|video|audio|document|sticker.
    media_only=True returns only rows that have media.
    """
    limit = max(1, min(int(limit or 30), 80))
    page = max(0, int(page or 0))
    try:
        conn = connect()
        cursor = conn.cursor()
        where_clauses = []
        params = []
        resolved_chats: List[str] = []

        if after:
            where_clauses.append("messages.timestamp > %s")
            params.append(as_dt(after))
        if before:
            where_clauses.append("messages.timestamp < %s")
            params.append(as_dt(before))
        if sender_phone_number:
            sender_ids, direct_chats = lookup_parties(cursor, sender_phone_number)
            where_clauses.append("(messages.sender = ANY(%s::text[]) OR messages.chat_jid = ANY(%s::text[]))")
            params.extend([sender_ids or [""], direct_chats or [""]])
        if chat_jid:
            resolved_chats = resolve_chat_jids(cursor, chat_jid)
            if not resolved_chats:
                return {
                    "messages": [],
                    "count": 0,
                    "has_more": False,
                    "next_before": None,
                    "chats": [],
                    "hint": "Chat tidak ketemu. Persempit nama atau nomor lewat search_contacts.",
                }
            where_clauses.append("messages.chat_jid = ANY(%s::text[])")
            params.append(resolved_chats)
        if media_only:
            where_clauses.append("COALESCE(messages.media_type, '') <> ''")
        if media_type and media_type.strip():
            where_clauses.append("LOWER(COALESCE(messages.media_type, '')) = %s")
            params.append(media_type.strip().lower())
        if query and query.strip():
            words = [word for word in re.split(r"\s+", query.strip()) if word][:6]
            for word in words:
                # Match caption/content OR original filename so docs/images are findable
                where_clauses.append(
                    "(messages.content ILIKE %s ESCAPE '\\' OR COALESCE(messages.filename, '') ILIKE %s ESCAPE '\\')"
                )
                pattern = like_pattern(word)
                params.extend([pattern, pattern])

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        cursor.execute(
            f"""
            WITH hits AS MATERIALIZED (
                SELECT
                    messages.id,
                    messages.chat_jid,
                    messages.sender,
                    messages.content,
                    messages.timestamp,
                    messages.is_from_me,
                    messages.media_type,
                    chats.name AS chat_name,
                    messages.filename,
                    messages.file_length
                FROM messages
                JOIN chats ON chats.jid = messages.chat_jid
                {where_sql}
                ORDER BY messages.timestamp DESC
                LIMIT %s OFFSET %s
            )
            SELECT
                hits.id,
                hits.chat_jid,
                hits.sender,
                hits.content,
                hits.timestamp,
                hits.is_from_me,
                hits.media_type,
                hits.chat_name,
                hits.filename,
                hits.file_length
            FROM hits
            ORDER BY hits.timestamp DESC
            """,
            tuple(params) + (limit + 1, page * limit),
        )
        rows = cursor.fetchall()
        has_more = len(rows) > limit
        hits = [_message_dict(row) for row in rows[:limit]]
        _fill_sender_names(cursor, hits)
        hit_times = [hit["timestamp"] for hit in hits if hit.get("timestamp")]
        messages = hits
        if include_context and hits and (context_before or context_after):
            messages = _attach_message_context(cursor, hits, context_before, context_after)
        return {
            "messages": messages,
            "count": len(hits),
            "has_more": has_more,
            "next_before": min(hit_times) if hit_times else None,
            "chats": resolved_chats,
            "hint": "Untuk pesan lebih lama, panggil list_messages lagi dengan filter yang sama dan before=next_before.",
        }
    except ValueError:
        raise
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return {
            "messages": [],
            "count": 0,
            "has_more": False,
            "next_before": None,
            "chats": [],
            "hint": f"Database error: {e}",
        }
    finally:
        if 'conn' in locals():
            conn.close()


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = connect()
        cursor = conn.cursor()
        
        # Get the target message first
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = %s
        """, (message_id,))
        msg_data = cursor.fetchone()
        
        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")
            
        target_message = Message(
            timestamp=as_dt(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8]
        )
        
        # Get messages before
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = %s AND messages.timestamp < %s
            ORDER BY messages.timestamp DESC
            LIMIT %s
        """, (msg_data[7], msg_data[0], before))
        
        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(Message(
                timestamp=as_dt(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        # Get messages after
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = %s AND messages.timestamp > %s
            ORDER BY messages.timestamp ASC
            LIMIT %s
        """, (msg_data[7], msg_data[0], after))
        
        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(Message(
                timestamp=as_dt(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        return MessageContext(
            message=target_message,
            before=before_messages,
            after=after_messages
        )
        
    except psycopg.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    try:
        conn = connect()
        cursor = conn.cursor()
        
        # When include_last_message is False, do not reference messages.* without a JOIN.
        if include_last_message:
            query_parts = ["""
                SELECT
                    chats.jid,
                    chats.name,
                    chats.last_message_time,
                    messages.content as last_message,
                    messages.sender as last_sender,
                    messages.is_from_me as last_is_from_me
                FROM chats
                LEFT JOIN messages ON chats.jid = messages.chat_jid
                AND chats.last_message_time = messages.timestamp
            """]
        else:
            query_parts = ["""
                SELECT
                    chats.jid,
                    chats.name,
                    chats.last_message_time,
                    NULL as last_message,
                    NULL as last_sender,
                    NULL as last_is_from_me
                FROM chats
            """]

        where_clauses = []
        params = []

        if query:
            pattern = like_pattern(query)
            matched_chats = [
                item["chat_jid"]
                for item in search_contacts(query, limit=50)
                if item.get("chat_jid")
            ]
            where_clauses.append("(chats.name ILIKE %s ESCAPE '\\' OR chats.jid ILIKE %s ESCAPE '\\' OR chats.jid = ANY(%s::text[]))")
            params.extend([pattern, pattern, matched_chats or [""]])

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")

        # Add pagination
        offset = (page ) * limit
        query_parts.append("LIMIT %s OFFSET %s")
        params.extend([limit, offset])

        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=as_dt(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def search_contacts(query: str, limit: int = 20) -> List[dict]:
    """Find a person in one query using the contact book, LID map, and direct chat.

    Matches first name, full name, push name, business name, and phone number
    (08..., 62..., +62, spaces, or the internal LID). Each result carries the
    phone, the saved names, and the chat_jid to use for messages.
    """
    raw = (query or "").strip()
    if not raw:
        return []
    pattern = like_pattern(raw)
    variants = phone_variants(raw)
    suffixes = [value for value in variants if len(value) >= 8]
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            WITH linked AS (
                SELECT
                    NULLIF(ct.first_name, '') AS first_name,
                    NULLIF(ct.full_name, '') AS full_name,
                    NULLIF(ct.push_name, '') AS push_name,
                    NULLIF(ct.business_name, '') AS business_name,
                    CASE
                        WHEN ct.their_jid LIKE '%%@s.whatsapp.net' THEN split_part(ct.their_jid, '@', 1)
                        ELSE lm.pn
                    END AS phone,
                    CASE
                        WHEN ct.their_jid LIKE '%%@lid' THEN split_part(ct.their_jid, '@', 1)
                        ELSE lm.lid
                    END AS lid
                FROM whatsmeow_contacts ct
                LEFT JOIN whatsmeow_lid_map lm
                  ON lm.lid = split_part(ct.their_jid, '@', 1)
                  OR lm.pn = split_part(ct.their_jid, '@', 1)
                WHERE ct.their_jid NOT LIKE '%%@g.us'
                  AND ct.their_jid NOT LIKE '%%@bot'
            ),
            people AS (
                SELECT
                    phone,
                    lid,
                    max(first_name) AS first_name,
                    max(full_name) AS full_name,
                    max(push_name) AS push_name,
                    max(business_name) AS business_name
                FROM linked
                GROUP BY phone, lid
            ),
            ranked AS (
                SELECT
                    p.phone,
                    p.lid,
                    p.first_name,
                    p.full_name,
                    p.push_name,
                    p.business_name,
                    COALESCE(p.full_name, p.first_name, p.push_name, p.business_name, ch.name, p.phone, p.lid) AS name,
                    COALESCE(
                        ch.jid,
                        CASE WHEN p.lid IS NOT NULL THEN p.lid || '@lid' END,
                        CASE WHEN p.phone IS NOT NULL THEN p.phone || '@s.whatsapp.net' END
                    ) AS chat_jid,
                    COALESCE(
                        CASE WHEN p.phone IS NOT NULL THEN p.phone || '@s.whatsapp.net' END,
                        CASE WHEN p.lid IS NOT NULL THEN p.lid || '@lid' END
                    ) AS jid,
                    ch.last_message_time,
                    CASE
                        WHEN p.phone = ANY(%s::text[]) OR p.lid = ANY(%s::text[]) THEN 0
                        WHEN lower(coalesce(p.full_name, p.first_name, p.push_name, '')) = lower(%s) THEN 1
                        ELSE 2
                    END AS rank
                FROM people p
                LEFT JOIN LATERAL (
                    SELECT c.jid, c.name, c.last_message_time
                    FROM chats c
                    WHERE c.jid = p.lid || '@lid'
                       OR c.jid = p.phone || '@s.whatsapp.net'
                    ORDER BY c.last_message_time DESC NULLS LAST
                    LIMIT 1
                ) ch ON true
                WHERE p.full_name ILIKE %s ESCAPE '\\'
                   OR p.first_name ILIKE %s ESCAPE '\\'
                   OR p.push_name ILIKE %s ESCAPE '\\'
                   OR p.business_name ILIKE %s ESCAPE '\\'
                   OR ch.name ILIKE %s ESCAPE '\\'
                   OR p.phone = ANY(%s::text[])
                   OR p.lid = ANY(%s::text[])
                   OR (
                        cardinality(%s::text[]) > 0
                        AND p.phone LIKE ANY (SELECT '%%' || v FROM unnest(%s::text[]) AS v)
                   )
            )
            SELECT DISTINCT ON (coalesce(phone, lid, jid))
                phone, lid, first_name, full_name, push_name, business_name,
                name, chat_jid, jid, last_message_time, rank
            FROM ranked
            ORDER BY coalesce(phone, lid, jid), (phone IS NOT NULL AND lid IS NOT NULL) DESC, rank, last_message_time DESC NULLS LAST
            """,
            (
                variants, variants, raw,
                pattern, pattern, pattern, pattern, pattern,
                variants, variants, suffixes, suffixes,
            ),
        )
        picked = cursor.fetchall()
        picked.sort(key=lambda row: (row[10], row[9] is None, -(row[9].timestamp() if row[9] else 0)))
        result = []
        for row in picked[: max(1, limit)]:
            result.append({
                "phone_number": row[0],
                "lid": row[1],
                "first_name": row[2],
                "full_name": row[3],
                "push_name": row[4],
                "business_name": row[5],
                "name": row[6],
                "chat_jid": row[7],
                "jid": row[8],
                "last_message_time": row[9].isoformat() if row[9] else None,
            })
        return result
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = connect()
        cursor = conn.cursor()
        
        sender_ids, chat_jids = lookup_parties(cursor, jid)
        cursor.execute("""
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender = ANY(%s) OR c.jid = ANY(%s)
            ORDER BY c.last_message_time DESC
            LIMIT %s OFFSET %s
        """, (sender_ids or [jid], chat_jids or [jid], limit, page * limit))
        
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=as_dt(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_last_interaction(jid: str) -> str:
    """Get most recent message involving the contact."""
    try:
        conn = connect()
        cursor = conn.cursor()
        
        sender_ids, chat_jids = lookup_parties(cursor, jid)
        cursor.execute("""
            SELECT 
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender = ANY(%s) OR c.jid = ANY(%s)
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, (sender_ids or [jid], chat_jids or [jid]))
        
        msg_data = cursor.fetchone()
        
        if not msg_data:
            return None
            
        message = Message(
            timestamp=as_dt(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7]
        )
        
        return format_message(message)
        
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    try:
        conn = connect()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """
        
        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND c.last_message_time = m.timestamp
            """
            
        query += " WHERE c.jid = %s"
        
        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=as_dt(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except psycopg.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    """Open the direct chat for a phone number or contact name.

    Resolves 08.../62.../LID to the chat that actually stores the messages.
    """
    matches = search_contacts(sender_phone_number)
    direct = [item for item in matches if item.get("chat_jid") and not str(item["chat_jid"]).endswith("@g.us")]
    if not direct:
        return None
    return get_chat(direct[0]["chat_jid"], include_last_message=True)

def send_message(recipient: str, message: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "message": message,
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_file(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        bridge_path = media_path_for_bridge(media_path)
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": bridge_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_audio_message(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"

        bridge_path = media_path_for_bridge(media_path)
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": bridge_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def download_media(message_id: str, chat_jid: str) -> Optional[dict]:
    """Download media from a message and return host-readable metadata.

    Returns dict with file_path (Windows host path), media_type, filename,
    or None on failure.
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {
            "message_id": message_id,
            "chat_jid": chat_jid
        }
        
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                bridge_path = result.get("path")
                host_path = bridge_path_to_host(bridge_path)
                print(f"Media downloaded successfully: bridge={bridge_path} host={host_path}")
                return {
                    "file_path": host_path,
                    "bridge_path": bridge_path,
                    "filename": result.get("filename"),
                    "media_type": result.get("media_type") or result.get("type"),
                    "success": True,
                }
            else:
                print(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None
            
    except requests.RequestException as e:
        print(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None


@dataclass
class HistorySyncResult:
    """Result of a history sync request."""
    success: bool
    message: str
    request_accepted: bool = False
    chat_jid: Optional[str] = None
    anchor_message_id: Optional[str] = None
    anchor_timestamp: Optional[str] = None
    count: Optional[int] = None
    cooldown_seconds: Optional[int] = None


def request_history_sync(
    chat_jid: str,
    anchor_message_id: Optional[str] = None,
    count: int = 50
) -> HistorySyncResult:
    """Request on-demand history resync from WhatsApp primary device.
    
    This sends a request to the user's own primary phone (NOT to any contact/group)
    asking for additional message history for a specific chat. The response arrives
    asynchronously as an ON_DEMAND history sync event.
    
    Use this to:
    - Repair corrupted group message senders after the root fix is deployed
    - Fetch older messages not in the initial sync
    - Re-sync messages that may have been missed
    
    Args:
        chat_jid: The chat JID to resync (e.g., "120363429500369902@g.us" for group,
                  "6281234567890@s.whatsapp.net" for direct chat)
        anchor_message_id: Optional specific message ID to anchor from. If not provided,
                          the system auto-selects the newest valid anchor. For groups,
                          corrupted anchors (where sender=group_id) are rejected.
        count: Number of messages to request, bounded to 1-50. Default 50.
    
    Returns:
        HistorySyncResult with success status and details.
        
    Notes:
        - Has a 30-second cooldown between requests to prevent rate limiting
        - For groups, only valid anchors are accepted (self-messages or valid participants)
        - Response arrives async via history sync event handler
        - Uses SendPeerMessage (to phone) NOT SendMessage (to contacts)
    """
    # Validate inputs
    if not chat_jid:
        return HistorySyncResult(
            success=False,
            message="chat_jid is required"
        )
    
    # Validate JID format
    if "@" not in chat_jid:
        return HistorySyncResult(
            success=False,
            message="Invalid chat_jid format. Must be a full JID like 'number@s.whatsapp.net' or 'id@g.us'"
        )
    
    # Bound count to 1-50
    count = max(1, min(50, count))
    
    try:
        url = f"{WHATSAPP_API_BASE_URL}/history-sync"
        payload = {
            "chat_jid": chat_jid,
            "count": count,
        }
        
        if anchor_message_id:
            payload["anchor_message_id"] = anchor_message_id
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return HistorySyncResult(
                success=result.get("success", False),
                message=result.get("message", "Unknown response"),
                request_accepted=result.get("request_accepted", False),
                chat_jid=result.get("chat_jid"),
                anchor_message_id=result.get("anchor_message_id"),
                anchor_timestamp=result.get("anchor_timestamp"),
                count=result.get("count"),
                cooldown_seconds=result.get("cooldown_seconds")
            )
        elif response.status_code == 429:
            # Rate limited / cooldown
            result = response.json()
            return HistorySyncResult(
                success=False,
                message=result.get("message", "Rate limited"),
                cooldown_seconds=result.get("cooldown_seconds")
            )
        else:
            error_msg = f"HTTP {response.status_code}"
            try:
                result = response.json()
                error_msg = result.get("message", error_msg)
            except json.JSONDecodeError:
                error_msg = f"{error_msg} - {response.text}"
            return HistorySyncResult(
                success=False,
                message=f"Error: {error_msg}"
            )
            
    except requests.RequestException as e:
        return HistorySyncResult(
            success=False,
            message=f"Request error: {str(e)}"
        )
    except Exception as e:
        return HistorySyncResult(
            success=False,
            message=f"Unexpected error: {str(e)}"
        )


def get_corrupted_group_message_count(chat_jid: str) -> dict:
    """Check how many messages in a group have a corrupted sender (sender = group id).

    Counts only; never returns message content.
    """
    if not chat_jid:
        return {"error": "chat_jid is required"}
    if not chat_jid.endswith("@g.us"):
        return {
            "chat_jid": chat_jid,
            "is_group": False,
            "error": "Not a group chat - no sender corruption possible",
        }
    group_user = chat_jid.split("@")[0]
    try:
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT COUNT(*),
                              COUNT(*) FILTER (WHERE sender = %s AND NOT COALESCE(is_from_me, false)),
                              COUNT(*) FILTER (WHERE COALESCE(is_from_me, false))
                       FROM messages WHERE chat_jid = %s""",
                    (group_user, chat_jid),
                )
                total, corrupted, from_me = cursor.fetchone()
        return {
            "chat_jid": chat_jid,
            "is_group": True,
            "total_messages": total,
            "corrupted_count": corrupted,
            "valid_count": total - corrupted,
            "from_me_count": from_me,
            "corruption_percentage": round(corrupted / total * 100, 1) if total > 0 else 0,
        }
    except psycopg.Error as e:
        return {"error": f"Database error: {e}"}
