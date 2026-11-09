from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
    list_messages as whatsapp_list_messages,
    list_chats as whatsapp_list_chats,
    get_chat as whatsapp_get_chat,
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
    get_contact_chats as whatsapp_get_contact_chats,
    get_last_interaction as whatsapp_get_last_interaction,
    get_message_context as whatsapp_get_message_context,
    send_message as whatsapp_send_message,
    send_file as whatsapp_send_file,
    send_audio_message as whatsapp_audio_voice_message,
    download_media as whatsapp_download_media,
    request_history_sync as whatsapp_request_history_sync,
    get_corrupted_group_message_count as whatsapp_get_corrupted_count,
)

# Initialize FastMCP server
mcp = FastMCP("whatsapp")

@mcp.tool()
def search_contacts(query: str) -> List[Dict[str, Any]]:
    """Find a WhatsApp contact in one call by name or phone number.

    Matches first name, full name, push name, business name, and phone
    (08..., 62..., +62, spaces or dashes, or the internal LID).
    Each result includes phone_number, first_name, full_name, push_name,
    lid, jid, and chat_jid. Use chat_jid for messages or sending.
    A second lookup is only needed when several people share the name.

    Args:
        query: Name fragment or phone number
    """
    contacts = whatsapp_search_contacts(query)
    return contacts

@mcp.tool()
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
) -> Dict[str, Any]:
    """Search chat keywords and read older messages in one call.

    chat_jid accepts a chat JID, a phone number (08... or 62...), or a
    contact/group name. query matches every word against content OR filename.
    Each message may include media_type, filename, file_length, has_media.
    Media-only rows without caption show content like "[image] filename.jpg".
    To open the file/image, call download_media(message_id, chat_jid) then Read the returned file_path.
    The result includes messages, has_more, and next_before.
    To read older history, call again with the same filters and before=next_before.
    Do not walk history with page offsets.

    Args:
        after: Only messages after this ISO-8601 timestamp
        before: Only messages before this ISO-8601 timestamp. Use next_before from the previous result.
        sender_phone_number: Phone number, 08.../62... form, or LID
        chat_jid: Chat JID, phone number, or contact/group name
        query: Keyword or words to find in message text or filename
        limit: Messages per page, 1 to 80 (default 30)
        page: Offset page. Prefer before=next_before instead.
        include_context: When true, keyword hits include neighboring lines in the same response
        context_before: Lines before each hit when include_context is true
        context_after: Lines after each hit when include_context is true
        media_type: Optional filter: image, video, audio, document, or sticker
        media_only: When true, return only messages that have media
    """
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after,
        media_type=media_type,
        media_only=media_only,
    )
    return messages

@mcp.tool()
def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.
    
    Args:
        query: Name, phone, or JID. Name search also uses the contact book, not only the chat title.
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
        include_last_message: Whether to include the last message in each chat (default True)
        sort_by: Field to sort results by, either "last_active" or "name" (default "last_active")
    """
    chats = whatsapp_list_chats(
        query=query,
        limit=limit,
        page=page,
        include_last_message=include_last_message,
        sort_by=sort_by
    )
    return chats

@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> Dict[str, Any]:
    """Get WhatsApp chat metadata by JID.
    
    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat

@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> Dict[str, Any]:
    """Open one direct chat by phone number or contact name.

    Resolves the phone or LID to the chat that stores the messages.
    If the name matches several people, call search_contacts instead.

    Args:
        sender_phone_number: Phone number (08... or 62...) or contact name
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat

@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats

@mcp.tool()
def get_last_interaction(jid: str) -> str:
    """Get most recent WhatsApp message involving the contact.
    
    Args:
        jid: The JID of the contact to search for
    """
    message = whatsapp_get_last_interaction(jid)
    return message

@mcp.tool()
def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> Dict[str, Any]:
    """Get context around a specific WhatsApp message.
    
    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return context

@mcp.tool()
def send_message(
    recipient: str,
    message: str
) -> Dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send
    
    Returns:
        A dictionary containing success status and a status message
    """
    # Validate input
    if not recipient:
        return {
            "success": False,
            "message": "Recipient must be provided"
        }
    
    # Call the whatsapp_send_message function with the unified recipient parameter
    success, status_message = whatsapp_send_message(recipient, message)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_file(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document)
    
    Returns:
        A dictionary containing success status and a status message
    """
    
    # Call the whatsapp_send_file function
    success, status_message = whatsapp_send_file(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_audio_message(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send any audio file as a WhatsApp audio message to the specified recipient. For group messages use the JID. If it errors due to ffmpeg not being installed, use send_file instead.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send (will be converted to Opus .ogg if it's not a .ogg file)
    
    Returns:
        A dictionary containing success status and a status message
    """
    success, status_message = whatsapp_audio_voice_message(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> Dict[str, Any]:
    """Download media from a WhatsApp message to a host path Cursor can Read.

    Use after list_messages shows has_media/media_type. Pass the message id and
    chat_jid from that row. Returns a Windows host file_path under
    whatsapp-bridge/store/ suitable for the Read tool (images/docs).

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        success, message, file_path (host), filename, media_type
    """
    result = whatsapp_download_media(message_id, chat_jid)
    
    if result and result.get("file_path"):
        return {
            "success": True,
            "message": "Media downloaded successfully",
            "file_path": result.get("file_path"),
            "filename": result.get("filename"),
            "media_type": result.get("media_type"),
            "bridge_path": result.get("bridge_path"),
        }
    else:
        return {
            "success": False,
            "message": "Failed to download media"
        }


@mcp.tool()
def request_history_sync(
    chat_jid: str,
    anchor_message_id: Optional[str] = None,
    count: int = 50
) -> Dict[str, Any]:
    """Request on-demand message history resync from WhatsApp primary device.
    
    Sends a request to your own phone asking for older messages in a chat.
    This is useful to:
    - Repair corrupted group message senders after the root fix is deployed
    - Fetch older messages not in the initial sync
    - Re-sync messages that may have been missed
    
    The response arrives asynchronously via the history sync handler.
    
    IMPORTANT: This sends a protocol message to YOUR OWN primary phone,
    NOT to any contact or group. The phone will respond with ON_DEMAND
    history sync data.
    
    Args:
        chat_jid: The chat JID to resync (e.g., "120363429500369902@g.us" for group,
                  "6281234567890@s.whatsapp.net" for direct chat)
        anchor_message_id: Optional specific message ID to anchor from. If not provided,
                          the system auto-selects the newest valid anchor. For groups,
                          corrupted anchors (sender=group_id) are rejected.
        count: Number of messages to request, bounded 1-50. Default 50.
    
    Returns:
        Dict with success, message, request_accepted, chat_jid, anchor_message_id,
        anchor_timestamp, count, and cooldown_seconds (if rate limited)
    
    Notes:
        - Has a 30-second cooldown between requests
        - For groups, only valid anchors are accepted
        - Response arrives async; check messages later
    """
    result = whatsapp_request_history_sync(chat_jid, anchor_message_id, count)
    
    return {
        "success": result.success,
        "message": result.message,
        "request_accepted": result.request_accepted,
        "chat_jid": result.chat_jid,
        "anchor_message_id": result.anchor_message_id,
        "anchor_timestamp": result.anchor_timestamp,
        "count": result.count,
        "cooldown_seconds": result.cooldown_seconds
    }


@mcp.tool()
def get_corrupted_group_message_count(chat_jid: str) -> Dict[str, Any]:
    """Check how many messages in a group have corrupted sender (sender=group_id).
    
    This helps assess the extent of the history sync sender corruption bug
    before and after running history resync repair.
    
    Args:
        chat_jid: The group chat JID to check (e.g., "120363429500369902@g.us")
    
    Returns:
        Dict with: chat_jid, is_group, total_messages, corrupted_count,
        valid_count, from_me_count, corruption_percentage
    """
    return whatsapp_get_corrupted_count(chat_jid)


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')