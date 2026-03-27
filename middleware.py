from flask import request, jsonify
import time
import logging
from functools import wraps
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Simple rate limiting
rate_limit_store = {}

def rate_limit(max_requests=10, window_seconds=60):
    """Rate limiting decorator"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR', 'unknown'))
            current_time = time.time()
            
            # Clean old entries
            cutoff_time = current_time - window_seconds
            rate_limit_store[client_ip] = [req_time for req_time in rate_limit_store.get(client_ip, []) if req_time > cutoff_time]
            
            # Check limit
            if len(rate_limit_store.get(client_ip, [])) >= max_requests:
                logger.warning(f"Rate limit exceeded for IP: {client_ip}")
                return jsonify({'error': 'Rate limit exceeded. Please try again later.'}), 429
            
            # Add current request
            rate_limit_store.setdefault(client_ip, []).append(current_time)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def extract_channel_id(channel_input):
    """Extract channel ID from various input formats"""
    if not channel_input:
        return None
    
    channel_input = channel_input.strip()
    
    # If it's already a valid channel ID format, return as-is
    if channel_input.startswith('@') or channel_input.startswith('-100') or channel_input.lstrip('-').isdigit():
        return channel_input
    
    # Handle private channel IDs that start with +
    if channel_input.startswith('+'):
        # For Telethon, we need to construct the full t.me URL
        return f'https://t.me/{channel_input}'
    
    # Handle telegram.me/t.me links
    if 'telegram.me/' in channel_input or 't.me/' in channel_input:
        # Extract username from URL (including + sign for private channels)
        match = re.search(r'(?:telegram\.me|t\.me)/([^/?]+)', channel_input)
        if match:
            identifier = match.group(1)
            # Remove any trailing slashes or query params
            identifier = identifier.split('/')[0].split('?')[0]
            # For private channels with + sign, Telethon needs the full URL
            if identifier.startswith('+'):
                return channel_input  # Return the full URL for Telethon
            else:
                return f'@{identifier}'
    
    # Handle invite links (t.me/joinchat/)
    if 'joinchat/' in channel_input:
        # Extract invite hash
        match = re.search(r'joinchat/([^/?]+)', channel_input)
        if match:
            # For invite links, we need to return the full join URL
            # as Telethon can handle these directly
            return channel_input
    
    # If it's just a username without @, add it (including + sign for private channels)
    if re.match(r'^[+a-zA-Z0-9_]{3,32}$', channel_input):
        return f'@{channel_input}'
    
    return None

def validate_channel_id(f):
    """Validate channel ID format"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        data = request.get_json()
        if not data or 'channel_id' not in data:
            return jsonify({'error': 'channel_id is required'}), 400
        
        channel_input = data['channel_id'].strip()
        
        # Basic validation
        if not channel_input:
            return jsonify({'error': 'channel_id cannot be empty'}), 400
        
        # Extract and validate channel ID
        extracted_id = extract_channel_id(channel_input)
        if not extracted_id:
            return jsonify({
                'error': 'Invalid channel format. Use:\n' +
                       '- @channelname\n' +
                       '- @+privatechannelid (for private channels)\n' +
                       '- Channel ID (e.g., -1001234567890)\n' +
                       '- Telegram link (e.g., https://t.me/channelname)\n' +
                       '- Private channel link (e.g., https://t.me/+ABCDEF)\n' +
                       '- Invite link (e.g., https://t.me/joinchat/ABCDEF)'
            }), 400
        
        # Replace the channel_id with the extracted ID
        data['channel_id'] = extracted_id
        
        return f(*args, **kwargs)
    return decorated_function
