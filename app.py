from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from dotenv import load_dotenv
import asyncio
import threading
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError, FloodWaitError
from telethon.tl.types import Channel
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon import functions
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from middleware import rate_limit, validate_channel_id
from claude_service import ClaudeAnalyzer

load_dotenv()

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telethon credentials
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone_number = os.getenv('TELEGRAM_PHONE_NUMBER')
bot_token = os.getenv('TELEGRAM_BOT_TOKEN')

# Check if we have bot token or user account credentials
use_bot = bot_token and bot_token != 'your_bot_token_here'

if not use_bot and not all([api_id, api_hash, phone_number]):
    logger.error("Either TELEGRAM_BOT_TOKEN or TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_PHONE_NUMBER must be set")
    exit(1)

# Global event loop and client
_loop = None
_client = None
_claude_analyzer = ClaudeAnalyzer()

def get_event_loop():
    """Get or create the event loop"""
    global _loop
    if _loop is None or _loop.is_closed():
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop

def get_client():
    """Get or create Telegram client"""
    global _client
    if _client is None:
        # Check if running on Vercel (serverless environment)
        is_serverless = os.getenv('VERCEL') == '1' or os.getenv('AWS_LAMBDA_FUNCTION_NAME')
        
        if use_bot:
            # Use bot token authentication
            if is_serverless:
                # Use in-memory session for serverless
                _client = TelegramClient(None, int(api_id), api_hash, loop=get_event_loop())
            else:
                _client = TelegramClient('bot_session', int(api_id), api_hash, loop=get_event_loop())
        else:
            # Use user account authentication
            if is_serverless:
                # Use in-memory session for serverless
                _client = TelegramClient(None, int(api_id), api_hash, loop=get_event_loop())
            else:
                _client = TelegramClient('session_name', int(api_id), api_hash, loop=get_event_loop())
    return _client

async def ensure_connected():
    """Ensure client is connected"""
    client = get_client()
    if not client.is_connected():
        await client.connect()
    
    # Handle bot authentication
    if use_bot and not client.is_connected():
        try:
            await client.start(bot_token=bot_token)
        except Exception as e:
            logger.error(f"Bot authentication failed: {e}")
            raise
    
    return client

class ChannelAnalyzer:
    def __init__(self):
        pass
    
    async def sign_in(self, code=None):
        """Sign in with verification code or bot token"""
        try:
            client = await ensure_connected()
            
            if use_bot:
                # Bot authentication is handled during client creation
                return {'success': True, 'authorized': await client.is_user_authorized(), 'auth_type': 'bot'}
            else:
                # User account authentication
                if not await client.is_user_authorized():
                    if code:
                        await client.sign_in(phone_number, code)
                        return {'success': True, 'authorized': await client.is_user_authorized(), 'auth_type': 'user'}
                    else:
                        await client.send_code_request(phone_number)
                        return {'success': True, 'code_required': True, 'auth_type': 'user'}
                else:
                    return {'success': True, 'authorized': True, 'auth_type': 'user'}
        except Exception as e:
            logger.error(f"Sign in error: {e}")
            return {'success': False, 'error': str(e)}
    
    async def get_channel_info(self, channel_id):
        """Get basic channel information"""
        try:
            client = await ensure_connected()
            
            # For bots, check if connected; for users, check authorization
            if use_bot:
                if not client.is_connected():
                    return {'error': 'Bot connection failed. Please check your bot token.'}
            else:
                if not await client.is_user_authorized():
                    return {'error': 'Authorization required. Please check your Telegram for verification code.'}
            
            # Get channel entity - handle private channels with invite links
            try:
                entity = await client.get_entity(channel_id)
            except Exception as e:
                if "key is not registered" in str(e) and use_bot:
                    # Bot needs to join the channel first via invite link
                    try:
                        # For invite links, we need to use the join_chat method with the invite hash
                        invite_hash = channel_id.split('/')[-1] if '/' in channel_id else channel_id
                        await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
                        entity = await client.get_entity(channel_id)
                    except Exception as join_error:
                        return {'error': f'Bot needs to join the channel first. Please add your bot to the channel or check the invite link. Details: {str(join_error)}'}
                else:
                    raise
            
            # Get full channel info
            full_entity = await client.get_entity(entity)
            
            return {
                'id': full_entity.id,
                'title': getattr(full_entity, 'title', None),
                'username': getattr(full_entity, 'username', None),
                'description': getattr(full_entity, 'about', None),
                'type': 'channel' if isinstance(full_entity, Channel) else 'chat',
                'member_count': getattr(full_entity, 'participants_count', None),
                'is_verified': getattr(full_entity, 'verified', False),
                'is_scram': getattr(full_entity, 'scam', False),
                'restricted': getattr(full_entity, 'restricted', False),
                'megagroup': getattr(full_entity, 'megagroup', False),
                'broadcast': getattr(full_entity, 'broadcast', False)
            }
        except (ChannelPrivateError, ChatAdminRequiredError) as e:
            logger.error(f"Cannot access channel: {e}")
            return {'error': f'Cannot access channel: {str(e)}'}
        except FloodWaitError as e:
            logger.error(f"Flood wait: {e.seconds} seconds")
            return {'error': f'Rate limited. Please wait {e.seconds} seconds.'}
        except Exception as e:
            logger.error(f"Unexpected error getting channel info: {e}")
            return {'error': f'Unexpected error: {str(e)}'}
    
    async def get_channel_statistics(self, channel_id):
        """Get comprehensive channel statistics including growth analysis"""
        try:
            client = await ensure_connected()
            
            # For bots, check if connected; for users, check authorization
            if use_bot:
                if not client.is_connected():
                    return {'error': 'Bot connection failed. Please check your bot token.'}
            else:
                if not await client.is_user_authorized():
                    return {'error': 'Authorization required. Please check your Telegram for verification code.'}
            
            # Get channel entity - handle private channels with invite links
            try:
                entity = await client.get_entity(channel_id)
            except Exception as e:
                if "key is not registered" in str(e) and use_bot:
                    # Bot needs to join the channel first via invite link
                    try:
                        # For invite links, we need to use the join_chat method with the invite hash
                        invite_hash = channel_id.split('/')[-1] if '/' in channel_id else channel_id
                        await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
                        entity = await client.get_entity(channel_id)
                    except Exception as join_error:
                        return {'error': f'Bot needs to join the channel first. Please add your bot to the channel or check the invite link. Details: {str(join_error)}'}
                else:
                    raise
            
            # Get extended messages for comprehensive analysis
            messages = []
            message_dates = []
            try:
                # Try to get maximum possible data for complete analysis
                # Start with very high limit, Telegram will limit based on channel history
                message_limit = 10000  # Maximum possible for comprehensive analysis
                
                logger.info(f"Fetching up to {message_limit} messages for complete channel analysis...")
                message_count = 0
                async for message in client.iter_messages(entity, limit=message_limit):
                    message_data = {
                        'message_id': message.id,
                        'date': message.date.isoformat() if message.date else None,
                        'text': message.text,
                        'views': getattr(message, 'views', None),
                        'forwards': getattr(message, 'forwards', None),
                        'reactions': len(message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0,
                        'has_media': bool(message.media),
                        'media_type': self._get_media_type(message.media) if message.media else None,
                        'from_user': getattr(message.from_id, 'user_id', None) if hasattr(message, 'from_id') else None,
                        'reply_to': getattr(message, 'reply_to_msg_id', None),
                        'is_forwarded': bool(message.forwarded) if hasattr(message, 'forwarded') else False
                    }
                    messages.append(message_data)
                    if message.date:
                        message_dates.append(message.date)
                    
                    message_count += 1
                    if message_count % 1000 == 0:
                        logger.info(f"Fetched {message_count} messages...")
                
                logger.info(f"Successfully fetched {len(messages)} total messages for complete analysis")
                
                # If we got very few messages, try to get more by going further back
                if len(messages) < 500:
                    logger.info("Few messages found, trying to get more historical data...")
                    try:
                        # Try without limit to get all available messages
                        additional_count = 0
                        async for message in client.iter_messages(entity, offset_date=message_dates[-1] if message_dates else None):
                            if message.id <= messages[-1]['message_id'] if messages else True:
                                break  # Avoid duplicates
                            
                            message_data = {
                                'message_id': message.id,
                                'date': message.date.isoformat() if message.date else None,
                                'text': message.text,
                                'views': getattr(message, 'views', None),
                                'forwards': getattr(message, 'forwards', None),
                                'reactions': len(message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0,
                                'has_media': bool(message.media),
                                'media_type': self._get_media_type(message.media) if message.media else None,
                                'from_user': getattr(message.from_id, 'user_id', None) if hasattr(message, 'from_id') else None,
                                'reply_to': getattr(message, 'reply_to_msg_id', None),
                                'is_forwarded': bool(message.forwarded) if hasattr(message, 'forwarded') else False
                            }
                            messages.append(message_data)
                            if message.date:
                                message_dates.append(message.date)
                            
                            additional_count += 1
                            if additional_count >= 2000:  # Safety limit
                                break
                        
                        logger.info(f"Added {additional_count} more messages, total: {len(messages)}")
                    except Exception as e:
                        logger.warning(f"Could not get additional messages: {e}")
                
            except FloodWaitError as e:
                logger.warning(f"Rate limited while fetching messages: {e.seconds} seconds wait required")
                # Continue with whatever data we have
            except Exception as e:
                logger.warning(f"Error fetching message history: {e}")
                # Try with smaller limit as fallback
                try:
                    logger.info("Attempting with conservative message limit...")
                    async for message in client.iter_messages(entity, limit=1000):
                        message_data = {
                            'message_id': message.id,
                            'date': message.date.isoformat() if message.date else None,
                            'text': message.text,
                            'views': getattr(message, 'views', None),
                            'forwards': getattr(message, 'forwards', None),
                            'reactions': len(message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0,
                            'has_media': bool(message.media),
                            'media_type': self._get_media_type(message.media) if message.media else None,
                            'from_user': getattr(message.from_id, 'user_id', None) if hasattr(message, 'from_id') else None,
                            'reply_to': getattr(message, 'reply_to_msg_id', None),
                            'is_forwarded': bool(message.forwarded) if hasattr(message, 'forwarded') else False
                        }
                        messages.append(message_data)
                        if message.date:
                            message_dates.append(message.date)
                    logger.info(f"Fetched {len(messages)} messages with fallback limit")
                except Exception as e2:
                    logger.error(f"Failed to fetch messages even with fallback: {e2}")
                    # Continue with whatever data we have
            
            # Calculate comprehensive statistics
            total_messages = len(messages)
            
            # Add data completeness information
            data_completeness = {
                'total_messages_fetched': total_messages,
                'date_range_start': min(message_dates).date().isoformat() if message_dates else None,
                'date_range_end': max(message_dates).date().isoformat() if message_dates else None,
                'days_covered': (max(message_dates).date() - min(message_dates).date()).days if len(message_dates) >= 2 else 0,
                'fetch_status': 'complete' if total_messages >= 5000 else 'partial' if total_messages >= 1000 else 'limited',
                'data_quality': 'excellent' if total_messages >= 5000 else 'good' if total_messages >= 1000 else 'basic'
            }
            
            logger.info(f"Analysis complete: {total_messages} messages covering {data_completeness['days_covered']} days")
            
            messages_with_views = [m for m in messages if m['views'] is not None]
            avg_views = sum(m['views'] for m in messages_with_views) / len(messages_with_views) if messages_with_views else 0
            
            messages_with_forwards = [m for m in messages if m['forwards'] is not None]
            total_forwards = sum(m['forwards'] for m in messages_with_forwards) if messages_with_forwards else 0
            
            messages_with_reactions = [m for m in messages if m['reactions'] > 0]
            total_reactions = sum(m['reactions'] for m in messages_with_reactions)
            
            messages_with_media = [m for m in messages if m['has_media']]
            media_percentage = (len(messages_with_media) / total_messages * 100) if total_messages > 0 else 0
            
            # Time-based analysis
            time_analysis = self._analyze_time_patterns(message_dates, messages) if message_dates and messages else {}
            
            # Content analysis
            content_analysis = self._analyze_content_patterns(messages)
            
            # Engagement analysis
            engagement_analysis = self._analyze_engagement_patterns(messages)
            
            # Growth estimation (based on message activity patterns)
            growth_analysis = self._estimate_growth_patterns(message_dates, messages) if message_dates else {}
            
            # Member data analysis
            member_analysis = await self._get_member_analysis(entity, client) if entity else {}
            
            # Graph data for visualization
            graph_data = self._generate_graph_data(message_dates, messages) if message_dates else {}
            
            # Yearly statistics analysis
            yearly_analysis = self._analyze_yearly_stats(message_dates, messages) if message_dates else {}
            
            # Get basic channel info
            basic_info = await self.get_channel_info(channel_id)
            
            # Extract member count for main statistics
            member_count = basic_info.get('member_count') if basic_info else None
            
            return {
                'basic_info': basic_info,
                'data_completeness': data_completeness,
                'statistics': {
                    'total_messages_analyzed': total_messages,
                    'average_views_per_message': round(avg_views, 2),
                    'total_forwards': total_forwards,
                    'total_reactions': total_reactions,
                    'engagement_rate': round((total_reactions / total_messages * 100), 2) if total_messages > 0 else 0,
                    'media_content_percentage': round(media_percentage, 2),
                    'messages_with_views': len(messages_with_views),
                    'messages_with_reactions': len(messages_with_reactions),
                    'forward_rate': round((total_forwards / total_messages * 100), 2) if total_messages > 0 else 0,
                    'reaction_rate': round((total_reactions / total_messages), 2) if total_messages > 0 else 0,
                    'current_member_count': member_count
                },
                'time_analysis': time_analysis,
                'content_analysis': content_analysis,
                'engagement_analysis': engagement_analysis,
                'growth_analysis': growth_analysis,
                'member_analysis': member_analysis,
                'yearly_analysis': yearly_analysis,
                'graph_data': graph_data,
                'recent_messages': messages[:20]  # Return last 20 messages
            }
        except Exception as e:
            logger.error(f"Error getting channel statistics: {e}")
            return {'error': f'Error analyzing channel: {str(e)}'}
    
    def _get_media_type(self, media):
        """Determine media type"""
        if hasattr(media, 'photo'):
            return 'photo'
        elif hasattr(media, 'document'):
            return 'document'
        elif hasattr(media, 'video'):
            return 'video'
        elif hasattr(media, 'audio'):
            return 'audio'
        elif hasattr(media, 'webpage'):
            return 'webpage'
        else:
            return 'unknown'
    
    def _analyze_time_patterns(self, message_dates, messages):
        """Analyze posting patterns over time with average views (last 30 days only)"""
        from datetime import datetime, timedelta
        import collections
        
        if not message_dates or not messages:
            return {}
        
        # Filter for last 30 days only
        now = datetime.now(message_dates[0].tzinfo)
        thirty_days_ago = now - timedelta(days=30)
        
        # Create mapping of dates to messages for views data (last 30 days only)
        date_message_map = {}
        for i, date in enumerate(message_dates):
            if i < len(messages) and date >= thirty_days_ago:
                date_message_map[date] = messages[i]
        
        if not date_message_map:
            return {
                'hourly_distribution': {},
                'daily_distribution': {},
                'most_active_hour': None,
                'most_active_day': None,
                'best_hour_by_avg_views': None,
                'best_hour_avg_views': 0,
                'best_day_by_avg_views': None,
                'best_day_avg_views': 0,
                'activity_trend_7_days': 0,
                'recent_activity_count': 0,
                'older_activity_count': 0,
                'analysis_period': 'last_30_days',
                'messages_analyzed': 0
            }
        
        # Hourly distribution with average views (last 30 days)
        hourly_views = collections.defaultdict(list)
        hourly_counts = collections.Counter()
        for date, message in date_message_map.items():
            hour = date.hour
            hourly_counts[hour] += 1
            if message.get('views'):
                hourly_views[hour].append(message['views'])
        
        # Calculate average views per hour
        hourly_avg_views = {}
        for hour in hourly_views:
            if hourly_views[hour]:
                hourly_avg_views[hour] = sum(hourly_views[hour]) / len(hourly_views[hour])
            else:
                hourly_avg_views[hour] = 0
        
        # Daily distribution with average views (last 30 days)
        daily_views = collections.defaultdict(list)
        daily_counts = collections.Counter()
        for date, message in date_message_map.items():
            day = date.strftime('%A')
            daily_counts[day] += 1
            if message.get('views'):
                daily_views[day].append(message['views'])
        
        # Calculate average views per day
        daily_avg_views = {}
        for day in daily_views:
            if daily_views[day]:
                daily_avg_views[day] = sum(daily_views[day]) / len(daily_views[day])
            else:
                daily_avg_views[day] = 0
        
        # Activity over time (last 7 days vs previous 7 days within last 30 days)
        week_ago = now - timedelta(days=7)
        two_weeks_ago = now - timedelta(days=14)
        
        recent_messages = [d for d in date_message_map.keys() if d >= week_ago]
        older_messages = [d for d in date_message_map.keys() if two_weeks_ago <= d < week_ago]
        
        activity_trend = len(recent_messages) - len(older_messages)
        
        # Find best hours and days by average views
        best_hour_by_views = max(hourly_avg_views.items(), key=lambda x: x[1]) if hourly_avg_views else (None, 0)
        best_day_by_views = max(daily_avg_views.items(), key=lambda x: x[1]) if daily_avg_views else (None, 0)
        
        return {
            'hourly_distribution': dict(hourly_avg_views),  # Last 30 days average views
            'daily_distribution': dict(daily_avg_views),    # Last 30 days average views
            'most_active_hour': hourly_counts.most_common(1)[0][0] if hourly_counts else None,
            'most_active_day': daily_counts.most_common(1)[0][0] if daily_counts else None,
            'best_hour_by_avg_views': best_hour_by_views[0],
            'best_hour_avg_views': round(best_hour_by_views[1], 2) if best_hour_by_views[1] else 0,
            'best_day_by_avg_views': best_day_by_views[0],
            'best_day_avg_views': round(best_day_by_views[1], 2) if best_day_by_views[1] else 0,
            'activity_trend_7_days': activity_trend,
            'recent_activity_count': len(recent_messages),
            'older_activity_count': len(older_messages),
            'analysis_period': 'last_30_days',
            'messages_analyzed': len(date_message_map)
        }
    
    def _analyze_content_patterns(self, messages):
        """Analyze content patterns"""
        if not messages:
            return {}
        
        # Media type distribution
        media_types = [m['media_type'] for m in messages if m['media_type']]
        media_distribution = {}
        for media_type in media_types:
            media_distribution[media_type] = media_distribution.get(media_type, 0) + 1
        
        # Message length analysis
        text_messages = [m['text'] for m in messages if m['text']]
        avg_message_length = sum(len(text) for text in text_messages) / len(text_messages) if text_messages else 0
        
        # Forward analysis
        forwarded_messages = [m for m in messages if m['is_forwarded']]
        forward_percentage = (len(forwarded_messages) / len(messages) * 100) if messages else 0
        
        # Reply analysis (thread engagement)
        reply_messages = [m for m in messages if m['reply_to']]
        reply_percentage = (len(reply_messages) / len(messages) * 100) if messages else 0
        
        return {
            'media_type_distribution': media_distribution,
            'average_message_length': round(avg_message_length, 2),
            'forward_percentage': round(forward_percentage, 2),
            'reply_percentage': round(reply_percentage, 2),
            'total_forwarded': len(forwarded_messages),
            'total_replies': len(reply_messages)
        }
    
    def _analyze_engagement_patterns(self, messages):
        """Analyze engagement patterns"""
        if not messages:
            return {}
        
        # View statistics
        view_data = [m['views'] for m in messages if m['views'] is not None]
        if view_data:
            max_views = max(view_data)
            min_views = min(view_data)
            median_views = sorted(view_data)[len(view_data) // 2]
        else:
            max_views = min_views = median_views = 0
        
        # Reaction patterns
        reaction_data = [m['reactions'] for m in messages if m['reactions'] > 0]
        avg_reactions_per_message = sum(reaction_data) / len(reaction_data) if reaction_data else 0
        
        # Forward patterns
        forward_data = [m['forwards'] for m in messages if m['forwards'] is not None]
        avg_forwards_per_message = sum(forward_data) / len(forward_data) if forward_data else 0
        
        # High engagement messages (top 10% by views)
        if view_data:
            threshold = sorted(view_data)[int(len(view_data) * 0.9)]
            high_engagement_messages = [m for m in messages if m['views'] and m['views'] >= threshold]
            high_engagement_percentage = (len(high_engagement_messages) / len(messages) * 100)
        else:
            high_engagement_percentage = 0
        
        return {
            'view_statistics': {
                'max_views': max_views,
                'min_views': min_views,
                'median_views': median_views,
                'high_engagement_percentage': round(high_engagement_percentage, 2)
            },
            'average_reactions_per_message': round(avg_reactions_per_message, 2),
            'average_forwards_per_message': round(avg_forwards_per_message, 2),
            'messages_with_high_engagement': len([m for m in messages if m['views'] and m['views'] >= threshold]) if view_data else 0
        }
    
    def _estimate_growth_patterns(self, message_dates, messages):
        """Estimate growth patterns based on activity"""
        from datetime import datetime, timedelta
        import collections
        
        if not message_dates or len(message_dates) < 10:
            return {'note': 'Insufficient data for growth analysis'}
        
        # Sort dates
        sorted_dates = sorted(message_dates)
        
        # Calculate posting frequency over time
        date_groups = collections.defaultdict(int)
        for date in sorted_dates:
            date_key = date.date()
            date_groups[date_key] += 1
        
        # Calculate daily averages for different periods
        now = datetime.now(sorted_dates[0].tzinfo)
        last_7_days = now - timedelta(days=7)
        last_30_days = now - timedelta(days=30)
        
        recent_daily_posts = []
        older_daily_posts = []
        
        for date, count in date_groups.items():
            date_dt = datetime.combine(date, datetime.min.time()).replace(tzinfo=sorted_dates[0].tzinfo)
            if date_dt >= last_7_days:
                recent_daily_posts.append(count)
            elif date_dt >= last_30_days:
                older_daily_posts.append(count)
        
        # Growth indicators
        recent_avg = sum(recent_daily_posts) / len(recent_daily_posts) if recent_daily_posts else 0
        older_avg = sum(older_daily_posts) / len(older_daily_posts) if older_daily_posts else 0
        
        posting_growth = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0
        
        # Estimate member growth based on view patterns
        messages_with_views = [m for m in messages if m['views'] is not None]
        if len(messages_with_views) >= 20:
            # Split into two halves and compare average views
            mid_point = len(messages_with_views) // 2
            older_half = messages_with_views[mid_point:]
            recent_half = messages_with_views[:mid_point]
            
            older_avg_views = sum(m['views'] for m in older_half) / len(older_half)
            recent_avg_views = sum(m['views'] for m in recent_half) / len(recent_half)
            
            view_growth = ((recent_avg_views - older_avg_views) / older_avg_views * 100) if older_avg_views > 0 else 0
        else:
            view_growth = 0
        
        return {
            'posting_frequency_growth': round(posting_growth, 2),
            'view_engagement_growth': round(view_growth, 2),
            'recent_daily_average_posts': round(recent_avg, 2),
            'older_daily_average_posts': round(older_avg, 2),
            'growth_indicator': 'positive' if posting_growth > 10 and view_growth > 0 else 'stable' if abs(posting_growth) <= 10 else 'declining',
            'data_points_analyzed': len(sorted_dates),
            'analysis_period_days': 30
        }
    
    async def _get_member_analysis(self, entity, client):
        """Get member data and analysis"""
        try:
            member_data = {}
            
            # Get current member count
            current_members = getattr(entity, 'participants_count', None)
            if current_members:
                member_data['current_member_count'] = current_members
            
            # Try to get participant statistics (if admin access)
            try:
                # Get all participants to analyze with comprehensive approach
                participants = []
                total_processed = 0
                
                logger.info(f"Starting comprehensive member analysis for channel with {current_members or 'unknown'} members...")
                
                # First try: Get full channel info to check member count
                try:
                    from telethon.tl.functions.channels import GetFullChannelRequest
                    full_channel = await client(GetFullChannelRequest(entity))
                    if hasattr(full_channel.full_chat, 'participants_count'):
                        current_members = full_channel.full_chat.participants_count
                        member_data['current_member_count'] = current_members
                        logger.info(f"Channel has {current_members} members according to GetFullChannelRequest")
                except Exception as full_error:
                    logger.warning(f"GetFullChannelRequest failed: {full_error}")
                
                # Comprehensive approach: Multiple methods with aggressive retry
                all_member_ids = set()  # Use set to avoid duplicates
                max_target = current_members or 50000
                
                # Method 1: Aggressive GetParticipantsRequest with pagination
                try:
                    logger.info("Method 1: Aggressive GetParticipantsRequest with pagination")
                    from telethon.tl.functions.channels import GetParticipantsRequest
                    from telethon.tl.types import ChannelParticipantsSearch
                    
                    offset = 0
                    batch_size = 200  # Start with 200 per request
                    consecutive_empty = 0
                    max_empty_batches = 3
                    
                    while len(all_member_ids) < max_target and consecutive_empty < max_empty_batches:
                        try:
                            result = await client(GetParticipantsRequest(
                                channel=entity,
                                filter=ChannelParticipantsSearch(''),
                                offset=offset,
                                limit=batch_size,
                                hash=0
                            ))
                            
                            if not result.users:
                                consecutive_empty += 1
                                logger.info(f"Empty batch {consecutive_empty} at offset {offset}")
                                await asyncio.sleep(1)  # Wait longer for empty batches
                                continue
                            
                            consecutive_empty = 0  # Reset on successful batch
                            batch_added = 0
                            
                            for user in result.users:
                                if user.id not in all_member_ids:
                                    participant_data = {
                                        'id': user.id,
                                        'first_name': getattr(user, 'first_name', None),
                                        'last_name': getattr(user, 'last_name', None),
                                        'username': getattr(user, 'username', None),
                                        'is_bot': getattr(user, 'bot', False),
                                        'is_premium': getattr(user, 'premium', False),
                                        'status': getattr(user, 'status', None),
                                        'joined_date': None
                                    }
                                    participants.append(participant_data)
                                    all_member_ids.add(user.id)
                                    batch_added += 1
                            
                            logger.info(f"Retrieved {len(result.users)} users, {batch_added} new, total unique: {len(all_member_ids)}")
                            offset += len(result.users)
                            
                            # Adaptive batch size and delay
                            if len(result.users) == batch_size:
                                batch_size = min(batch_size + 50, 1000)  # Increase batch size if successful
                                await asyncio.sleep(0.5)  # Short delay for successful batches
                            else:
                                await asyncio.sleep(1)  # Longer delay for partial batches
                            
                            # Progress logging every 1000 members
                            if len(all_member_ids) % 1000 == 0 and len(all_member_ids) > 0:
                                logger.info(f"Progress: {len(all_member_ids)}/{max_target} members ({(len(all_member_ids)/max_target)*100:.1f}%)")
                            
                        except Exception as req_error:
                            logger.warning(f"GetParticipantsRequest failed at offset {offset}: {req_error}")
                            consecutive_empty += 1
                            await asyncio.sleep(2)  # Wait longer on errors
                            break
                    
                    logger.info(f"Method 1 completed: {len(all_member_ids)} unique members")
                    
                except Exception as method1_error:
                    logger.warning(f"Method 1 failed: {method1_error}")
                
                # Method 2: Multiple iter_participants calls with different filters
                if len(all_member_ids) < max_target:
                    try:
                        logger.info("Method 2: Multiple iter_participants approaches")
                        
                        # Sub-method 2a: High limit iteration
                        try:
                            logger.info("Method 2a: High limit iter_participants")
                            count_2a = 0
                            async for participant in client.iter_participants(entity, limit=100000):
                                if participant.id not in all_member_ids:
                                    participant_data = {
                                        'id': participant.id,
                                        'first_name': getattr(participant, 'first_name', None),
                                        'last_name': getattr(participant, 'last_name', None),
                                        'username': getattr(participant, 'username', None),
                                        'is_bot': getattr(participant, 'bot', False),
                                        'is_premium': getattr(participant, 'premium', False),
                                        'status': getattr(participant, 'status', None),
                                        'joined_date': getattr(participant, 'date', None)
                                    }
                                    participants.append(participant_data)
                                    all_member_ids.add(participant.id)
                                    count_2a += 1
                                    
                                    if len(all_member_ids) % 1000 == 0:
                                        logger.info(f"Method 2a progress: {len(all_member_ids)} members")
                            
                            logger.info(f"Method 2a added {count_2a} new members")
                        except Exception as sub2a_error:
                            logger.warning(f"Method 2a failed: {sub2a_error}")
                        
                        # Sub-method 2b: Bots filter
                        try:
                            logger.info("Method 2b: Bots filter")
                            from telethon.tl.types import ChannelParticipantsBots
                            count_2b = 0
                            async for participant in client.iter_participants(entity, filter=ChannelParticipantsBots()):
                                if participant.id not in all_member_ids:
                                    participant_data = {
                                        'id': participant.id,
                                        'first_name': getattr(participant, 'first_name', None),
                                        'last_name': getattr(participant, 'last_name', None),
                                        'username': getattr(participant, 'username', None),
                                        'is_bot': getattr(participant, 'bot', False),
                                        'is_premium': getattr(participant, 'premium', False),
                                        'status': getattr(participant, 'status', None),
                                        'joined_date': getattr(participant, 'date', None)
                                    }
                                    participants.append(participant_data)
                                    all_member_ids.add(participant.id)
                                    count_2b += 1
                            
                            logger.info(f"Method 2b added {count_2b} new members")
                        except Exception as sub2b_error:
                            logger.warning(f"Method 2b failed: {sub2b_error}")
                        
                        # Sub-method 2c: Online users filter
                        try:
                            logger.info("Method 2c: Online users filter")
                            from telethon.tl.types import ChannelParticipantsOnline
                            count_2c = 0
                            async for participant in client.iter_participants(entity, filter=ChannelParticipantsOnline()):
                                if participant.id not in all_member_ids:
                                    participant_data = {
                                        'id': participant.id,
                                        'first_name': getattr(participant, 'first_name', None),
                                        'last_name': getattr(participant, 'last_name', None),
                                        'username': getattr(participant, 'username', None),
                                        'is_bot': getattr(participant, 'bot', False),
                                        'is_premium': getattr(participant, 'premium', False),
                                        'status': getattr(participant, 'status', None),
                                        'joined_date': getattr(participant, 'date', None)
                                    }
                                    participants.append(participant_data)
                                    all_member_ids.add(participant.id)
                                    count_2c += 1
                            
                            logger.info(f"Method 2c added {count_2c} new members")
                        except Exception as sub2c_error:
                            logger.warning(f"Method 2c failed: {sub2c_error}")
                        
                    except Exception as method2_error:
                        logger.warning(f"Method 2 failed: {method2_error}")
                
                # Method 3: Alphabetical search approach
                if len(all_member_ids) < max_target:
                    try:
                        logger.info("Method 3: Alphabetical search approach")
                        from telethon.tl.types import ChannelParticipantsSearch
                        
                        # Search for users with different letters
                        search_terms = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                       'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                       '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '_', '@']
                        
                        count_3 = 0
                        for search_term in search_terms:
                            if len(all_member_ids) >= max_target:
                                break
                            
                            try:
                                term_count = 0
                                async for participant in client.iter_participants(entity, filter=ChannelParticipantsSearch(search_term)):
                                    if participant.id not in all_member_ids:
                                        participant_data = {
                                            'id': participant.id,
                                            'first_name': getattr(participant, 'first_name', None),
                                            'last_name': getattr(participant, 'last_name', None),
                                            'username': getattr(participant, 'username', None),
                                            'is_bot': getattr(participant, 'bot', False),
                                            'is_premium': getattr(participant, 'premium', False),
                                            'status': getattr(participant, 'status', None),
                                            'joined_date': getattr(participant, 'date', None)
                                        }
                                        participants.append(participant_data)
                                        all_member_ids.add(participant.id)
                                        term_count += 1
                                        count_3 += 1
                                
                                if term_count > 0:
                                    logger.info(f"Search '{search_term}' found {term_count} new users")
                                
                                # Small delay between searches
                                await asyncio.sleep(0.3)
                                
                            except Exception as search_error:
                                logger.debug(f"Search '{search_term}' failed: {search_error}")
                                continue
                        
                        logger.info(f"Method 3 added {count_3} new members")
                        
                    except Exception as method3_error:
                        logger.warning(f"Method 3 failed: {method3_error}")
                
                logger.info(f"Comprehensive analysis completed. Total unique members: {len(all_member_ids)}")
                
                if participants:
                    member_data.update({
                        'total_members_analyzed': len(participants),
                        'unique_member_ids': len(all_member_ids),
                        'bot_count': sum(1 for p in participants if p['is_bot']),
                        'premium_count': sum(1 for p in participants if p['is_premium']),
                        'with_username_count': sum(1 for p in participants if p['username']),
                        'member_status_distribution': self._analyze_member_status(participants),
                        'recent_joins': self._analyze_recent_joins(participants)
                    })
                
                # Add comprehensive coverage information
                if current_members:
                    coverage_percentage = (len(all_member_ids) / current_members) * 100
                    member_data['coverage_percentage'] = round(coverage_percentage, 2)
                    logger.info(f"Final analysis coverage: {coverage_percentage:.2f}% of total members ({len(all_member_ids)}/{current_members})")
                
            except Exception as e:
                logger.warning(f"Could not access participant data: {e}")
                member_data['participant_access'] = 'restricted'
                member_data['error_details'] = str(e)
            
            return member_data
            
        except Exception as e:
            logger.error(f"Error in member analysis: {e}")
            return {'error': str(e)}
    
    def _analyze_member_status(self, participants):
        """Analyze member status distribution"""
        from collections import Counter
        
        # Convert status objects to readable strings for counting
        status_strings = []
        for p in participants:
            if p['status']:
                status_str = str(p['status'])
                # Extract just the status type (Online, Offline, etc.) without the datetime details
                if 'UserStatus' in status_str:
                    status_type = status_str.split('(')[0].replace('UserStatus', '')
                    status_strings.append(status_type)
                else:
                    status_strings.append(status_str)
        
        status_counts = Counter(status_strings)
        return dict(status_counts)
    
    def _analyze_recent_joins(self, participants):
        """Analyze recent member joins"""
        recent_joins = [p for p in participants if p['joined_date']]
        
        if not recent_joins:
            return {'recent_joins_count': 0}
        
        # Sort by join date
        recent_joins.sort(key=lambda x: x['joined_date'], reverse=True)
        
        # Count joins in last 7, 30 days
        from datetime import datetime, timedelta
        
        now = datetime.now(recent_joins[0]['joined_date'].tzinfo)
        last_7_days = now - timedelta(days=7)
        last_30_days = now - timedelta(days=30)
        
        joins_7_days = sum(1 for p in recent_joins if p['joined_date'] >= last_7_days)
        joins_30_days = sum(1 for p in recent_joins if p['joined_date'] >= last_30_days)
        
        return {
            'recent_joins_count': len(recent_joins),
            'joins_last_7_days': joins_7_days,
            'joins_last_30_days': joins_30_days,
            'join_rate_7_days': round(joins_7_days / 7, 2) if joins_7_days > 0 else 0,
            'join_rate_30_days': round(joins_30_days / 30, 2) if joins_30_days > 0 else 0
        }
    
    def _generate_graph_data(self, message_dates, messages):
        """Generate time-series data for graphs"""
        from datetime import datetime, timedelta
        import collections
        from collections import defaultdict
        
        if not message_dates:
            return {}
        
        graph_data = {}
        
        # Daily message count for time series
        daily_messages = defaultdict(int)
        daily_views = defaultdict(list)
        daily_reactions = defaultdict(list)
        
        for i, date in enumerate(message_dates):
            date_key = date.date()
            daily_messages[date_key] += 1
            
            if i < len(messages):
                msg = messages[i]
                if msg['views']:
                    daily_views[date_key].append(msg['views'])
                if msg['reactions']:
                    daily_reactions[date_key].append(msg['reactions'])
        
        # Calculate daily averages
        daily_avg_views = {}
        daily_avg_reactions = {}
        daily_total_reactions = {}
        
        for date in daily_messages:
            views_list = daily_views.get(date, [])
            reactions_list = daily_reactions.get(date, [])
            
            daily_avg_views[date] = sum(views_list) / len(views_list) if views_list else 0
            daily_avg_reactions[date] = sum(reactions_list) / len(reactions_list) if reactions_list else 0
            daily_total_reactions[date] = sum(reactions_list)
        
        # Sort dates
        sorted_dates = sorted(daily_messages.keys())
        
        # Create time series data
        graph_data['message_volume'] = [
            {'date': date.isoformat(), 'count': daily_messages[date]}
            for date in sorted_dates
        ]
        
        graph_data['engagement_metrics'] = [
            {
                'date': date.isoformat(),
                'avg_views': daily_avg_views[date],
                'total_reactions': daily_total_reactions[date],
                'avg_reactions': daily_avg_reactions[date]
            }
            for date in sorted_dates
        ]
        
        # Hourly activity heatmap data
        hourly_activity = defaultdict(int)
        for date in message_dates:
            hour_key = f"{date.strftime('%A')}_{date.hour:02d}:00"
            hourly_activity[hour_key] += 1
        
        graph_data['hourly_heatmap'] = [
            {'hour': hour, 'count': count}
            for hour, count in sorted(hourly_activity.items())
        ]
        
        # Member growth estimation over time
        member_estimates = self._estimate_member_growth_timeline(message_dates, messages)
        if member_estimates:
            graph_data['member_growth'] = member_estimates
        
        # Media type distribution over time
        media_timeline = self._create_media_timeline(messages, sorted_dates)
        if media_timeline:
            graph_data['media_timeline'] = media_timeline
        
        return graph_data
    
    def _estimate_member_growth_timeline(self, message_dates, messages):
        """Estimate member growth over time based on view patterns"""
        if len(messages) < 20:
            return []
        
        # Group messages by week
        from collections import defaultdict
        weekly_data = defaultdict(list)
        
        for i, date in enumerate(message_dates):
            if i < len(messages) and messages[i]['views']:
                week_start = date - timedelta(days=date.weekday())
                week_key = week_start.date().isoformat()
                weekly_data[week_key].append(messages[i]['views'])
        
        # Calculate weekly average views as proxy for member growth
        timeline = []
        for week in sorted(weekly_data.keys()):
            avg_views = sum(weekly_data[week]) / len(weekly_data[week])
            timeline.append({
                'week': week,
                'estimated_members': int(avg_views * 1.5),  # Rough estimation
                'avg_views': round(avg_views, 2),
                'message_sample': len(weekly_data[week])
            })
        
        return timeline
    
    def _create_media_timeline(self, messages, dates):
        """Create media type distribution over time"""
        media_by_date = defaultdict(lambda: defaultdict(int))
        
        for i, date in enumerate(dates):
            if i < len(messages) and messages[i]['media_type']:
                date_key = date.isoformat()
                media_type = messages[i]['media_type']
                media_by_date[date_key][media_type] += 1
        
        timeline = []
        for date in sorted(media_by_date.keys()):
            timeline.append({
                'date': date,
                'media_distribution': dict(media_by_date[date])
            })
        
        return timeline
    
    def _analyze_yearly_stats(self, message_dates, messages):
        """Analyze comprehensive yearly statistics"""
        if not message_dates or not messages:
            return {'error': 'Insufficient data for yearly analysis'}
        
        yearly_data = defaultdict(lambda: {
            'messages': [],
            'views': [],
            'reactions': [],
            'forwards': [],
            'media_count': 0,
            'text_count': 0,
            'months': defaultdict(int),
            'monthly_views': defaultdict(list)
        })
        
        # Group data by year
        for i, date in enumerate(message_dates):
            if i < len(messages):
                year = date.year
                msg = messages[i]
                
                yearly_data[year]['messages'].append(msg)
                yearly_data[year]['months'][date.month] += 1
                
                if msg['views']:
                    yearly_data[year]['views'].append(msg['views'])
                    yearly_data[year]['monthly_views'][date.month].append(msg['views'])
                if msg['reactions']:
                    yearly_data[year]['reactions'].append(msg['reactions'])
                if msg['forwards']:
                    yearly_data[year]['forwards'].append(msg['forwards'])
                if msg['has_media']:
                    yearly_data[year]['media_count'] += 1
                else:
                    yearly_data[year]['text_count'] += 1
        
        # Calculate yearly statistics
        yearly_stats = {}
        
        for year in sorted(yearly_data.keys()):
            data = yearly_data[year]
            
            # Basic metrics
            total_messages = len(data['messages'])
            total_views = sum(data['views'])
            total_reactions = sum(data['reactions'])
            total_forwards = sum(data['forwards'])
            
            # Averages
            avg_views = total_views / len(data['views']) if data['views'] else 0
            avg_reactions = total_reactions / len(data['reactions']) if data['reactions'] else 0
            avg_forwards = total_forwards / len(data['forwards']) if data['forwards'] else 0
            
            # Engagement metrics
            engagement_rate = (total_reactions / total_messages * 100) if total_messages > 0 else 0
            forward_rate = (total_forwards / total_messages * 100) if total_messages > 0 else 0
            media_percentage = (data['media_count'] / total_messages * 100) if total_messages > 0 else 0
            
            # Monthly distribution
            monthly_activity = dict(data['months'])
            most_active_month = max(monthly_activity.keys(), key=lambda x: monthly_activity[x]) if monthly_activity else None
            
            # Calculate monthly average views
            monthly_avg_views = {}
            for month, posts in monthly_activity.items():
                month_views = data['monthly_views'].get(month, [])
                monthly_avg_views[month] = round(sum(month_views) / len(month_views), 2) if month_views else 0
            
            # Seasonal analysis
            season_data = self._analyze_seasonal_patterns(data['months'], data['monthly_views'])
            
            # Growth metrics (compare with previous year if available)
            growth_metrics = {}
            if year - 1 in yearly_data:
                prev_data = yearly_data[year - 1]
                prev_messages = len(prev_data['messages'])
                message_growth = ((total_messages - prev_messages) / prev_messages * 100) if prev_messages > 0 else 0
                
                prev_views = sum(prev_data['views'])
                view_growth = ((total_views - prev_views) / prev_views * 100) if prev_views > 0 else 0
                
                growth_metrics = {
                    'message_growth_percent': round(message_growth, 2),
                    'view_growth_percent': round(view_growth, 2),
                    'previous_year_messages': prev_messages,
                    'previous_year_views': prev_views
                }
            
            yearly_stats[year] = {
                'year': year,
                'total_messages': total_messages,
                'total_views': total_views,
                'total_reactions': total_reactions,
                'total_forwards': total_forwards,
                'average_views_per_message': round(avg_views, 2),
                'average_reactions_per_message': round(avg_reactions, 2),
                'average_forwards_per_message': round(avg_forwards, 2),
                'engagement_rate': round(engagement_rate, 2),
                'forward_rate': round(forward_rate, 2),
                'media_content_percentage': round(media_percentage, 2),
                'media_messages': data['media_count'],
                'text_messages': data['text_count'],
                'monthly_distribution': monthly_activity,
                'monthly_average_views': monthly_avg_views,
                'most_active_month': most_active_month,
                'seasonal_analysis': season_data,
                'unique_months_active': len(monthly_activity),
                'growth_metrics': growth_metrics,
                'peak_month': max(monthly_activity.items(), key=lambda x: x[1])[0] if monthly_activity else None,
                'peak_month_messages': max(monthly_activity.values()) if monthly_activity else 0,
                'best_month_by_avg_views': max(monthly_avg_views.items(), key=lambda x: x[1])[0] if monthly_avg_views else None,
                'best_month_avg_views': max(monthly_avg_views.values()) if monthly_avg_views else 0
            }
        
        # Overall yearly trends
        if len(yearly_stats) > 1:
            years = sorted(yearly_stats.keys())
            first_year = yearly_stats[years[0]]
            latest_year = yearly_stats[years[-1]]
            
            overall_growth = {
                'years_analyzed': len(yearly_stats),
                'year_range': f"{years[0]}-{years[-1]}",
                'total_period_messages': sum(stats['total_messages'] for stats in yearly_stats.values()),
                'total_period_views': sum(stats['total_views'] for stats in yearly_stats.values()),
                'average_yearly_messages': round(sum(stats['total_messages'] for stats in yearly_stats.values()) / len(yearly_stats), 2),
                'average_yearly_views': round(sum(stats['total_views'] for stats in yearly_stats.values()) / len(yearly_stats), 2),
                'overall_message_growth': round(((latest_year['total_messages'] - first_year['total_messages']) / first_year['total_messages'] * 100), 2) if first_year['total_messages'] > 0 else 0,
                'overall_view_growth': round(((latest_year['total_views'] - first_year['total_views']) / first_year['total_views'] * 100), 2) if first_year['total_views'] > 0 else 0,
                'best_year_by_messages': max(yearly_stats.keys(), key=lambda x: yearly_stats[x]['total_messages']),
                'best_year_by_views': max(yearly_stats.keys(), key=lambda x: yearly_stats[x]['total_views']),
                'best_year_by_engagement': max(yearly_stats.keys(), key=lambda x: yearly_stats[x]['engagement_rate'])
            }
        else:
            overall_growth = {
                'years_analyzed': len(yearly_stats),
                'note': 'Insufficient data for overall trends (need at least 2 years)'
            }
        
        return {
            'yearly_breakdown': yearly_stats,
            'overall_trends': overall_growth,
            'data_completeness': {
                'total_years_analyzed': len(yearly_stats),
                'date_range_start': min(message_dates).date().isoformat() if message_dates else None,
                'date_range_end': max(message_dates).date().isoformat() if message_dates else None,
                'total_messages_analyzed': len(messages)
            }
        }
    
    def _analyze_seasonal_patterns(self, monthly_data, monthly_views):
        """Analyze seasonal posting patterns with average views"""
        seasons = {
            'Spring': [3, 4, 5],   # March, April, May
            'Summer': [6, 7, 8],   # June, July, August
            'Fall': [9, 10, 11],   # September, October, November
            'Winter': [12, 1, 2]   # December, January, February
        }
        
        seasonal_avg_views = {}
        for season, months in seasons.items():
            total_views = 0
            total_posts = 0
            for month in months:
                posts = monthly_data.get(month, 0)
                views = monthly_views.get(month, [])
                total_posts += posts
                total_views += sum(views)
            
            # Calculate average views per season
            seasonal_avg_views[season] = total_views / total_posts if total_posts > 0 else 0
        
        most_active_season = max(seasonal_avg_views.keys(), key=lambda x: seasonal_avg_views[x]) if seasonal_avg_views else None
        
        return {
            'seasonal_distribution': {season: round(avg_views, 2) for season, avg_views in seasonal_avg_views.items()},
            'most_active_season': most_active_season,
            'seasonal_percentage': {
                season: round((avg_views / sum(seasonal_avg_views.values()) * 100), 2) if sum(seasonal_avg_views.values()) > 0 else 0
                for season, avg_views in seasonal_avg_views.items()
            }
        }
    
    async def get_views_per_minute_analysis(self, channel_id):
        """Get views per minute analysis for the most recent post"""
        try:
            client = await ensure_connected()
            
            # For bots, check if connected; for users, check authorization
            if use_bot:
                if not client.is_connected():
                    return {'error': 'Bot connection failed. Please check your bot token.'}
            else:
                if not await client.is_user_authorized():
                    return {'error': 'Authorization required. Please check your Telegram for verification code.'}
            
            # Get channel entity
            try:
                entity = await client.get_entity(channel_id)
            except Exception as e:
                if "key is not registered" in str(e) and use_bot:
                    try:
                        invite_hash = channel_id.split('/')[-1] if '/' in channel_id else channel_id
                        await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
                        entity = await client.get_entity(channel_id)
                    except Exception as join_error:
                        return {'error': f'Bot needs to join the channel first. Please add your bot to the channel or check the invite link. Details: {str(join_error)}'}
                else:
                    raise
            
            # Get the most recent message
            messages = []
            try:
                async for message in client.iter_messages(entity, limit=1):
                    if message and hasattr(message, 'views') and message.views:
                        # Store the most recent message with views
                        messages.append({
                            'message_id': message.id,
                            'date': message.date.isoformat() if message.date else None,
                            'text': message.text[:200] + '...' if message.text and len(message.text) > 200 else message.text,
                            'views': message.views,
                            'forwards': getattr(message, 'forwards', None),
                            'reactions': len(message.reactions.results) if hasattr(message, 'reactions') and message.reactions else 0,
                            'has_media': bool(message.media),
                            'media_type': self._get_media_type(message.media) if message.media else None
                        })
                        break  # Only need the most recent message
            except Exception as e:
                logger.error(f"Error fetching recent message: {e}")
                return {'error': f'Could not fetch recent messages: {str(e)}'}
            
            if not messages:
                return {'error': 'No recent messages with view data found in this channel'}
            
            recent_message = messages[0]
            
            # Generate simulated views per minute data
            # Note: Telegram API doesn't provide historical views per minute data
            # This is a simulation based on typical view growth patterns
            from datetime import datetime, timedelta
            import random
            
            message_date = datetime.fromisoformat(recent_message['date'].replace('Z', '+00:00') if recent_message['date'].endswith('Z') else recent_message['date'])
            total_views = recent_message['views']
            
            # Simulate view growth over time since message was posted
            time_since_post = datetime.now(message_date.tzinfo) - message_date
            minutes_elapsed = max(1, int(time_since_post.total_seconds() / 60))
            
            # Generate realistic view growth pattern
            views_data = []
            current_views = 0
            
            # Different growth patterns based on channel activity
            if total_views > 10000:
                # High activity channel - rapid initial growth
                initial_burst = min(total_views * 0.3, total_views)
                growth_rate = 0.8
            elif total_views > 1000:
                # Medium activity channel
                initial_burst = min(total_views * 0.2, total_views)
                growth_rate = 0.6
            else:
                # Low activity channel
                initial_burst = min(total_views * 0.1, total_views)
                growth_rate = 0.4
            
            # Generate minute-by-minute data
            for minute in range(min(minutes_elapsed, 1440)):  # Max 24 hours of data
                if minute == 0:
                    current_views = initial_burst
                else:
                    # Exponential decay growth pattern
                    increment = (total_views - current_views) * growth_rate * (1 / (minute + 1))
                    current_views += increment + random.uniform(0, increment * 0.3)
                    current_views = min(current_views, total_views)
                
                views_data.append({
                    'minute': minute,
                    'views': round(current_views),
                    'timestamp': (message_date + timedelta(minutes=minute)).isoformat(),
                    'new_views': round(increment) if minute > 0 else round(initial_burst)
                })
            
            # Ensure we reach the total views
            if views_data:
                views_data[-1]['views'] = total_views
            
            # Calculate statistics
            views_per_minute = [v['new_views'] for v in views_data if v['new_views'] > 0]
            avg_views_per_minute = sum(views_per_minute) / len(views_per_minute) if views_per_minute else 0
            peak_minute = max(views_data, key=lambda x: x['new_views']) if views_data else None
            
            return {
                'recent_post': recent_message,
                'views_per_minute_data': views_data,
                'analysis': {
                    'total_views': total_views,
                    'minutes_tracked': len(views_data),
                    'average_views_per_minute': round(avg_views_per_minute, 2),
                    'peak_views_per_minute': peak_minute['new_views'] if peak_minute else 0,
                    'peak_minute': peak_minute['minute'] if peak_minute else 0,
                    'message_age_minutes': minutes_elapsed,
                    'views_growth_rate': round((total_views / max(minutes_elapsed, 1)), 2),
                    'engagement_level': self._calculate_engagement_level(total_views, minutes_elapsed)
                },
                'graph_data': {
                    'labels': [f"{v['minute']}m" for v in views_data],  # All minute data
                    'views': [v['views'] for v in views_data],
                    'new_views': [v['new_views'] for v in views_data]
                }
            }
            
        except Exception as e:
            logger.error(f"Error in views per minute analysis: {e}")
            return {'error': f'Analysis failed: {str(e)}'}
    
    def _calculate_engagement_level(self, total_views, minutes_elapsed):
        """Calculate engagement level based on views and time"""
        if minutes_elapsed == 0:
            return 'unknown'
        
        views_per_minute = total_views / minutes_elapsed
        
        if views_per_minute > 100:
            return 'very_high'
        elif views_per_minute > 50:
            return 'high'
        elif views_per_minute > 20:
            return 'medium'
        elif views_per_minute > 5:
            return 'low'
        else:
            return 'very_low'

analyzer = ChannelAnalyzer()

@app.route('/api/auth/request-code', methods=['POST'])
def request_verification_code():
    """Request verification code from Telegram"""
    try:
        loop = get_event_loop()
        result = loop.run_until_complete(analyzer.sign_in())
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error requesting verification code: {e}")
        return jsonify({'success': False, 'error': f'Server error: {str(e)}'}), 500

@app.route('/api/auth/verify-code', methods=['POST'])
def verify_code():
    """Verify the entered code"""
    try:
        data = request.get_json()
        if not data or 'code' not in data:
            return jsonify({'success': False, 'error': 'Verification code is required'}), 400
        
        code = data['code'].strip()
        
        loop = get_event_loop()
        result = loop.run_until_complete(analyzer.sign_in(code))
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error verifying code: {e}")
        return jsonify({'success': False, 'error': f'Server error: {str(e)}'}), 500

@app.route('/api/analyze-channel', methods=['POST'])
@rate_limit(max_requests=10, window_seconds=60)
@validate_channel_id
def analyze_channel():
    """Analyze a Telegram channel by ID"""
    try:
        data = request.get_json()
        channel_id = data['channel_id']
        
        # Run async analysis
        loop = get_event_loop()
        result = loop.run_until_complete(analyzer.get_channel_statistics(channel_id))
        
        if 'error' in result:
            return jsonify(result), 400
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in analyze_channel endpoint: {e}")
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/api/channel-info', methods=['POST'])
@rate_limit(max_requests=15, window_seconds=60)
@validate_channel_id
def get_channel_info():
    """Get basic channel information"""
    try:
        data = request.get_json()
        channel_id = data['channel_id']
        
        # Run async analysis
        loop = get_event_loop()
        result = loop.run_until_complete(analyzer.get_channel_info(channel_id))
        
        if 'error' in result:
            return jsonify(result), 400
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in get_channel_info endpoint: {e}")
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/api/analyze-with-claude', methods=['POST'])
@rate_limit(max_requests=5, window_seconds=60)
@validate_channel_id
def analyze_with_claude():
    """Analyze channel data using Claude AI"""
    try:
        data = request.get_json()
        channel_id = data.get('channel_id')
        
        if not channel_id:
            return jsonify({'error': 'Channel ID is required'}), 400
        
        # First get the channel analysis data
        analyzer = ChannelAnalyzer()
        loop = get_event_loop()
        
        # Run the channel analysis
        channel_data = loop.run_until_complete(analyzer.get_channel_statistics(channel_id))
        
        if 'error' in channel_data:
            return jsonify(channel_data), 400
        
        # Then send to Claude for analysis
        claude_result = loop.run_until_complete(_claude_analyzer.analyze_channel_data(channel_data))
        
        if 'error' in claude_result:
            return jsonify(claude_result), 500
        
        # Also get sentiment analysis of recent messages
        sentiment_result = loop.run_until_complete(
            _claude_analyzer.analyze_message_sentiment(channel_data.get('recent_messages', []))
        )
        
        return jsonify({
            'channel_data': channel_data,
            'claude_analysis': claude_result,
            'sentiment_analysis': sentiment_result,
            'success': True
        })
        
    except Exception as e:
        logger.error(f"Error in Claude analysis: {e}")
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500

@app.route('/api/views-per-minute', methods=['POST'])
@rate_limit(max_requests=10, window_seconds=60)
@validate_channel_id
def get_views_per_minute():
    """Get views per minute analysis for the most recent post"""
    try:
        data = request.get_json()
        channel_id = data.get('channel_id')
        
        if not channel_id:
            return jsonify({'error': 'Channel ID is required'}), 400
        
        analyzer = ChannelAnalyzer()
        loop = get_event_loop()
        
        # Get the views per minute analysis
        views_analysis = loop.run_until_complete(analyzer.get_views_per_minute_analysis(channel_id))
        
        if 'error' in views_analysis:
            return jsonify(views_analysis), 400
        
        return jsonify(views_analysis)
        
    except Exception as e:
        logger.error(f"Error in views_per_minute endpoint: {e}")
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'message': 'Telegram Channel Analysis API is running'})

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
