import os
import json
from anthropic import Anthropic
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class ClaudeAnalyzer:
    def __init__(self):
        self.api_key = os.getenv('CLAUDE_API_KEY')
        if not self.api_key or self.api_key == 'your_claude_api_key_here':
            logger.warning("Claude API key not configured. Please set CLAUDE_API_KEY in your .env file.")
            self.client = None
        else:
            self.client = Anthropic(api_key=self.api_key)
    
    def is_configured(self) -> bool:
        """Check if Claude API is properly configured"""
        return self.client is not None
    
    async def analyze_channel_data(self, channel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send channel data to Claude for analysis"""
        if not self.is_configured():
            return {
                'error': 'Claude API not configured. Please add your Claude API key to the .env file.',
                'configured': False
            }
        
        try:
            # Prepare a comprehensive summary of the channel data
            analysis_prompt = self._create_analysis_prompt(channel_data)
            
            # Call Claude API
            response = self.client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=2000,
                temperature=0.3,
                messages=[
                    {
                        "role": "user",
                        "content": analysis_prompt
                    }
                ]
            )
            
            # Parse Claude's response
            claude_analysis = response.content[0].text
            
            return {
                'success': True,
                'analysis': claude_analysis,
                'configured': True,
                'model_used': 'claude-3-sonnet-20240229'
            }
            
        except Exception as e:
            logger.error(f"Error calling Claude API: {e}")
            return {
                'error': f'Failed to get analysis from Claude: {str(e)}',
                'configured': True
            }
    
    def _create_analysis_prompt(self, channel_data: Dict[str, Any]) -> str:
        """Create a comprehensive analysis prompt for Claude"""
        
        # Extract key information
        basic_info = channel_data.get('basic_info', {})
        statistics = channel_data.get('statistics', {})
        time_analysis = channel_data.get('time_analysis', {})
        content_analysis = channel_data.get('content_analysis', {})
        engagement_analysis = channel_data.get('engagement_analysis', {})
        growth_analysis = channel_data.get('growth_analysis', {})
        data_completeness = channel_data.get('data_completeness', {})
        
        prompt = f"""
Please analyze this Telegram channel data and provide comprehensive insights. Here's the data:

## CHANNEL BASIC INFORMATION
- Name: {basic_info.get('title', 'N/A')}
- Username: @{basic_info.get('username', 'N/A')}
- Members: {basic_info.get('member_count', 'N/A')}
- Type: {basic_info.get('type', 'N/A')}
- Verified: {basic_info.get('is_verified', False)}
- Description: {basic_info.get('description', 'N/A')}

## CHANNEL STATISTICS
- Total Messages Analyzed: {statistics.get('total_messages_analyzed', 0)}
- Average Views per Message: {statistics.get('average_views_per_message', 0)}
- Total Forwards: {statistics.get('total_forwards', 0)}
- Total Reactions: {statistics.get('total_reactions', 0)}
- Engagement Rate: {statistics.get('engagement_rate', 0)}%
- Media Content Percentage: {statistics.get('media_content_percentage', 0)}%
- Forward Rate: {statistics.get('forward_rate', 0)}%
- Reaction Rate: {statistics.get('reaction_rate', 0)}

## ACTIVITY PATTERNS
- Most Active Hour: {time_analysis.get('most_active_hour', 'N/A')}
- Most Active Day: {time_analysis.get('most_active_day', 'N/A')}
- Activity Trend (7 days): {time_analysis.get('activity_trend_7_days', 0)}

## CONTENT ANALYSIS
- Average Message Length: {content_analysis.get('average_message_length', 0)} characters
- Forward Percentage: {content_analysis.get('forward_percentage', 0)}%
- Reply Percentage: {content_analysis.get('reply_percentage', 0)}%
- Media Types: {content_analysis.get('media_type_distribution', {})}

## ENGAGEMENT ANALYSIS
- Max Views: {engagement_analysis.get('view_statistics', {}).get('max_views', 0)}
- Median Views: {engagement_analysis.get('view_statistics', {}).get('median_views', 0)}
- High Engagement Percentage: {engagement_analysis.get('view_statistics', {}).get('high_engagement_percentage', 0)}%
- Average Reactions per Message: {engagement_analysis.get('average_reactions_per_message', 0)}

## GROWTH ANALYSIS
- Posting Frequency Growth: {growth_analysis.get('posting_frequency_growth', 0)}%
- View Engagement Growth: {growth_analysis.get('view_engagement_growth', 0)}%
- Growth Indicator: {growth_analysis.get('growth_indicator', 'N/A')}

## DATA QUALITY
- Messages Analyzed: {data_completeness.get('total_messages_fetched', 0)}
- Days Covered: {data_completeness.get('days_covered', 0)}
- Data Quality: {data_completeness.get('data_quality', 'N/A')}

Please provide a comprehensive analysis covering:

1. **Channel Health Assessment**: Overall health and engagement quality
2. **Content Strategy Insights**: What type of content performs best
3. **Growth Potential**: Analysis of growth patterns and future potential
4. **Engagement Optimization**: Recommendations to improve engagement
5. **Audience Analysis**: Insights about the audience behavior
6. **Competitive Positioning**: How this channel might compare to similar channels
7. **Actionable Recommendations**: Specific, actionable advice for channel improvement

Format your response in clear sections with bullet points where appropriate. Be specific and provide data-driven insights based on the numbers provided.
"""
        
        return prompt
    
    async def analyze_message_sentiment(self, messages: list) -> Dict[str, Any]:
        """Analyze sentiment of recent messages"""
        if not self.is_configured():
            return {
                'error': 'Claude API not configured',
                'configured': False
            }
        
        try:
            # Take first 10 messages for sentiment analysis
            message_texts = []
            for msg in messages[:10]:
                if msg.get('text') and len(msg.get('text', '').strip()) > 0:
                    message_texts.append(msg.get('text'))
            
            if not message_texts:
                return {'error': 'No text messages found for analysis', 'configured': True}
            
            sentiment_prompt = f"""
Analyze the sentiment and tone of these recent Telegram messages:

{json.dumps(message_texts, indent=2)}

Please provide:
1. Overall sentiment analysis (positive, negative, neutral)
2. Emotional tone assessment
3. Key themes or topics detected
4. Engagement level indicators
5. Content quality assessment

Keep your analysis concise but insightful.
"""
            
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1000,
                temperature=0.2,
                messages=[
                    {
                        "role": "user",
                        "content": sentiment_prompt
                    }
                ]
            )
            
            return {
                'success': True,
                'sentiment_analysis': response.content[0].text,
                'configured': True,
                'messages_analyzed': len(message_texts)
            }
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return {
                'error': f'Failed to analyze sentiment: {str(e)}',
                'configured': True
            }
