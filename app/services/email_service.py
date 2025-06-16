# FILE: app/services/email_service.py
import os
import smtplib
import boto3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional, Dict, Any
import aiosmtplib
import asyncio
from jinja2 import Template
import logging
from datetime import datetime, timedelta
import uuid
import json

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self):
        self.provider = os.getenv("EMAIL_PROVIDER", "smtp").lower()
        self.from_email = os.getenv("FROM_EMAIL", "noreply@jmkfacilities.ie")
        self.from_name = os.getenv("FROM_NAME", "JMK Facilities Management")
        
        # Initialize based on provider
        if self.provider == "ses":
            self._init_ses()
        elif self.provider == "smtp":
            self._init_smtp()
        elif self.provider == "gmail":
            self._init_gmail()
        else:
            raise ValueError(f"Unsupported email provider: {self.provider}")
    
    def _init_ses(self):
        """Initialize AWS SES"""
        self.ses_client = boto3.client(
            'ses',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_SES_REGION', 'eu-west-1')
        )
    
    def _init_smtp(self):
        """Initialize generic SMTP"""
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    
    def _init_gmail(self):
        """Initialize Gmail SMTP (convenience wrapper)"""
        self.smtp_host = "smtp.gmail.com"
        self.smtp_port = 587
        self.smtp_username = os.getenv("GMAIL_USERNAME")
        self.smtp_password = os.getenv("GMAIL_APP_PASSWORD")  # App password, not regular password
        self.use_tls = True

    async def send_email(
        self,
        to_emails: List[str],
        subject: str,
        html_content: str,
        text_content: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        reply_to: Optional[str] = None
    ) -> bool:
        """Send email using configured provider"""
        try:
            if self.provider == "ses":
                return await self._send_via_ses(to_emails, subject, html_content, text_content)
            else:
                return await self._send_via_smtp(to_emails, subject, html_content, text_content, attachments, reply_to)
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False

    async def _send_via_ses(self, to_emails: List[str], subject: str, html_content: str, text_content: Optional[str] = None) -> bool:
        """Send email via AWS SES"""
        try:
            body = {}
            if html_content:
                body['Html'] = {'Data': html_content, 'Charset': 'UTF-8'}
            if text_content:
                body['Text'] = {'Data': text_content, 'Charset': 'UTF-8'}

            response = self.ses_client.send_email(
                Source=f"{self.from_name} <{self.from_email}>",
                Destination={'ToAddresses': to_emails},
                Message={
                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                    'Body': body
                }
            )
            logger.info(f"Email sent via SES. Message ID: {response['MessageId']}")
            return True
        except Exception as e:
            logger.error(f"SES send failed: {str(e)}")
            return False

    async def _send_via_smtp(
        self, 
        to_emails: List[str], 
        subject: str, 
        html_content: str, 
        text_content: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        reply_to: Optional[str] = None
    ) -> bool:
        """Send email via SMTP"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(to_emails)
            
            if reply_to:
                msg['Reply-To'] = reply_to

            # Add text and HTML parts
            if text_content:
                msg.attach(MIMEText(text_content, 'plain', 'utf-8'))
            if html_content:
                msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            # Add attachments
            if attachments:
                for attachment in attachments:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment['content'])
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {attachment["filename"]}'
                    )
                    msg.attach(part)

            # Send email
            async with aiosmtplib.SMTP(hostname=self.smtp_host, port=self.smtp_port, use_tls=self.use_tls) as server:
                if self.smtp_username and self.smtp_password:
                    await server.login(self.smtp_username, self.smtp_password)
                await server.send_message(msg)

            logger.info(f"Email sent via SMTP to: {', '.join(to_emails)}")
            return True
        except Exception as e:
            logger.error(f"SMTP send failed: {str(e)}")
            return False

# Email Templates
class EmailTemplates:
    @staticmethod
    def password_reset_template(reset_link: str, user_name: str, expires_minutes: int = 15) -> tuple:
        """Password reset email template"""
        html_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Password Reset - JMK Facilities</title>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; background-color: #f8fafc; }
                .container { max-width: 600px; margin: 0 auto; background-color: white; }
                .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center; }
                .header h1 { color: white; margin: 0; font-size: 24px; }
                .content { padding: 40px 30px; }
                .button { display: inline-block; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: 600; margin: 20px 0; }
                .button:hover { background: linear-gradient(135deg, #5a6fd8 0%, #6b4190 100%); }
                .warning { background-color: #fef2f2; border: 1px solid #fecaca; padding: 15px; border-radius: 6px; margin: 20px 0; }
                .footer { background-color: #f8fafc; padding: 20px; text-align: center; font-size: 14px; color: #6b7280; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔒 Password Reset Request</h1>
                </div>
                <div class="content">
                    <h2>Hi {{ user_name }},</h2>
                    <p>We received a request to reset your password for your JMK Facilities Management account.</p>
                    <p>Click the button below to create a new password:</p>
                    
                    <div style="text-align: center;">
                        <a href="{{ reset_link }}" class="button">Reset My Password</a>
                    </div>
                    
                    <div class="warning">
                        <strong>⚠️ Important Security Information:</strong>
                        <ul>
                            <li>This link expires in {{ expires_minutes }} minutes</li>
                            <li>If you didn't request this reset, please ignore this email</li>
                            <li>Never share this link with anyone</li>
                        </ul>
                    </div>
                    
                    <p>If the button doesn't work, copy and paste this link into your browser:</p>
                    <p style="word-break: break-all; color: #667eea;">{{ reset_link }}</p>
                    
                    <hr style="margin: 30px 0; border: none; border-top: 1px solid #e5e7eb;">
                    <p style="font-size: 14px; color: #6b7280;">
                        For security reasons, this password reset link will expire automatically. 
                        If you need assistance, please contact your system administrator.
                    </p>
                </div>
                <div class="footer">
                    <p>© {{ current_year }} JMK Facilities Management</p>
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """)
        
        text_template = Template("""
        Password Reset Request - JMK Facilities Management
        
        Hi {{ user_name }},
        
        We received a request to reset your password for your JMK Facilities Management account.
        
        Click this link to reset your password:
        {{ reset_link }}
        
        ⚠️ IMPORTANT:
        - This link expires in {{ expires_minutes }} minutes
        - If you didn't request this reset, please ignore this email
        - Never share this link with anyone
        
        For security reasons, this password reset link will expire automatically.
        If you need assistance, please contact your system administrator.
        
        © {{ current_year }} JMK Facilities Management
        This is an automated message. Please do not reply to this email.
        """)
        
        context = {
            'user_name': user_name,
            'reset_link': reset_link,
            'expires_minutes': expires_minutes,
            'current_year': datetime.now().year
        }
        
        return html_template.render(**context), text_template.render(**context)

    @staticmethod
    def welcome_email_template(user_name: str, user_email: str, login_url: str, temporary_password: str = None) -> tuple:
        """Welcome email for new users"""
        html_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Welcome to JMK Facilities</title>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; background-color: #f8fafc; }
                .container { max-width: 600px; margin: 0 auto; background-color: white; }
                .header { background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 30px; text-align: center; }
                .header h1 { color: white; margin: 0; font-size: 24px; }
                .content { padding: 40px 30px; }
                .button { display: inline-block; background: linear-gradient(135deg, #10b981 0%, #059669 100%); color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: 600; margin: 20px 0; }
                .credentials { background-color: #f0fdf4; border: 1px solid #bbf7d0; padding: 15px; border-radius: 6px; margin: 20px 0; }
                .footer { background-color: #f8fafc; padding: 20px; text-align: center; font-size: 14px; color: #6b7280; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎉 Welcome to JMK Facilities!</h1>
                </div>
                <div class="content">
                    <h2>Hi {{ user_name }},</h2>
                    <p>Welcome to the JMK Facilities Management System! Your account has been created successfully.</p>
                    
                    <div class="credentials">
                        <strong>Your Login Details:</strong>
                        <p><strong>Email:</strong> {{ user_email }}</p>
                        {% if temporary_password %}
                        <p><strong>Temporary Password:</strong> {{ temporary_password }}</p>
                        <p style="color: #dc2626; font-size: 14px;">⚠️ Please change this password after your first login for security.</p>
                        {% endif %}
                    </div>
                    
                    <div style="text-align: center;">
                        <a href="{{ login_url }}" class="button">Login to Your Account</a>
                    </div>
                    
                    <h3>Getting Started:</h3>
                    <ul>
                        <li>Login with your credentials above</li>
                        <li>Complete your profile setup</li>
                        <li>Explore the system features</li>
                        <li>Contact support if you need help</li>
                    </ul>
                    
                    <p>If you have any questions or need assistance, please don't hesitate to reach out to your system administrator.</p>
                </div>
                <div class="footer">
                    <p>© {{ current_year }} JMK Facilities Management</p>
                </div>
            </div>
        </body>
        </html>
        """)
        
        context = {
            'user_name': user_name,
            'user_email': user_email,
            'login_url': login_url,
            'temporary_password': temporary_password,
            'current_year': datetime.now().year
        }
        
        html_content = html_template.render(**context)
        text_content = f"""
        Welcome to JMK Facilities Management!
        
        Hi {user_name},
        
        Your account has been created successfully.
        
        Login Details:
        Email: {user_email}
        {'Temporary Password: ' + temporary_password if temporary_password else ''}
        
        Login here: {login_url}
        
        {'⚠️ Please change your temporary password after first login.' if temporary_password else ''}
        
        © {datetime.now().year} JMK Facilities Management
        """
        
        return html_content, text_content

    @staticmethod
    def account_locked_template(user_name: str, unlock_time: str, admin_email: str) -> tuple:
        """Account locked notification"""
        html_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Security Alert - Account Locked</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f8fafc; }
                .container { max-width: 600px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; }
                .header { background-color: #dc2626; padding: 20px; text-align: center; }
                .header h1 { color: white; margin: 0; }
                .content { padding: 30px; }
                .alert { background-color: #fef2f2; border: 1px solid #fecaca; padding: 15px; border-radius: 6px; margin: 20px 0; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔒 Security Alert</h1>
                </div>
                <div class="content">
                    <h2>Hi {{ user_name }},</h2>
                    <div class="alert">
                        <p><strong>Your account has been temporarily locked due to multiple failed login attempts.</strong></p>
                    </div>
                    <p><strong>Account will be unlocked at:</strong> {{ unlock_time }}</p>
                    <p>If this wasn't you, please contact your administrator immediately at: {{ admin_email }}</p>
                </div>
            </div>
        </body>
        </html>
        """)
        
        context = {
            'user_name': user_name,
            'unlock_time': unlock_time,
            'admin_email': admin_email
        }
        
        return html_template.render(**context), f"Security Alert: Your account has been locked until {unlock_time}. Contact {admin_email} if this wasn't you."

# Password Reset Token Management
class PasswordResetManager:
    def __init__(self, s3_client, bucket_name: str):
        self.s3 = s3_client
        self.bucket_name = bucket_name
        self.reset_tokens_key = "data/password_reset_tokens.json"
    
    def _load_tokens(self) -> dict:
        """Load reset tokens from S3"""
        try:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=self.reset_tokens_key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except self.s3.exceptions.NoSuchKey:
            return {}
    
    def _save_tokens(self, tokens: dict):
        """Save reset tokens to S3"""
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=self.reset_tokens_key,
            Body=json.dumps(tokens, indent=2),
            ContentType="application/json"
        )
    
    def create_reset_token(self, user_id: str, expires_minutes: int = 15) -> str:
        """Create a password reset token"""
        tokens = self._load_tokens()
        
        # Clean expired tokens
        now = datetime.utcnow()
        tokens = {
            token: data for token, data in tokens.items()
            if datetime.fromisoformat(data['expires']) > now
        }
        
        # Create new token
        token = str(uuid.uuid4())
        expires = now + timedelta(minutes=expires_minutes)
        
        tokens[token] = {
            'user_id': user_id,
            'expires': expires.isoformat(),
            'created': now.isoformat()
        }
        
        self._save_tokens(tokens)
        return token
    
    def validate_reset_token(self, token: str) -> Optional[str]:
        """Validate reset token and return user_id if valid"""
        tokens = self._load_tokens()
        
        if token not in tokens:
            return None
        
        token_data = tokens[token]
        expires = datetime.fromisoformat(token_data['expires'])
        
        if datetime.utcnow() > expires:
            # Token expired, remove it
            del tokens[token]
            self._save_tokens(tokens)
            return None
        
        return token_data['user_id']
    
    def consume_reset_token(self, token: str) -> Optional[str]:
        """Consume (delete) reset token and return user_id if valid"""
        user_id = self.validate_reset_token(token)
        if user_id:
            tokens = self._load_tokens()
            if token in tokens:
                del tokens[token]
                self._save_tokens(tokens)
        return user_id

# Initialize global instances
email_service = EmailService()
