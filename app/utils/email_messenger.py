from email.message import EmailMessage
import aiosmtplib

async def send_email(to, subject, body):
    message = EmailMessage()
    message["From"] = "info@jmkfacilities.ie"
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body)

    await aiosmtplib.send(
        message,
        hostname="smtp.blacknight.com",
        port=587,
        username="info@jmkfacilities.ie",
        password=os.getenv("EMAIL_PASS"),
        start_tls=True
    )