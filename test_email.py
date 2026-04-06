#!/usr/bin/env python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Test email configuration
smtp_host = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'saugnikaich123@gmail.com'
smtp_password = 'lcbwjjeyiulgxuhf'
recipient_email = 'saugnikaich123@gmail.com'

print("Testing email configuration...")
print(f"SMTP Host: {smtp_host}")
print(f"SMTP Port: {smtp_port}")
print(f"Username: {smtp_username}")

try:
    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
        print("✅ Connected to SMTP server")
        
        smtp.ehlo()
        print("✅ EHLO sent")
        
        smtp.starttls()
        print("✅ TLS started")
        
        smtp.ehlo()
        print("✅ EHLO sent after TLS")
        
        smtp.login(smtp_username, smtp_password)
        print("✅ Login successful")
        
        # Send test email
        msg = MIMEMultipart()
        msg['Subject'] = 'Test Email - Website Monitor'
        msg['From'] = smtp_username
        msg['To'] = recipient_email
        msg.attach(MIMEText('This is a test email from the Website Monitor\n\nIf you received this, email is working correctly!', 'plain'))
        
        smtp.sendmail(smtp_username, [recipient_email], msg.as_string())
        print("✅ Test email sent successfully!")
        print(f"\n✅ ALL EMAIL TESTS PASSED - Check your inbox at {recipient_email}")
        
except smtplib.SMTPAuthenticationError as e:
    print(f"❌ Authentication failed: {e}")
    print("Check your email username and password")
except smtplib.SMTPException as e:
    print(f"❌ SMTP error: {e}")
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {e}")
