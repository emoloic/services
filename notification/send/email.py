import smtplib, os, json
from email.message import EmailMessage

def notification(message):
    try:
        message = json.loads(message)
        mp3_fid = message["mp3_fid"]
        sender_address = os.getenv("GMAIL_ADDRESS")
        sender_password = os.getenv("GMAIL_PASSWORD")
        receiver_address = message["username"]

        msg = EmailMessage()
        msg.set_content(f"mp3 file_id: {mp3_fid} is now ready!")
        msg["Subject"] = "MP3 Download"
        msg["From"] = sender_address
        msg["To"] = receiver_address

        # Create an SMTP session for sending the mail
        # Connect to google SMTP server, log to our gmail account and send the email
        session = smtplib.SMTP("smtp.gmail.com", 587)
        session.starttls() # Secure our packet in transit so that there can be intercept
        session.login(sender_address, sender_password)
        session.send_message(msg, sender_address, receiver_address)
        session.quit()
        print("Mail sent")
    except Exception as err:
        print(err)
        return err

