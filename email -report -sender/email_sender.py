import smtplib
from email.message import EmailMessage
from pathlib import Path

def send_email(subject:str,body:str,attachments:list[Path])->None:
    FROM="mbewef86@gmail.com"
    TO="stephenakutii4@gmail.com"
    SMTP_SERVER="smtp.gmail.com"
    SMTP_PORT=587
    PASSWORD="Yuichi1234@@@@"

    msg=EmailMessage()
    msg["From"]=FROM
    msg["To"]=TO
    msg["Subject"]=subject
    msg.set_content(body)
    
    for file in attachments:
        with open(file,"rb") as f:
         if isinstance(file,Path):
          file_info:Path=f.read()

        msg.add_attachment(file_info,maintype="application",subtype="octet_stream",filename=file_info.name)

    with smtplib.SMTP(SMTP_SERVER,SMTP_PORT) as server:
       server.starttls()
       server.login(FROM,PASSWORD)
       server.send_message(msg)
       