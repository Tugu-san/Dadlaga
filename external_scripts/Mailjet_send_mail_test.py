from mailjet_rest import Client
import os

# Best practice: Store these in environment variables for security
MAILJET_API_KEY = 'f4129f12edcb297538d927bf479e9df8'
MAILJET_API_SECRET = '427e512e949f23e82cbf7a6d6e131232'

# Email content
from_email = 'ttogoldor144@gmail.com'
to_email = 'togoldort226@gmail.com'
subject = 'Hello from Mailjet and Python!'
text_part = 'This is a plain text body of the email.'
html_part = '<h3>This is an <strong>HTML</strong> body of the email.</h3>'

def send_email():
    mailjet = Client(auth=(MAILJET_API_KEY, MAILJET_API_SECRET), version='v3.1')
    
    data = {
        'Messages': [
            {
                "From": {
                    "Email": from_email,
                    "Name": "Your Name"
                },
                "To": [
                    {
                        "Email": to_email,
                        "Name": "Recipient Name"
                    }
                ],
                "Subject": subject,
                "TextPart": text_part,
                "HTMLPart": html_part,
                "CustomID": "PythonMailjetTest"
            }
        ]
    }

    result = mailjet.send.create(data=data)
    print(result.status_code)
    print(result.json())

if __name__ == '__main__':
    send_email()

