import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint

# Configure API key authorization: api-key
configuration = sib_api_v3_sdk.Configuration()
configuration.api_key['api-key'] = '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'  # Replace with your actual API key

# Create an instance of the API class
api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

# Define sender and recipient
sender = {"name": "Tuguldur Govikhuu", "email": "ttogoldor144@gmail.com"}
to = [{"email": "togoldort226@gmail.com", "name": "Togoldor Toogoo"}]

# Compose your email
subject = "Hello from Brevo via Python"
html_content = "<html><body><h1>This is a test email</h1><p>Sent using Brevo's API.</p></body></html>"

# Create the email object
send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
    to=to,
    sender=sender,
    subject=subject,
    html_content=html_content
)

try:
    # Send the email
    api_response = api_instance.send_transac_email(send_smtp_email)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling TransactionalEmailsApi->send_transac_email: %s\n" % e)
