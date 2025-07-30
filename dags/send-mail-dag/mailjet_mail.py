from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='mailjet_mail_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 24),
    schedule='@hourly',
    catchup=False,
    tags=['email', 'mailjet']
) as dag:
    
    def mailjet_mail():
        from mailjet_rest import Client

        # Best practice: Store these in environment variables for security
        MAILJET_API_KEY = 'f4129f12edcb297538d927bf479e9df8'
        MAILJET_API_SECRET = '427e512e949f23e82cbf7a6d6e131232'

        # Email content
        from_email = 'ttogoldor144@gmail.com'
        to_email = 'togoldort226@gmail.com'
        subject = 'Hello from Mailjet and Python!'
        text_part = 'This is a plain text body of the email.'
        html_part = '<h3>This is an <strong>HTML</strong> body of the email.</h3>'

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
              

    mailjet_email_send_task = PythonOperator(
        task_id = "Mailjet_Email_task",
        python_callable = mailjet_mail,
        dag = dag
    )
    mailjet_email_send_task

