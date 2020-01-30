import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
from datetime import date, timedelta
import os

sync_command = f"aws s3 sync s3://moovement/ /home/ubuntu/s3_storage"
os.system(sync_command)

tabel = pd.read_csv("s3_storage/davedata_19jan_24jan.csv")
tabel['Date'] = pd.to_datetime(tabel["gateway_time"])

#possible to replace with date fill in: '2020-01-20 00:00:00' pd.Timestamp.today() - timedelta(days=2)
from_date = '2020-01-21 00:00:00'
fromdate = tabel['Date'] >= from_date
fromdate2= tabel[fromdate]
#Today: pd.Timestamp.today()
till_date = '2020-01-21 04:08:00'
tilldate = fromdate2['Date'] <= till_date
tilldate2= fromdate2[tilldate]

#gpspackage = tilldate2['lora_packet_type'] == "GPS"
#gpspackage2 = tilldate2[gpspackage]



pivottable_gps_notsent = pd.pivot_table(tilldate2, index= 'hardware_serial', columns='lora_packet_type',values= "time_to_fix", aggfunc=np.count_nonzero, fill_value=0)
check_if_no_location = pivottable_gps_notsent.reset_index()

if 'BASIC' in check_if_no_location.columns:       # condition in your case being `num2 == num5`
    check_if_no_location = check_if_no_location.drop(columns="BASIC")
else:
    print("No basic messages found")

if 'HEARTBEAT' in check_if_no_location.columns:       # condition in your case being `num2 == num5`
    check_if_no_location = check_if_no_location.drop(columns="HEARTBEAT")
else:
    print("No heartbeat messages found")

#check_if_no_location = check_if_no_location.drop(columns="RESET")


gps_not_sent = check_if_no_location['GPS'] == 0
gps_not_sent2 = check_if_no_location[gps_not_sent]




with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    print ((gps_not_sent2['hardware_serial']).to_string(index=False))

mailstring =  str((gps_not_sent2['hardware_serial']).to_string(index=False))

gps_not_sent2.to_csv('GPS_not_sent.csv')

# Replace sender@example.com with your "From" address.
# This address must be verified with Amazon SES.
SENDER = "Thomas Boer <Thomasboer1997@gmail.com>"

# Replace recipient@example.com with a "To" address. If your account
# is still in the sandbox, this address must be verified.
RECIPIENT = "thoom01@hotmail.com"


# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the
# ConfigurationSetName=CONFIGURATION_SET argument below.
#CONFIGURATION_SET = "ConfigSet"
#ConfigurationSetName=CONFIGURATION_SET

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "eu-central-1"

# The subject line for the email.
SUBJECT = "Hardware Tags without GPS location in past 2 days"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Amazon SES Test (Python)\r\n"
             "Hoi Gerben, als je dit leest is deze mail "
             "verzonden via de Ubuntu server van AWS"
             )

# The HTML body of the email.
BODY_HTML = "Goodmorning. These are the tags from where no GPS location was sent since the day before yesterday:<br><b>" \
            + mailstring +"</b>"

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses', region_name=AWS_REGION)

# Try to send the email.
try:
    # Provide the contents of the email.
    response = client.send_email(
        Destination={
            'ToAddresses': [
                RECIPIENT,
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=SENDER,
        # If you are not using a configuration set, comment or delete the
        # following line
        #ConfigurationSetName=CONFIGURATION_SET,
    )
# Display an error if something goes wrong.
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:"),
    print(response['MessageId'])