import pytz
import time
from datetime import datetime as dt
from twilio.rest import Client
import requests
import json

# Twilio credentials
account_sid = 'AC31f35ab1b7e268a8b1b0147ac9ac012c'
auth_token = '4c9e51ae628cf196402380b236f534b5'
twilio_whatsapp_number = 'whatsapp:+14155238886'  # Twilio's WhatsApp number

# Shifting schedule and WhatsApp numbers
whatsapp_number = {
    "rian": 'whatsapp:+6281282847410',
    "ardian": 'whatsapp:+6285878929886',
    "kharis": 'whatsapp:+6281227148885',
    "muhidin": 'whatsapp:+628999999488',
}


schedule = {
    "01-02-2024": ["rian","muhidin","kharis"],
    "02-02-2024": ["ardian","rian","muhidin"],
    "03-02-2024": ["kharis","ardian","rian"],
    "04-02-2024": ["muhidin","kharis","ardian"],
    "05-02-2024": ["rian","muhidin","kharis"],
    "06-02-2024": ["ardian","rian","muhidin"],
    "07-02-2024": ["kharis","ardian","rian"],
    "08-02-2024": ["muhidin","kharis","ardian"],
    "09-02-2024": ["rian","muhidin","kharis"],
    "10-02-2024": ["ardian","rian","muhidin"],
    "11-02-2024": ["kharis","ardian","rian"],
    "12-02-2024": ["muhidin","kharis","ardian"],
    "13-02-2024": ["rian","muhidin","kharis"],
    "14-02-2024": ["ardian","rian","muhidin"],
    "15-02-2024": ["kharis","ardian","rian"],
    "16-02-2024": ["muhidin","kharis","ardian"],
    "17-02-2024": ["rian","muhidin","kharis"],
    "18-02-2024": ["ardian","rian","muhidin"],
    "19-02-2024": ["kharis","ardian","rian"],
    "20-02-2024": ["muhidin","kharis","ardian"],
    "21-02-2024": ["rian","muhidin","kharis"],
    "22-02-2024": ["ardian","rian","muhidin"],
    "23-02-2024": ["kharis","ardian","rian"],
    "24-02-2024": ["muhidin","kharis","ardian"],
    "25-02-2024": ["rian","muhidin","kharis"],
    "26-02-2024": ["ardian","rian","muhidin"],
    "27-02-2024": ["kharis","ardian","rian"],
    "28-02-2024": ["muhidin","kharis","ardian"],
    "29-02-2024": ["rian","muhidin","kharis"],
}

wa_dags = [
    'cbs_tradefinance_to_landing',
    'cbs_loaniq_to_landing',
    'dag_cbs_ism_gbr',
    'cms_cccore_to_datalake',
    'dag_wms_crn_level',
    'equation_batch_btpmis',
    'equation_ymis_to_datalake',
    'ods_cms_data_metric',
    'wms_data_metric',
    'customer_golden_data_metric_rev',
    'dag_exus_to_datalake',
    'fes_to_datalake',
    'jfast_daily_data_metric',
    'cbs_lotus_to_landing',
    'crm_data_drop',
    'dag_exus_job',
    'dsme_reminder',
    'eadvis_batch_airflow',
    'equation_batch_data_metric',
    'NET_POS_HIST_RLUD_Job',
    'ods_to_staging_and_history_data_metric',
    'tfms_daily_metric_net',
    'penyamaan_collect_data_metric',
    'ods_jfmf_data_metric',
    'TCMS_Job',
    'two_job',
    'cbs_mspayment',
    'cms_dlk_to_efs',
    'TIS',
    'TWO_TCMS_To_ODS_NET',
    'cbs_loaniq_to_ods',
    'cbs_tradefinance_to_ods',
    'cbs_treasury_to_ods',
    'cbs_datalake2edw',
    'dag_ers_job',
]

# Twilio client setup
client = Client(account_sid, auth_token)

URL = "https://drive.google.com/uc?id=14w6vFHohd651RABg5cSdfzEjYjUKVQsx"

response = requests.get(URL)
dag = json.loads(response.text)

def sleep_until_next_minute():
    # Set the desired timezone (Asia/Jakarta)
    jakarta_timezone = pytz.timezone('Asia/Jakarta')

    # Get the current time in the specified timezone
    current_time_utc = time.gmtime()
    current_time = pytz.utc.localize(dt(*current_time_utc[:6])).astimezone(jakarta_timezone)

    # Calculate the number of seconds until the next minute
    seconds_until_next_minute = 60 - current_time.second

    # Sleep for the calculated duration
    time.sleep(seconds_until_next_minute)

def bold(text):
    return f"\033[1m{text}\033[0m"

def get_shift(current_time, shifts_for_date):
    for i, shift in enumerate(shifts_for_date, start=1):
        shift_start_hour = (i - 1) * 8  # Assuming each shift is 8 hours long
        shift_end_hour = i * 8
        if shift_start_hour <= current_time.hour < shift_end_hour:
            return shift
    return None

while True:

    # Set the desired timezone (Asia/Jakarta)
    jakarta_timezone = pytz.timezone('Asia/Jakarta')

    # Get the current time in the specified timezone
    current_time_utc = time.gmtime()
    current_time = pytz.utc.localize(dt(*current_time_utc[:6])).astimezone(jakarta_timezone)
    datetime_format = f"{current_time.day}/{current_time.month}/{current_time.year} {current_time.hour}:{current_time.minute}:{current_time.second}"
    current_date = dt.now(pytz.timezone('Asia/Jakarta')).strftime('%d-%m-%Y')

    if current_date in schedule:
        print(f'\ncurrent date and time: {datetime_format}')
        shifts_for_date = schedule[current_date]
        print(f'shifts for date: {shifts_for_date}')

        # Find the shift for the person on duty during the current time
        current_shift = get_shift(current_time, shifts_for_date)
        if current_shift:
            print(f'current shift: {current_shift}')

            for job in dag:
                job_name = job['dag_name']
                start_at = dt.strptime(job['start_at'], '%H:%M:%S').time()
                done_at = dt.strptime(job['done_at'], '%H:%M:%S').time()

                # Print the job only if the current time is equal to the 'done_at' time of the job
                if current_time.hour == done_at.hour and current_time.minute == done_at.minute:
                    if job_name in wa_dags:
                        print(f'\nWhatsapp notification job: {bold(job_name)} - READY_TO_CHECK')

                        # Send WhatsApp notification using Twilio to the person on duty
                        message_body = f"Whatsapp notification job: *{job_name}* is ready to check"
                        message = client.messages.create(
                            body=message_body,
                            from_=twilio_whatsapp_number,
                            to=whatsapp_number[current_shift]  # Send to the person on duty during the current shift
                        )
                        print(f"WhatsApp notification sent to {current_shift}, {whatsapp_number[current_shift]}: {message.body}\n")
                        time.sleep(1)

                    else:
                        print(f'\n{bold(job_name)} - READY_TO_CHECK')

                        # Send WhatsApp notification using Twilio to the person on duty
                        message_body = f"*{job_name}* is ready to check"
                        message = client.messages.create(
                            body=message_body,
                            from_=twilio_whatsapp_number,
                            to=whatsapp_number[current_shift]  # Send to the person on duty during the current shift
                        )
                        print(f"WhatsApp notification sent to {current_shift}, {whatsapp_number[current_shift]}: {message.body}\n")
                        time.sleep(1)

            if current_time.weekday() < 5:
                if 8 <= current_time.hour <= 17 and current_time.minute == 55:
                    print(f'\nhourly job: {bold("cbs_mspayment_intraday")} - READY_TO_CHECK')

                    # Send WhatsApp notification using Twilio to the person on duty
                    message_body = f"hourly job: *cbs_mspayment_intraday* is ready to check"
                    message = client.messages.create(
                        body=message_body,
                        from_=twilio_whatsapp_number,
                        to=whatsapp_number[current_shift]  # Send to the person on duty during the current shift
                    )
                    print(f"WhatsApp notification sent to {current_shift}, {whatsapp_number[current_shift]}: {message.body}\n")

            if current_time.weekday() > 4:
                if 8 <= current_time.hour <= 17 and current_time.minute == 33:
                    print(f'\nhourly job: {bold("cbs_mspayment_intraday")} - READY_TO_CHECK')

                    # Send WhatsApp notification using Twilio to the person on duty
                    message_body = f"hourly job: *cbs_mspayment_intraday* is ready to check"
                    message = client.messages.create(
                        body=message_body,
                        from_=twilio_whatsapp_number,
                        to=whatsapp_number[current_shift]  # Send to the person on duty during the current shift
                    )
                    print(f"WhatsApp notification sent to {current_shift}, {whatsapp_number[current_shift]}: {message.body}\n")

            if current_time.day == 13 and current_time.hour == 7 and current_time.minute == 5:
                print(f"\nmonthly job: {bold('base_audit_confirmation')} - READY_TO_CHECK")

                message_body = f"nmonthly job: *base_audit_confirmation* is ready to check"
                message = client.messages.create(
                    body=message_body,
                    from_=twilio_whatsapp_number,
                    to=whatsapp_number[current_shift]  # Send to the person on duty during the current shift
                )
                print(f"WhatsApp notification sent to {current_shift}, {whatsapp_number[current_shift]}: {message.body}\n")

    # Add a sleep statement to introduce a delay between iterations (e.g., 60 seconds)
    sleep_until_next_minute()