import json

from update_checker import check_for_updates
from send_email import send_alerts, email

def lambda_handler(event, context):
    
    try:
        new_countries, new_updates = check_for_updates()
        
        send_alerts(new_countries, new_updates)
    
    except Exception as e:
        email(e)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully loaded')
    }
