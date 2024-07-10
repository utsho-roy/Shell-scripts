#!/bin/bash
#----------------------------------------------------------------------------
# Created By  : Utsa Roy ( UROY0 )
# Created Date: 06/06/2023
# version ='1.0'
# ---------------------------------------------------------------------------
# Script name= anaplan_password_notification.sh
#Purpose: The purpose of this script is to send timely reminder emails to users regarding 
#		  the expiration of their Anaplan passwords. By calculating the number of days 
#         until password expiry and comparing it with a predefined threshold, the script  
#		  ensures that the team is notified in advance. Whether the password has already  
#         expired or will expire soon, the script composes personalized email messages, 
#		  urging the team to update the passwords promptly.
# ---------------------------------------------------------------------------
 

# Calculate the current date
current_date=$(date +%Y-%m-%d)

# Define the password renewal interval in days
renewal_interval=90

# Define the date when the password was last changed
pass_line=$(grep HSDEPlan /var/app/shs_datahub_anaplan_prd/config/shs-datahub-anaplan-config)
password=$(echo "$pass_line" | cut -d '"' -f 2)
last_password_date=$(date -d "${password:8:4}-${password:12:2}-${password:14:2}" +%Y-%m-%d)

# Calculate the number of days until password expiry
days_until_expiry=$(( ( $(date -d "$last_password_date + $renewal_interval days" +%s) - $(date -d "$current_date" +%s) ) / 86400 ))

# Check if it's time to send the password renewal reminder
if [ $days_until_expiry -le 5 ]; then
	if [ $days_until_expiry -le 0 ]; then
		# Compose the email message
		email_subject="Anaplan Password Renewal Reminder"
		email_body="Anaplan password has expired $days_until_expiry days ago. Please update the password as soon as possible.\n\nThank you."
		# Send the email
		echo -e "$email_body" | mail -s "$email_subject"  HSDataEnggTeam@searshc.com
	else 
		# Compose the email message
		email_subject="Anaplan Password Renewal Reminder"
		email_body="Anaplan password will expire in $days_until_expiry days. Please update the password as soon as possible.\n\nThank you."
		# Send the email
		echo -e "$email_body" | mail -s "$email_subject"  HSDataEnggTeam@searshc.com
	fi
fi
