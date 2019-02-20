#!/usr/bin/python
 
import pprint
import skew
from datetime import datetime, timedelta, timezone

# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.message import EmailMessage

import argparse
parser = argparse.ArgumentParser()
parser.add_argument(
        "--mailserver", 
        help="An open mail server (Port 25, plain will be used)",
        action="store",
        default="mail"
    )
parser.add_argument(
        "--reply", 
        help="Where will replies to the email be routed to (should be you ticket system)",
        action="store",
        default="info@example.com"
    )
parser.add_argument(
        "--to", 
        help="The recipients list (we use a distribution group)",
        action="store",
        default="info@example.com"
    )
args = parser.parse_args()


def send_email(subject, body):
    print("Sending mail: %s" % subject)

    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(body)
    msg['From'] = args.reply
    msg['To'] = args.to
    msg['Subject'] = subject

    # Send the message via our own SMTP server.
    s = smtplib.SMTP(args.mailserver)
    s.send_message(msg)
    s.quit()



output = {
    'warning': [],
    'critical': [],
    'delete': [],
}

uri = 'arn:aws:*:*:*:*/*'
# uri = 'arn:aws:ec2:*:*:volume/*' # Smaller sample for quicker testing
print("Looking for resources with filter %s" % (uri))
for resource in skew.scan(uri):
    # Ignore resources younger then 1 day and make sure to warn users about deletion
    level = ''
    resource_age = datetime.now(timezone.utc) - datetime.now(timezone.utc)
    if 'CreateTime' in resource.data:
        resource_age = datetime.now(timezone.utc) - resource.data['CreateTime']
    if resource_age.days > 31:
        print("  Marking resource %s for deletion" % (resource.arn))
        output['delete'].append(resource.arn)
    if resource_age.days > 20:
        level = 'critical'
    elif resource_age.days > 1:
        level = 'warning'

    if level != '':
        print("  Found %s resource %s" % (level, resource.arn))
        if not resource.tags:
            # print('%s is untagged' % resource.arn)
            output[level].append({'data': resource.data, 'arn': resource.arn})
        else:
            if not ("Name" in resource.tags  or "exa:owner" in resource.tags or "exa:project" in resource.tags or "exa:department" in resource.tags):
                # print('%s is tagged wrong' % resource.arn)
                output[level].append({'data': resource.data, 'arn': resource.arn})

print("Done browsing.\n")


def getDisplay(item):
    return_str = "%s - CreateTime: %s"% (
            item['arn'], 
            item['data']['CreateTime'],
        )
    if "Tags" in item['data']:
        return_str = return_str + "\n    Tags: %s" % (
                item['data']['Tags'],
            )
    return return_str + "\n"


# Send warning mail
if (len(output['warning']) > 0):
    mail_body = """Hello AWS User,

The following resources are older then 1 day and are not tagged yet or violate the tagging guideline!
https://intranet.srv.exasol.com/confluence/x/EwHfAw

Please note, that all resources, that are untagged or violate the tagging guideline and are older then 7 days will be deleted automatically!


""" + "\n".join(
        map(
            getDisplay,
            output['warning']
        )
    )
    send_email(
        "WARNING - Untagged AWS Resources",
        mail_body
    )


# Send critical mail
if (len(output['critical']) > 0):
    mail_body = """Hello AWS User,

The following resources are not tagged yet or violate the tagging guideline and will be deleted tomorrow!
https://intranet.srv.exasol.com/confluence/x/EwHfAw


""" + "\n".join(
        map(
            getDisplay,
            output['critical']
        )
    )
    send_email(
        "CRITICAL - AWS Resources to be deleted tomorrow",
        mail_body
    )

# Store stuff to be deleted
if (len(output['delete']) > 0):
    with open('/tmp/aws_delete.lst', 'w') as f:
        for item in output['delete']:
            f.write("%s\n" % item)
