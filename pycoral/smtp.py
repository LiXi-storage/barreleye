"""
SMTP library to send notification
"""
import smtplib
import traceback
from email.mime.text import MIMEText
from pycoral import clog


def email_send(log, subject, content, sender_addr, recipient_addrs,
               stmp_host, sender_user=None, sender_password=None,
               stmp_port=0):
    """
    Send a message to a email address
    """
    # pylint: disable=too-many-arguments,bad-option-value,redefined-variable-type
    text = MIMEText(content, 'plain')
    text['From'] = sender_addr
    text['To'] = ", ".join(recipient_addrs)
    text['Subject'] = subject
    message = text.as_string()
    try:
        smtp = smtplib.SMTP_SSL(stmp_host, stmp_port)
    except:
        log.cl_info("failed to connect SMTP [%s:%s] using SSL, "
                    "trying unsafe mode", stmp_host, stmp_port)
        try:
            smtp = smtplib.SMTP(stmp_host, stmp_port)
        except:
            log.cl_error("failed to connect to [%s:%s]",
                         stmp_host, stmp_port)
            return -1

    try:
        if sender_user is not None and sender_password is not None:
            smtp.login(sender_user, sender_password)
        smtp.sendmail(sender_addr, recipient_addrs, message)
        smtp.quit()
    except:
        log.cl_error("exception when sending email [%s]: [%s]",
                     message, traceback.format_exc())
        return -1
    log.cl_info("email has been sent")
    return 0


def test_email_send():
    """
    Main function
    """
    log = clog.get_log(console_format=clog.FMT_NORMAL)
    message = "Hello, this is a test of SMTP"
    stmp_host = "smtp.163.com"
    sender_addr = "lustredev@163.com"
    sender_user = "lustredev@163.com"
    sender_password = "BGGLSUFXFWXJVRQV"
    recipient_addrs = ["lustredev@163.com"]
    for stmp_port in [0, 25, 465, 994]:
        subject = "Email from SMTP [%s:%s]" % (stmp_host, stmp_port)
        ret = email_send(log, subject, message, sender_addr,
                         recipient_addrs, stmp_host,
                         sender_user=sender_user,
                         sender_password=sender_password, stmp_port=stmp_port)
        if ret:
            log.cl_error("failed to send email")
            return -1
    return 0


if __name__ == "__main__":
    test_email_send()
